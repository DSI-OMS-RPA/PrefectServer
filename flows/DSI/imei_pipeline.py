from datetime import datetime, date as date_cls
from typing import Dict, List

import pendulum
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.task_runners import ConcurrentTaskRunner

from common.database.mongo_client import MongoDBClient
from common.database.sql_server import SQLServerClient
from models.imei_mediation import IMEIDataModel
from queries.loader import load_sql_query


@task(name="Get Date Range to Process", retries=2)
def get_date_range(reset: bool = False) -> List[str]:
    """
    Determine the date range to process based on last run or reset flag.

    Args:
        reset: If True, reprocess from beginning of current month

    Returns:
        List of dates to process in YYYY-MM-DD format
    """
    logger = get_run_logger()

    # Get current date and yesterday
    now = pendulum.now("UTC")
    yesterday = now.subtract(days=1)
    yesterday_str = yesterday.format("YYYY-MM-DD")

    # If reset, start from beginning of current month
    if reset:
        start_date = now.start_of("month")
        logger.info(
            f"Reset requested - processing from {start_date.format('YYYY-MM-DD')} to {yesterday_str}"
        )
        dates = []
        current = start_date
        while current <= yesterday:
            dates.append(current.format("YYYY-MM-DD"))
            current = current.add(days=1)
        return dates

    # Find most recent date in MongoDB to determine where to start
    try:
        with MongoDBClient(use_block="mongodb-imei") as mongo_client:
            # Get the most recent date from MongoDB collection
            collection_name = f"cdr{now.year}"
            mongo_client.set_collection(collection_name)

            # Query to find most recent date
            pipeline = [
                {"$sort": {"uf301": -1}},
                {"$limit": 1},
                {
                    "$project": {
                        "_id": 0,
                        "date": {
                            "$dateToString": {"format": "%Y-%m-%d", "date": "$uf301"}
                        },
                    }
                },
            ]

            result = mongo_client.aggregate(pipeline)
            if result and len(result) > 0:
                last_date = result[0].get("date")
                if last_date:
                    # Start from day after last date
                    start_date = pendulum.parse(last_date).add(days=1)

                    # If start date is in the future or equals today, nothing to process
                    if start_date > yesterday:
                        logger.info(
                            f"No new dates to process. Last processed: {last_date}"
                        )
                        return []

                    logger.info(
                        f"Processing from {start_date.format('YYYY-MM-DD')} to {yesterday_str}"
                    )

                    # Generate date range
                    dates = []
                    current = start_date
                    while current <= yesterday:
                        dates.append(current.format("YYYY-MM-DD"))
                        current = current.add(days=1)
                    return dates

            # If no records found or other error, process from beginning of current month
            start_date = now.start_of("month")
            logger.info(
                f"No previous processing found - starting from {start_date.format('YYYY-MM-DD')}"
            )
            dates = []
            current = start_date
            while current <= yesterday:
                dates.append(current.format("YYYY-MM-DD"))
                current = current.add(days=1)
            return dates

    except Exception as e:
        logger.error(f"Error determining date range: {e}")
        # Default to just yesterday if we encounter an error
        logger.info(f"Defaulting to processing just yesterday: {yesterday_str}")
        return [yesterday_str]


@task(name="Extract SQL Data", retries=3, retry_delay_seconds=10)
def extract_sql_data(date_str: str) -> List[Dict]:
    """
    Extract data from SQL Server for a specific date.

    Args:
        date_str: Date to process in YYYY-MM-DD format

    Returns:
        List of dictionaries with the extracted data
    """
    logger = get_run_logger()
    logger.info(f"Extracting data for date: {date_str}")

    # Convert date to the format needed for the query
    date = datetime.strptime(date_str, "%Y-%m-%d")
    date_val = date.strftime("%Y%m%d")
    month_year = date.strftime("%Y%m")

    # Load SQL query from file
    query_template = load_sql_query("DSI/imei_mediation_sql_server")

    try:
        # Connect to SQL Server using the block
        with SQLServerClient(use_block="sql-server-mediation") as sql_client:
            # Format query with date parameters
            formatted_query = query_template.format(
                month_year=month_year, date_val=date_val
            )

            # Execute query
            records = sql_client.execute_query(formatted_query)

            logger.info(f"Retrieved {len(records)} records for date {date_str}")

            # Validate minimum expected records
            MIN_EXPECTED_RECORDS = 1000
            if len(records) < MIN_EXPECTED_RECORDS:
                logger.warning(
                    f"Low record count: {len(records)} for {date_str}. Expected at least {MIN_EXPECTED_RECORDS}."
                )

            # Convert to IMEIDataModel instances
            transformed_records = []
            for record in records:
                try:
                    model = IMEIDataModel.from_dict(record)
                    transformed_records.append(model.model_dump())
                except Exception as e:
                    logger.error(f"Error transforming record: {e}")

            logger.info(
                f"Transformed {len(transformed_records)} records for date {date_str}"
            )
            return transformed_records

    except Exception as e:
        logger.error(f"Error extracting data for {date_str}: {e}")
        raise


@task(name="Insert to MongoDB", retries=3, retry_delay_seconds=60)
def insert_to_mongodb(data_batch: List[Dict], date_str: str) -> Dict:
    """
    Insert processed records into MongoDB, letting MongoDB generate _id.
    Handle duplicates gracefully.
    """
    logger = get_run_logger()

    if not data_batch:
        logger.info(f"No data to insert for date: {date_str}")
        return {"date": date_str, "inserted": 0, "updated": 0, "errors": 0}

    # Determine collection name based on year
    date = datetime.strptime(date_str, "%Y-%m-%d")
    collection_name = f"cdr{date.year}"

    try:
        with MongoDBClient(use_block="mongodb-imei") as mongo_client:
            # Ensure collection exists
            mongo_client.ensure_collection_exists(collection_name)

            # Convert date objects to datetime objects before insertion
            for record in data_batch:
                if (
                    "uf301" in record
                    and isinstance(record["uf301"], date_cls)
                    and not isinstance(record["uf301"], datetime)
                ):
                    record["uf301"] = datetime.combine(
                        record["uf301"], datetime.min.time()
                    )

                # Ensure no _id field exists - let MongoDB generate it
                record.pop("_id", None)
                record.pop("SequenceID", None)

            # Use insert_many with error handling for duplicates
            BATCH_SIZE = 10000
            total_inserted = 0
            duplicate_errors = 0
            other_errors = 0

            # Process in batches
            for i in range(0, len(data_batch), BATCH_SIZE):
                batch = data_batch[i : i + BATCH_SIZE]
                try:
                    # Insert batch - MongoDB will auto-generate _id
                    result = mongo_client.insert_many(
                        batch,
                        collection_name=collection_name,
                        ordered=False,  # Continue on duplicate errors
                    )

                    total_inserted += result
                    logger.info(
                        f"Batch {i // BATCH_SIZE + 1}: Inserted {result} records in {collection_name}"
                    )

                except Exception as e:
                    logger.warning(f"Batch insert had some errors: {e}")
                    # The insert_many method in your mongo_client handles BulkWriteError
                    # and returns the count of successfully inserted documents
                    # So we can still get a count even with some duplicates
                    if "duplicate key error" in str(e).lower():
                        duplicate_errors += 1
                    else:
                        other_errors += 1

            logger.info(
                f"Completed MongoDB insert for {date_str}: {total_inserted} new records inserted"
            )

            if duplicate_errors > 0:
                logger.info(
                    f"Encountered {duplicate_errors} batches with duplicate key errors (expected)"
                )
            if other_errors > 0:
                logger.warning(f"Encountered {other_errors} batches with other errors")

            return {
                "date": date_str,
                "inserted": total_inserted,
                "updated": 0,  # No updates with insert_many
                "duplicate_errors": duplicate_errors,
                "other_errors": other_errors,
            }

    except Exception as e:
        logger.error(f"Failed to insert data for {date_str}: {e}")
        return {
            "date": date_str,
            "inserted": 0,
            "updated": 0,
            "errors": 1,
            "error_message": str(e),
        }


@flow(name="Process Single Date", log_prints=True)
def process_single_date(date_str: str) -> Dict:
    """
    Process a single date from SQL to MongoDB.

    Args:
        date_str: Date to process in YYYY-MM-DD format

    Returns:
        Processing statistics
    """
    logger = get_run_logger()
    logger.info(f"Processing date: {date_str}")

    # Extract data
    data = extract_sql_data(date_str)

    if not data:
        logger.info(f"No data found for date: {date_str}")
        return {"date": date_str, "status": "complete", "records": 0, "inserted": 0}

    # Insert to MongoDB
    insertion_result = insert_to_mongodb(data, date_str)

    return {
        "date": date_str,
        "status": "complete",
        "records": len(data),
        "inserted": insertion_result.get("inserted", 0),
        "errors": insertion_result.get("errors", 0),
    }


@flow(name="IMEI ETL Flow", task_runner=ConcurrentTaskRunner())
def imei_etl_flow(reset: bool = False):
    """
    Main ETL flow for IMEI data processing.

    Args:
        reset: If True, reprocess from beginning of current month
    """
    logger = get_run_logger()
    logger.info(f"Starting IMEI ETL Flow (reset={reset})")

    # Get dates to process
    dates = get_date_range(reset)

    if not dates:
        logger.info("No dates to process. Exiting flow.")
        return {"status": "success", "message": "No dates to process"}

    logger.info(f"Processing {len(dates)} dates: {dates[0]} to {dates[-1]}")

    # Process each date and collect results
    results = []
    for date_str in dates:
        result = process_single_date(date_str)
        results.append(result)

    # Aggregate stats
    total_records = sum(r.get("records", 0) for r in results)
    total_inserted = sum(r.get("inserted", 0) for r in results)
    total_errors = sum(r.get("errors", 0) for r in results)

    # Create artifact with results table
    results_table = [
        {
            "Date": r.get("date"),
            "Records Processed": r.get("records", 0),
            "Records Inserted": r.get("inserted", 0),
            "Errors": r.get("errors", 0),
        }
        for r in results
    ]

    create_table_artifact(
        key="imei-etl-results",
        table=results_table,
        description=f"IMEI ETL Results - {len(dates)} dates processed",
    )

    logger.info(
        f"IMEI ETL Flow completed. Processed {len(dates)} dates, {total_records} records, inserted {total_inserted} records with {total_errors} errors."
    )

    return {
        "status": "success",
        "dates_processed": len(dates),
        "total_records": total_records,
        "total_inserted": total_inserted,
        "total_errors": total_errors,
    }


if __name__ == "__main__":
    # Run the flow for testing
    imei_etl_flow()
