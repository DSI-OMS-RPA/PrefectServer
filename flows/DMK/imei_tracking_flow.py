from prefect import flow, task, get_run_logger
from prefect.cache_policies import TASK_SOURCE, INPUTS
from datetime import datetime, time, timedelta
import pendulum
from typing import Any, Dict, List, Set, Optional

from common.database.mongo_client import MongoDBClient
from common.database.sql_server import SQLServerClient


@task(name="Get Last Processed Date", log_prints=True)
def get_last_processed_date() -> Optional[str]:
    """
    Retrieve the last processed date from SQL Server database.

    Returns:
        The last processed date in YYYY-MM-DD format, or None if no data exists
    """
    logger = get_run_logger()
    try:
        with SQLServerClient(use_block="sql-server-dmk") as sql_client:
            # Query to get the max date from the table
            query = "SELECT MAX(data) AS last_date FROM [dbo].[f_imei]"
            results = sql_client.execute_query(query)

            if results and results[0].get("last_date"):
                last_date = results[0]["last_date"].strftime("%Y-%m-%d")
                logger.info(f"Found last processed date from database: {last_date}")
                return last_date
            else:
                logger.info(
                    "No previous data found in database, this appears to be the first run"
                )
                return None
    except Exception as e:
        logger.error(f"Error retrieving last processed date from database: {e}")
        return None


@task(name="Generate Dates To Process", log_prints=True)
def generate_dates_to_process(last_processed_date: Optional[str]) -> List[str]:
    """
    Generate a list of dates to process based on the last processed date.

    For first run or reset: Generates dates from the beginning of the current month up to yesterday
    For subsequent runs: Generates only yesterday's date

    Args:
        last_processed_date: The last date that was successfully processed

    Returns:
        List of date strings to process in format YYYY-MM-DD
    """
    logger = get_run_logger()
    # Get current date and yesterday
    current_date = pendulum.now()
    yesterday = current_date.subtract(days=1)
    yesterday_str = yesterday.format("YYYY-MM-DD")

    # If we have a last processed date and it's equal to yesterday,
    # there's nothing new to process
    if last_processed_date and last_processed_date == yesterday_str:
        logger.info(
            f"Already processed data up to yesterday ({yesterday_str}), nothing new to process"
        )
        return []

    # If we have a last processed date, we only need to process dates after it
    if last_processed_date:
        # Parse the last processed date
        last_date = pendulum.parse(last_processed_date)

        # Get the start date (day after last processed date)
        start_date = last_date.add(days=1)

        # If start date is in the future, there's nothing to process
        if start_date > yesterday:
            logger.info(
                f"Next date to process would be {start_date.format('YYYY-MM-DD')}, which is in the future. Nothing to process."
            )
            return []

        logger.info(
            f"Will process dates from {start_date.format('YYYY-MM-DD')} to {yesterday_str}"
        )
    else:
        # For first run, start from beginning of month
        start_date = current_date.start_of("month")
        logger.info(
            f"First run detected. Will process dates from {start_date.format('YYYY-MM-DD')} to {yesterday_str}"
        )

    # Generate all dates from start to yesterday (inclusive)
    dates = []
    current = start_date
    while current <= yesterday:
        dates.append(current.format("YYYY-MM-DD"))
        current = current.add(days=1)

    logger.info(f"Generated {len(dates)} dates to process")
    return dates


@task(
    name="Extract IMEIs for Date",
    log_prints=True,
    retries=1,
    retry_delay_seconds=5,
    cache_expiration=timedelta(days=1),
    cache_policy=TASK_SOURCE + INPUTS,
)
def extract_imeis_for_date(date_str: str) -> List[Dict[str, Any]]:
    """
    Extract unique IMEIs with their associated phone numbers and call durations for a given date.
    For each IMEI, we keep the phone number with the highest total call duration.
    """
    logger = get_run_logger()
    date = datetime.strptime(date_str, "%Y-%m-%d")

    with MongoDBClient(use_block="mongodb-imei") as client:
        date_start = datetime.combine(date.date(), time.min)
        date_end = datetime.combine(date.date(), time.max)

        collection_name = f"cdr{date.year}"
        client.set_collection(collection_name)

        pipeline = [
            # Match documents for the given date
            {"$match": {"uf301": {"$gte": date_start, "$lte": date_end}}},
            # Filter out records where duration is 0
            {"$match": {"uf400": {"$gt": 0}}},
            # Group by IMEI and phone to calculate total duration
            {
                "$group": {
                    "_id": {
                        "imei": "$uf204",
                        "phone_number": "$uf201",
                        "date": {
                            "$dateToString": {"format": "%Y-%m-%d", "date": "$uf301"}
                        },
                    },
                    "total_duration_seconds": {"$sum": "$uf400"},
                    "call_count": {"$sum": 1},
                }
            },
            # Project fields for easier access
            {
                "$project": {
                    "_id": 0,
                    "imei": "$_id.imei",
                    "phone_number": "$_id.phone_number",
                    "date": "$_id.date",
                    "total_duration_seconds": 1,
                    "call_count": 1,
                }
            },
            # Sort by IMEI and total_duration in descending order
            {"$sort": {"imei": 1, "total_duration_seconds": -1}},
            # Group by IMEI to get the phone with highest duration
            {
                "$group": {
                    "_id": "$imei",
                    "phone_number": {"$first": "$phone_number"},
                    "date": {"$first": "$date"},
                    "total_duration_seconds": {"$first": "$total_duration_seconds"},
                    "call_count": {"$first": "$call_count"},
                }
            },
            # Final projection
            {
                "$project": {
                    "imei": "$_id",
                    "phone_number": 1,
                    "date": 1,
                    "total_duration_seconds": 1,
                    "call_count": 1,
                    "_id": 0,
                }
            },
            # Sort by IMEI
            {"$sort": {"imei": 1}},
        ]

        results = client.aggregate(pipeline=pipeline)
        logger.info(f"Extracted {len(results)} unique IMEIs for date {date_str}")
        return results


@task(
    name="Get Existing IMEIs",
    log_prints=True,
    retries=1,
    retry_delay_seconds=5,
)
def get_existing_imeis() -> Set[str]:
    """Get the set of all IMEIs already in the database."""
    logger = get_run_logger()

    with SQLServerClient(use_block="sql-server-dmk") as sql_client:
        query = "SELECT DISTINCT imei FROM [dbo].[f_imei]"
        results = sql_client.execute_query(query)
        existing_imeis = {row["imei"] for row in results}
        logger.info(f"Found {len(existing_imeis)} existing IMEIs in the database")
        return existing_imeis


@task(
    name="Insert New IMEIs",
    log_prints=True,
    retries=1,
    retry_delay_seconds=5,
)
def insert_new_imeis(new_imei_records: List[Dict[str, Any]]) -> int:
    """Insert new IMEI records into the database."""
    logger = get_run_logger()

    if not new_imei_records:
        logger.info("No new IMEIs to insert")
        return 0

    with SQLServerClient(use_block="sql-server-dmk") as sql_client:
        inserted_count = 0
        batch_size = 10000
        current_batch = []

        sql_client.connection.autocommit = False
        transaction_successful = False

        try:
            for record in new_imei_records:
                sql_record = {
                    "cliente": record["phone_number"],
                    "imei": record["imei"],
                    "duracao": record["total_duration_seconds"],
                    "data": datetime.strptime(record["date"], "%Y-%m-%d"),
                }

                current_batch.append(sql_record)

                if len(current_batch) >= batch_size:
                    inserted_count += _insert_batch(sql_client, current_batch)
                    current_batch = []

            # Insert any remaining records
            if current_batch:
                inserted_count += _insert_batch(sql_client, current_batch)

            # Commit the transaction
            sql_client.connection.commit()
            transaction_successful = True
            logger.info(
                f"Transaction committed successfully: {inserted_count} new IMEIs inserted"
            )

        except Exception as e:
            sql_client.connection.rollback()
            logger.error(f"Transaction failed and rolled back: {e}")
            raise

        finally:
            sql_client.connection.autocommit = True
            if not transaction_successful:
                logger.warning("Transaction was rolled back, no records were inserted")

        return inserted_count


def _insert_batch(sql_client, batch):
    """Insert a batch of records and return the count of successful insertions."""
    logger = get_run_logger()

    if not batch:
        return 0

    insert_query = """
    INSERT INTO [dbo].[f_imei] 
        ([cliente], [imei], [duracao], [data])
    VALUES 
        (?, ?, ?, ?)
    """

    cursor = sql_client.connection.cursor()
    inserted_in_batch = 0

    try:
        for record in batch:
            try:
                cursor.execute(
                    insert_query,
                    (
                        record["cliente"],
                        record["imei"],
                        record["duracao"],
                        record["data"],
                    ),
                )
                inserted_in_batch += 1
            except Exception as e:
                logger.warning(f"Error inserting record for IMEI {record['imei']}: {e}")
                continue

        logger.info(f"Successfully prepared {inserted_in_batch} records for insertion")
        return inserted_in_batch

    finally:
        cursor.close()


@flow(name="Process Single Date", log_prints=True)
def process_single_date(
    date_str: str, known_imeis: Optional[Set[str]] = None
) -> Dict[str, Any]:
    """
    Process a single date in the ETL flow.

    Args:
        date_str: Date to process in YYYY-MM-DD format
        known_imeis: Optional set of already known IMEIs to avoid duplicate fetching

    Returns:
        Dictionary with statistics about the processing and newly added IMEIs
    """
    logger = get_run_logger()
    logger.info(f"Processing date: {date_str}")

    # Extract IMEIs seen on this date
    imei_data = extract_imeis_for_date(date_str)

    if not imei_data:
        logger.info(f"No IMEI data found for date: {date_str}")
        return {
            "date": date_str,
            "total_imeis": 0,
            "new_imeis": 0,
            "success": True,
            "added_imeis": set(),  # Empty set since nothing was added
        }

    # Get existing IMEIs from the database if not provided
    existing_imeis = known_imeis if known_imeis is not None else get_existing_imeis()

    # Filter to get only new IMEIs
    new_imei_records = [
        record for record in imei_data if record["imei"] not in existing_imeis
    ]

    # Get set of new IMEIs that will be added
    new_imeis_set = {record["imei"] for record in new_imei_records}

    # Insert the new records
    new_imeis_count = insert_new_imeis(new_imei_records)

    logger.info(
        f"IMEI tracking completed for date {date_str}: "
        f"Found {len(imei_data)} total IMEIs, inserted {new_imeis_count} new IMEIs"
    )

    return {
        "date": date_str,
        "total_imeis": len(imei_data),
        "new_imeis": new_imeis_count,
        "success": True,
        "added_imeis": new_imeis_set,  # Return the set of newly added IMEIs
    }


@flow(name="IMEI Tracking Flow", log_prints=True)
def imei_tracking_flow(reset: bool = False) -> Dict[str, Any]:
    """
    Main flow for tracking IMEIs with intelligent date handling.

    This flow:
    1. Checks the last processed date from state storage
    2. If first run or reset=True: processes from the beginning of current month
    3. For daily runs: processes only new dates after the last processed date
    4. Saves state after successful processing

    Args:
        reset: If True, reprocess from the beginning of the current month

    Returns:
        Summary statistics of the ETL operation
    """
    logger = get_run_logger()
    # Get the last processed date from state storage (unless reset is requested)
    last_processed_date = None if reset else get_last_processed_date()

    # Generate dates to process based on last processed date
    dates_to_process = generate_dates_to_process(last_processed_date)

    if not dates_to_process:
        logger.info("No new dates to process")
        return {
            "processed_dates": 0,
            "total_imeis": 0,
            "new_imeis": 0,
            "message": "No new dates to process",
        }

    # Get initial set of existing IMEIs
    known_imeis = get_existing_imeis()

    # Process each date sequentially to ensure proper order
    results = []
    last_date = None

    for date_str in sorted(dates_to_process):
        result = process_single_date(date_str, known_imeis)
        results.append(result)

        # Update the set of known IMEIs with newly added ones
        if result.get("success", False) and "added_imeis" in result:
            known_imeis.update(result["added_imeis"])
            last_date = date_str

    # Aggregate results
    total_imeis = sum(result.get("total_imeis", 0) for result in results)
    new_imeis = sum(result.get("new_imeis", 0) for result in results)

    logger.info(
        f"Completed processing {len(dates_to_process)} dates. "
        f"Total IMEIs processed: {total_imeis}, New IMEIs inserted: {new_imeis}"
    )

    return {
        "processed_dates": len(dates_to_process),
        "total_imeis": total_imeis,
        "new_imeis": new_imeis,
        "last_processed_date": last_date,
        "date_details": results,
    }


if __name__ == "__main__":
    # Run the flow
    # For first run or to reprocess the entire month: imei_tracking_flow(reset=True)
    # For daily runs: imei_tracking_flow()
    imei_tracking_flow()
