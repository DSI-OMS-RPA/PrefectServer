# flows/DSI/cdr_deletion.py
from datetime import datetime
from typing import Dict, List, Any, Tuple
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.artifacts import create_markdown_artifact
import pandas as pd
import uuid  # Add this import

from common.database.mongo_client import MongoDBClient
from common.database.pgsql_client import PostgreSQLClient
from models.cdr_migration import MigrationStatus


def generate_daily_ranges(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    """Generate daily date ranges for granular processing."""
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    return [(d.strftime("%Y-%m-%d"), d.strftime("%Y-%m-%d")) for d in dates]


@task(name="Initialize Deletion Progress", retries=2)
def initialize_deletion_progress(
    date_str: str, data_types: List[str], pg_block_name: str = "postgresql-cdr"
) -> bool:
    """Initialize progress tracking for a deletion operation."""
    logger = get_run_logger()

    with PostgreSQLClient.from_block(pg_block_name) as pg_client:
        # Initialize migration progress (reusing existing table)
        pg_client.initialize_migration_progress(date_str)

        # Update status to indicate this is a deletion operation
        pg_client.update_migration_status(
            date_str,
            MigrationStatus.PENDING.value,  # Use valid status from the enum
            started_at=datetime.now(),
            # Use batch_summary to indicate this is a deletion operation
            batch_summary={"operation_type": "deletion"},
        )

        logger.info(f"Initialized deletion progress tracking for {date_str}")
        return True


@task(name="Get Type Counts for Deletion", retries=2)
def get_type_counts_for_range(
    start_date: str, end_date: str, mongo_block_name: str = "mongodb-imei"
) -> Dict[str, int]:
    """Get document counts by type for date range using optimized queries."""
    logger = get_run_logger()

    # Convert date strings to integers for MongoDB queries
    from datetime import datetime, timedelta

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # For the end date, we need to include the entire day
    next_day = end_dt + timedelta(days=1)

    start_int = int(start_dt.strftime("%Y%m%d"))
    end_int = int(next_day.strftime("%Y%m%d"))

    with MongoDBClient.from_block(mongo_block_name) as mongo_client:
        # Count documents for each data type
        counts = {}
        for data_type in ["long", "int", "decimal"]:
            try:
                # Build appropriate query for each type
                if data_type == "long":
                    query = {
                        "$and": [
                            {"uf301": {"$gte": start_int}},
                            {"uf301": {"$lt": end_int}},
                            {"uf301": {"$type": "long"}},
                        ]
                    }
                elif data_type == "int":
                    query = {
                        "$and": [
                            {"uf301": {"$gte": start_int}},
                            {"uf301": {"$lt": end_int}},
                            {"uf301": {"$type": "int"}},
                        ]
                    }
                else:  # decimal
                    query = {
                        "$and": [
                            {"uf301": {"$gte": float(start_int)}},
                            {"uf301": {"$lt": float(end_int)}},
                            {"uf301": {"$type": "decimal"}},
                        ]
                    }

                # Count documents
                collection = mongo_client.db["cdr"]
                count = collection.count_documents(
                    query, hint=[("uf301", 1), ("_id", 1)]
                )

                counts[data_type] = count
                logger.info(f"{data_type} type: {count:,} documents to delete")

            except Exception as e:
                logger.error(f"Error counting {data_type}: {e}")
                counts[data_type] = 0

    return counts


@task(name="Delete Records by Type", retries=2, retry_delay_seconds=60)
def delete_records_by_type(
    data_type: str,
    start_date: str,
    end_date: str,
    batch_size: int = 10000,
    mongo_block_name: str = "mongodb-imei",
    pg_block_name: str = "postgresql-cdr",
) -> Dict[str, Any]:
    """Delete records of a specific type within a date range."""
    logger = get_run_logger()

    # Convert date strings to integers for MongoDB queries
    from datetime import datetime, timedelta

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # For the end date, we need to include the entire day
    next_day = end_dt + timedelta(days=1)

    start_int = int(start_dt.strftime("%Y%m%d"))
    end_int = int(next_day.strftime("%Y%m%d"))

    date_str = start_date.replace("-", "")

    # Build the query based on data type
    if data_type == "long":
        query = {
            "$and": [
                {"uf301": {"$gte": start_int}},
                {"uf301": {"$lt": end_int}},
                {"uf301": {"$type": "long"}},
            ]
        }
    elif data_type == "int":
        query = {
            "$and": [
                {"uf301": {"$gte": start_int}},
                {"uf301": {"$lt": end_int}},
                {"uf301": {"$type": "int"}},
            ]
        }
    else:  # decimal
        query = {
            "$and": [
                {"uf301": {"$gte": float(start_int)}},
                {"uf301": {"$lt": float(end_int)}},
                {"uf301": {"$type": "decimal"}},
            ]
        }

    try:
        with MongoDBClient.from_block(mongo_block_name) as mongo_client:
            collection = mongo_client.db["cdr"]

            with PostgreSQLClient.from_block(pg_block_name) as pg_client:
                # Generate a proper UUID for extraction_id
                extraction_uuid = str(uuid.uuid4())

                # Update status to extracting (using as "deleting" equivalent)
                pg_client.update_migration_status(
                    date_str,
                    MigrationStatus.EXTRACTING.value,
                    extraction_id=extraction_uuid,  # Use proper UUID
                    batch_summary={
                        "operation_type": "deletion",
                        "data_type": data_type,
                    },
                )

                start_time = datetime.now()
                total_deleted = 0
                batch_count = 0

                # Delete in batches to avoid memory issues and provide progress updates
                while True:
                    # Find batch of records to delete
                    batch_ids = [
                        doc["_id"]
                        for doc in collection.find(
                            query, {"_id": 1}, limit=batch_size
                        ).hint([("uf301", 1), ("_id", 1)])
                    ]

                    if not batch_ids:
                        break

                    batch_count += 1

                    # Delete the batch
                    delete_result = collection.delete_many({"_id": {"$in": batch_ids}})
                    deleted_count = delete_result.deleted_count
                    total_deleted += deleted_count

                    # Log progress
                    logger.info(
                        f"Batch {batch_count}: Deleted {deleted_count} documents "
                        f"({total_deleted} total)"
                    )

                    # Update progress in PostgreSQL
                    pg_client.update_migration_status(
                        date_str,
                        MigrationStatus.EXTRACTED.value,
                        extracted_records=total_deleted,
                        batch_summary={
                            "operation_type": "deletion",
                            "data_type": data_type,
                            "deleted_count": total_deleted,
                        },
                    )

                # Calculate duration
                duration = (datetime.now() - start_time).total_seconds()

                # Update final status
                pg_client.update_migration_status(
                    date_str,
                    MigrationStatus.COMPLETED.value,
                    extracted_records=total_deleted,
                    completed_at=datetime.now(),
                    batch_summary={
                        "operation_type": "deletion",
                        "data_type": data_type,
                        "deleted_count": total_deleted,
                        "duration_seconds": duration,
                    },
                )

                logger.info(
                    f"Completed deletion of {data_type} type: {total_deleted} documents "
                    f"in {duration:.2f} seconds"
                )

                return {
                    "data_type": data_type,
                    "deleted_count": total_deleted,
                    "duration_seconds": duration,
                    "batch_count": batch_count,
                }

    except Exception as e:
        logger.error(f"Error deleting {data_type} records: {e}")

        # Update status to failed
        with PostgreSQLClient.from_block(pg_block_name) as pg_client:
            pg_client.update_migration_status(
                date_str,
                MigrationStatus.FAILED.value,
                error_message=str(e),
                error_count=1,
                batch_summary={
                    "operation_type": "deletion",
                    "data_type": data_type,
                    "error": str(e),
                },
            )

        raise


@flow(
    name="CDR Deletion Flow",
    task_runner=ConcurrentTaskRunner(max_workers=10),
    log_prints=True,
)
def cdr_deletion_flow(
    start_date: str,
    end_date: str,
    processing_mode: str = "daily",
    max_parallel_types: int = 3,
    batch_size: int = 10000,
    mongo_block_name: str = "mongodb-imei",
    pg_block_name: str = "postgresql-cdr",
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Delete CDR records within a specified date range.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        processing_mode: "daily" or "monthly"
        max_parallel_types: Number of data types to process in parallel (1-3)
        batch_size: Documents per deletion batch
        mongo_block_name: MongoDB connection block
        pg_block_name: PostgreSQL connection block
        dry_run: If True, only count records without deleting

    Returns:
        Summary of deletion operation
    """
    logger = get_run_logger()
    deletion_start = datetime.now()

    logger.info(f"Starting CDR deletion for date range: {start_date} to {end_date}")

    if dry_run:
        logger.info("DRY RUN MODE: Records will be counted but not deleted")

    # Generate date ranges
    if processing_mode == "daily":
        date_ranges = generate_daily_ranges(start_date, end_date)
    else:
        # Reuse daily ranges for simplicity since we're only keeping this flow
        date_ranges = generate_daily_ranges(start_date, end_date)

    all_results = []

    for range_start, range_end in date_ranges:
        date_str = range_start.replace("-", "")

        # Initialize progress tracking
        initialize_deletion_progress(
            date_str, ["long", "int", "decimal"], pg_block_name
        )

        # Get type counts
        type_counts = get_type_counts_for_range(
            range_start, range_end, mongo_block_name
        )

        # Skip if no data
        if sum(type_counts.values()) == 0:
            logger.info(f"No data found for {range_start}, skipping")
            all_results.append(
                {
                    "date_range": f"{range_start} to {range_end}",
                    "status": "skipped",
                    "reason": "no_data",
                }
            )
            continue

        # Process each type
        data_types = ["long", "int", "decimal"]
        types_to_process = [
            (dt, type_counts.get(dt, 0))
            for dt in data_types
            if type_counts.get(dt, 0) > 0
        ]

        if dry_run:
            # For dry run, just report counts
            logger.info(
                f"DRY RUN: Would delete {sum(c for _, c in types_to_process)} records:"
            )
            for data_type, count in types_to_process:
                logger.info(f"  {data_type}: {count:,} records")

            all_results.append(
                {
                    "date_range": f"{range_start} to {range_end}",
                    "status": "dry_run",
                    "counts": {dt: cnt for dt, cnt in types_to_process},
                    "total": sum(cnt for _, cnt in types_to_process),
                }
            )
            continue

        # Process types in batches respecting max_parallel_types
        type_results = {}
        total_deleted = 0

        for i in range(0, len(types_to_process), max_parallel_types):
            batch_types = types_to_process[i : i + max_parallel_types]
            type_futures = []

            for data_type, count in batch_types:
                logger.info(f"Submitting {data_type} deletion ({count:,} documents)")
                future = delete_records_by_type.submit(
                    data_type=data_type,
                    start_date=range_start,
                    end_date=range_end,
                    batch_size=batch_size,
                    mongo_block_name=mongo_block_name,
                    pg_block_name=pg_block_name,
                )
                type_futures.append((data_type, future))

            # Wait for this batch of types to complete before starting next batch
            for data_type, future in type_futures:
                try:
                    result = future.result()
                    type_results[data_type] = result
                    total_deleted += result.get("deleted_count", 0)
                    logger.info(
                        f"Completed {data_type} deletion: {result.get('deleted_count', 0):,} documents"
                    )
                except Exception as e:
                    logger.error(f"Failed to process {data_type}: {e}")
                    type_results[data_type] = {"status": "failed", "error": str(e)}

        all_results.append(
            {
                "date_range": f"{range_start} to {range_end}",
                "status": "completed",
                "total_deleted": total_deleted,
                "type_counts": type_counts,
                "type_results": type_results,
            }
        )

    # Generate summary report
    deletion_duration = (datetime.now() - deletion_start).total_seconds()
    total_deleted = sum(
        r.get("total_deleted", 0) for r in all_results if r.get("status") == "completed"
    )

    create_markdown_artifact(
        key="cdr-deletion-report",
        markdown=f"""
# CDR Deletion Report

## Summary
- **Total Documents Deleted**: {total_deleted:,}
- **Date Ranges Processed**: {len(all_results)}
- **Total Duration**: {deletion_duration / 60:.1f} minutes
- **Average Throughput**: {total_deleted / deletion_duration if deletion_duration > 0 else 0:.0f} docs/second
- **Parallel Type Processing**: {max_parallel_types} types concurrently

## Warning
This operation permanently deleted {total_deleted:,} documents from the CDR collection.
This action cannot be undone.
""",
        description="CDR Deletion Report",
    )

    return {
        "status": "completed",
        "total_deleted": total_deleted,
        "duration_seconds": deletion_duration,
        "results": all_results,
    }
