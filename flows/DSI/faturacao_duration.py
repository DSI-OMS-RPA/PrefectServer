# flows/faturacao/billing_process_flow.py
from datetime import date, datetime
from typing import Any, Dict, List
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact

from common.database.oracle_client import OracleClient
from common.database.pgsql_client import PostgreSQLClient
from queries.loader import load_sql_query

import calendar


@task(name="Validate Month Parameter", retries=1)
def validate_month_parameter(month: str) -> Dict[str, str]:
    """
    Validate and convert month parameter to date range.

    Args:
        month: Month in "mm-yyyy" format (e.g., "03-2025")

    Returns:
        Dictionary with formatted dates for the query
    """
    logger = get_run_logger()

    try:
        # Parse the month parameter
        month_part, year_part = month.split("-")
        month_int = int(month_part)
        year_int = int(year_part)

        # Validate month range
        if not (1 <= month_int <= 12):
            raise ValueError(f"Month must be between 01 and 12, got: {month_part}")

        # Apply business rule: use day 29 when possible, day 28 for February in non-leap years
        def get_target_day(year: int, month: int) -> int:
            if month == 2:  # February
                return 29 if calendar.isleap(year) else 28
            else:
                return 29  # All other months have at least 30 days

        # Create start date (29th of the target month, or 28th for February in non-leap years)
        start_day = get_target_day(year_int, month_int)
        start_date = date(year_int, month_int, start_day)

        # Create end date (same rule for next month)
        if month_int == 12:
            # Handle December -> January transition
            next_year = year_int + 1
            next_month = 1
        else:
            next_year = year_int
            next_month = month_int + 1

        end_day = get_target_day(next_year, next_month)
        end_date = date(next_year, next_month, end_day)

        result = {
            "execution_start_date": start_date.strftime("%d-%m-%Y"),
            "execution_end_date": end_date.strftime("%d-%m-%Y"),
            "month_faturacao": month,
        }

        logger.info(
            f"Date range calculated: {result['execution_start_date']} to {result['execution_end_date']}"
        )
        return result

    except ValueError as ve:
        # Handle month parsing errors specifically
        if "day is out of range" in str(ve):
            logger.error(f"Date calculation error for month '{month}': {ve}")
            raise ValueError(f"Invalid date calculation for month: '{month}'")
        elif "invalid literal" in str(ve):
            logger.error(f"Month format parsing error: {ve}")
            raise ValueError(
                f"Invalid month format. Expected 'mm-yyyy', got: '{month}'"
            )
        else:
            raise ve
    except Exception as e:
        logger.error(f"Error validating month parameter '{month}': {e}")
        raise ValueError(f"Invalid month format. Expected 'mm-yyyy', got: '{month}'")


@task(name="Extract Billing Process Data", retries=2, retry_delay_seconds=30)
def extract_billing_process_data(date_config: Dict[str, str]) -> List[Dict]:
    """
    Extract billing process data from Oracle database using the complete query.

    Args:
        date_config: Dictionary with date range configuration

    Returns:
        List of billing process records
    """
    logger = get_run_logger()

    try:
        # Load the SQL query from file
        query_template = load_sql_query("DSI/tempos-faturacao")

        with OracleClient.from_block("brm-oracle") as oracle_client:
            logger.info(
                f"Executing billing process extraction for month: {date_config['month_faturacao']}"
            )

            # Execute the templated query
            results = oracle_client.execute_templated_query(query_template, date_config)

            logger.info(
                f"Successfully extracted {len(results)} billing process records"
            )

            return results

    except Exception as e:
        logger.error(f"Error extracting billing process data: {e}")
        raise


@task(name="Validate and Filter Data")
def validate_and_filter_data(records: List[Dict], month: str) -> Dict[str, Any]:
    """
    Validate and filter extracted data based on business rules.

    Args:
        records: Raw extracted records
        month: Month being processed

    Returns:
        Dictionary containing filtered records and validation statistics
    """
    logger = get_run_logger()

    # Define valid segments
    VALID_SEGMENTS = {"101", "102", "201", "401", "402", "302", "303", "301"}

    # Validation counters
    valid_records = []
    invalid_segment_count = 0
    zero_duration_count = 0
    total_input_records = len(records)

    for record in records:
        segment = str(record.get("SEGMENT", "")).strip()
        duration_mn = record.get("DURATION_MN")

        # Check segment validity
        if segment not in VALID_SEGMENTS:
            invalid_segment_count += 1
            logger.debug(f"Skipping record with invalid segment: {segment}")
            continue

        # Check duration (skip if 0 or None)
        if duration_mn is None or duration_mn == 0:
            zero_duration_count += 1
            logger.debug(
                f"Skipping record with zero/null duration: {segment}-{record.get('COMPANY')}-{record.get('PROCESS')}"
            )
            continue

        # Record is valid
        valid_records.append(record)

    # Calculate statistics
    validation_stats = {
        "total_input_records": total_input_records,
        "valid_records": len(valid_records),
        "invalid_segment_count": invalid_segment_count,
        "zero_duration_count": zero_duration_count,
        "total_filtered_out": invalid_segment_count + zero_duration_count,
        "validity_rate": (len(valid_records) / total_input_records * 100)
        if total_input_records > 0
        else 0,
    }

    logger.info(f"Data validation for {month}:")
    logger.info(f"  Total input records: {total_input_records}")
    logger.info(f"  Valid records: {len(valid_records)}")
    logger.info(f"  Invalid segments filtered: {invalid_segment_count}")
    logger.info(f"  Zero duration filtered: {zero_duration_count}")
    logger.info(f"  Validity rate: {validation_stats['validity_rate']:.1f}%")

    if invalid_segment_count > 0:
        logger.warning(f"Found {invalid_segment_count} records with invalid segments")
    if zero_duration_count > 0:
        logger.warning(f"Found {zero_duration_count} records with zero/null duration")

    return {"filtered_records": valid_records, "validation_stats": validation_stats}


@task(name="Validate Data Completeness")
def validate_data_completeness(records: List[Dict], month: str) -> Dict[str, Any]:
    """
    Validate that we have complete process chains per segment.
    """
    logger = get_run_logger()

    expected_processes = {
        "BILLING",
        "INVOICING",
        "RELATORIO PROVISORIO",
        "RELATORIO DEFINITIVA",
    }

    # Group by segment and company
    segment_processes = {}
    for record in records:
        key = (record["SEGMENT"], record["COMPANY"])
        if key not in segment_processes:
            segment_processes[key] = set()
        segment_processes[key].add(record["PROCESS"])

    # Check completeness
    complete_segments = []
    incomplete_segments = []

    for (segment, company), processes in segment_processes.items():
        if processes == expected_processes:
            complete_segments.append(f"{segment}-{company}")
        else:
            missing = expected_processes - processes
            incomplete_segments.append(
                {
                    "segment": segment,
                    "company": company,
                    "present_processes": list(processes),
                    "missing_processes": list(missing),
                }
            )

    validation_result = {
        "month": month,
        "total_segments": len(segment_processes),
        "complete_segments": len(complete_segments),
        "incomplete_segments": len(incomplete_segments),
        "incomplete_details": incomplete_segments,
        "completeness_rate": len(complete_segments) / len(segment_processes) * 100
        if segment_processes
        else 0,
    }

    logger.info(
        f"Data completeness for {month}: {validation_result['completeness_rate']:.1f}% complete"
    )

    if incomplete_segments:
        logger.warning(f"Found {len(incomplete_segments)} incomplete segments:")
        for incomplete in incomplete_segments:
            logger.warning(
                f"  {incomplete['segment']}-{incomplete['company']}: missing {incomplete['missing_processes']}"
            )

    return validation_result


@task(name="Store to Table", retries=2, retry_delay_seconds=30)
def store_to_table(records: List[Dict], month: str) -> Dict[str, int]:
    """
    Store billing process records to PostgreSQL database with process step tracking.
    FIXED: Only delete Oracle-based records (steps 1-4), preserve PARTIAL BILLING (step 0).
    """
    logger = get_run_logger()

    if not records:
        logger.info("No records to store")
        return {"inserted": 0, "updated": 0, "errors": 0}

    # Process step mapping for flow tracking
    PROCESS_STEP_MAPPING = {
        "BILLING": 1,
        "INVOICING": 2,
        "RELATORIO PROVISORIO": 3,
        "RELATORIO DEFINITIVA": 4,
    }

    try:
        with PostgreSQLClient.from_block("postgresql-faturacao") as pg_client:
            # FIXED: Delete only Oracle-based records (steps 1-4), preserve PARTIAL BILLING (step 0)
            delete_query = """
            DELETE FROM faturacao_process 
            WHERE month_faturacao = %s 
            AND process_step BETWEEN 1 AND 4
            """
            delete_result = pg_client.execute_query(delete_query, (month,), fetch=False)
            deleted_count = delete_result[0].get("rowcount", 0) if delete_result else 0

            logger.info(
                f"Deleted {deleted_count} existing Oracle-based records for month {month} (preserved PARTIAL BILLING step 0)"
            )

            # Insert query now includes process_step column
            insert_query = """
            INSERT INTO faturacao_process (
                segment, company, process, process_step, init_dt, end_dt, 
                duration_mn, duration_hhmmss, status, month_faturacao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Convert records to tuples for batch insert
            insert_data = []
            skipped_records = 0

            for record in records:
                process_name = record["PROCESS"]
                process_step = PROCESS_STEP_MAPPING.get(process_name)

                # Validate that we have a valid process step mapping
                if process_step is None:
                    logger.warning(
                        f"Unknown process '{process_name}' - skipping record for {record.get('SEGMENT')}-{record.get('COMPANY')}"
                    )
                    skipped_records += 1
                    continue

                # Tuple now includes process_step
                insert_data.append(
                    (
                        record["SEGMENT"],
                        record["COMPANY"],
                        record["PROCESS"],
                        process_step,  # Process step for flow tracking
                        record["INIT_DT"],
                        record["END_DT"],
                        record["DURATION_MN"],
                        record["DURATION_HHMMSS"],
                        record["STATUS"],
                        record["MONTH_FATURACAO"],
                    )
                )

            # Log any skipped records
            if skipped_records > 0:
                logger.warning(
                    f"Skipped {skipped_records} records due to unknown process names"
                )

            # Batch insert
            inserted_count = pg_client.execute_many(insert_query, insert_data)

            # Validate process flows after insertion
            flow_validation = validate_process_flows(pg_client, month)

            stats = {
                "inserted": inserted_count,
                "updated": 0,
                "deleted": deleted_count,
                "errors": 0,
                "skipped": skipped_records,
                "total_processed": len(records),
                "flow_validation": flow_validation,
            }

            logger.info(f"Store completed for month {month}: {stats}")
            return stats

    except Exception as e:
        logger.error(f"Error storing data to PostgreSQL: {e}")
        raise


def validate_process_flows(pg_client: PostgreSQLClient, month: str) -> Dict[str, Any]:
    """
    Validate that process flows are complete after insertion.

    Args:
        pg_client: PostgreSQL client
        month: Month being processed

    Returns:
        Flow validation statistics
    """
    logger = get_run_logger()

    try:
        # Check flow completeness using the new process_step column
        validation_query = """
        SELECT 
            segment,
            company,
            COUNT(*) as total_steps,
            MAX(process_step) as highest_step,
            COUNT(DISTINCT process_step) as unique_steps,
            CASE 
                WHEN COUNT(DISTINCT process_step) = 4 AND MAX(process_step) = 4 THEN 'Complete'
                ELSE 'Incomplete'
            END as flow_status,
            ARRAY_AGG(process_step ORDER BY process_step) as steps_present
        FROM faturacao_process 
        WHERE month_faturacao = %s
        GROUP BY segment, company
        ORDER BY segment, company
        """

        results = pg_client.execute_query(validation_query, (month,))

        complete_flows = len([r for r in results if r["flow_status"] == "Complete"])
        incomplete_flows = len([r for r in results if r["flow_status"] == "Incomplete"])

        validation_stats = {
            "total_flows": len(results),
            "complete_flows": complete_flows,
            "incomplete_flows": incomplete_flows,
            "completion_rate": (complete_flows / len(results) * 100) if results else 0,
            "incomplete_details": [
                {
                    "segment": r["segment"],
                    "company": r["company"],
                    "steps_present": r["steps_present"],
                    "missing_steps": [
                        i for i in range(1, 5) if i not in r["steps_present"]
                    ],
                }
                for r in results
                if r["flow_status"] == "Incomplete"
            ],
        }

        logger.info(
            f"Flow validation: {complete_flows}/{len(results)} flows complete ({validation_stats['completion_rate']:.1f}%)"
        )

        if incomplete_flows > 0:
            logger.warning(f"Found {incomplete_flows} incomplete flows")
            for detail in validation_stats["incomplete_details"]:
                logger.warning(
                    f"  {detail['segment']}-{detail['company']}: missing steps {detail['missing_steps']}"
                )

        return validation_stats

    except Exception as e:
        logger.error(f"Error validating process flows: {e}")
        return {"error": str(e)}


@flow(name="Billing Process ETL Flow", log_prints=True)
def billing_process_etl_flow(month: str) -> Dict:
    """
    Main ETL flow with comprehensive data validation and quality monitoring.

    Args:
        month: Month to process in "mm-yyyy" format (e.g., "03-2025")

    Returns:
        Summary of the ETL operation including validation statistics
    """
    logger = get_run_logger()
    logger.info(f"Starting Billing Process ETL Flow for month: {month}")

    # Validate month parameter and calculate date range
    date_config = validate_month_parameter(month)

    # Extract data from Oracle
    raw_billing_data = extract_billing_process_data(date_config)

    # Validate and filter data
    validation_result = validate_and_filter_data(raw_billing_data, month)
    filtered_data = validation_result["filtered_records"]
    validation_stats = validation_result["validation_stats"]

    # Validate data completeness
    completeness_report = validate_data_completeness(filtered_data, month)

    # Store data to PostgreSQL
    store_stats = store_to_table(filtered_data, month)

    # Create comprehensive summary artifact
    summary_data = [
        {
            "Month": month,
            "Raw Records Extracted": validation_stats["total_input_records"],
            "Valid Records": validation_stats["valid_records"],
            "Invalid Segments Filtered": validation_stats["invalid_segment_count"],
            "Zero Duration Filtered": validation_stats["zero_duration_count"],
            "Validity Rate": f"{validation_stats['validity_rate']:.1f}%",
            "Records Stored": store_stats["inserted"],
            "Total Segments": completeness_report["total_segments"],
            "Complete Segments": completeness_report["complete_segments"],
            "Completeness Rate": f"{completeness_report['completeness_rate']:.1f}%",
            "Errors": store_stats["errors"],
        }
    ]

    create_table_artifact(
        key=f"billing-process-summary-{month.replace('-', '')}",
        table=summary_data,
        description=f"Billing Process ETL Summary for {month}",
    )

    # FIXED: Check incomplete_details (list) instead of incomplete_segments (int)
    if completeness_report["incomplete_details"]:
        incomplete_details = [
            {
                "Segment": item["segment"],
                "Company": item["company"],
                "Present Processes": ", ".join(item["present_processes"]),
                "Missing Processes": ", ".join(item["missing_processes"]),
            }
            for item in completeness_report["incomplete_details"]
        ]

        create_table_artifact(
            key=f"data-quality-issues-{month.replace('-', '')}",
            table=incomplete_details,
            description=f"Data Quality Issues for {month} - Incomplete Process Chains",
        )

    # Create validation artifact if significant data was filtered
    if validation_stats["total_filtered_out"] > 0:
        validation_details = [
            {
                "Filter Type": "Invalid Segments",
                "Records Filtered": validation_stats["invalid_segment_count"],
                "Description": "Segments not in valid list: 101,102,201,401,402,302,303,301",
            },
            {
                "Filter Type": "Zero Duration",
                "Records Filtered": validation_stats["zero_duration_count"],
                "Description": "Records with DURATION_MN = 0 or NULL",
            },
        ]

        create_table_artifact(
            key=f"validation-filters-{month.replace('-', '')}",
            table=validation_details,
            description=f"Data Validation Filters Applied for {month}",
        )

    logger.info(f"Billing Process ETL Flow completed for month {month}")

    return {
        "status": "success",
        "month": month,
        "raw_records_extracted": validation_stats["total_input_records"],
        "valid_records": validation_stats["valid_records"],
        "records_stored": store_stats["inserted"],
        "validity_rate": validation_stats["validity_rate"],
        "completeness_rate": completeness_report["completeness_rate"],
        "invalid_segments_filtered": validation_stats["invalid_segment_count"],
        "zero_duration_filtered": validation_stats["zero_duration_count"],
        "incomplete_segments": completeness_report["incomplete_segments"],
        "errors": store_stats["errors"],
    }


if __name__ == "__main__":
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2025, 6, 30)

    def month_year_range(start: datetime, end: datetime):
        """Generate month-year strings from start to end date."""
        current = start
        while current <= end:
            yield current.strftime("%m-%Y")
            # Move to the next month
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)

    for m_y in month_year_range(start_dt, end_dt):
        # billing_process_etl_flow("08-2024")
        billing_process_etl_flow(m_y)  # Format as "mm-yyyy"
