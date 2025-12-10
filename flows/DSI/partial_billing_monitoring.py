# flows/DSI/partial_billing_monitoring.py
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import NONE as NO_CACHE

from common.database.pgsql_client import PostgreSQLClient
from blocks.infrastructure import InfrastructureConfig
from common.utils.ssh import get_ssh_connection, release_ssh_connection, SSH
from flows.DSI.faturacao_duration import billing_process_etl_flow


@task(name="Validate Month Parameter", retries=1)
def validate_month_parameter(month: str) -> Dict[str, str]:
    """
    Validates the month parameter ("mm-yyyy") and constructs directory/file patterns.
    """
    logger = get_run_logger()
    try:
        month_dt = datetime.strptime(month, "%m-%Y")
        year_part, month_part = month_dt.strftime("%Y"), month_dt.strftime("%m")

        remote_base_path = "/opt/portal/7.4/apps/operacao/faturacao/logs"
        remote_log_directory = f"{remote_base_path}/{year_part}_{month_part}/partial"

        log_pattern = f"PARTIAL_BILLING_*_{month_part}_{year_part}.log"

        result = {
            "month": month,
            "year": year_part,
            "month_num": month_part,
            "remote_log_directory": remote_log_directory,
            "log_pattern": log_pattern,
        }
        logger.info(
            f"Validated month {month}. Remote log directory: {result['remote_log_directory']}"
        )
        return result
    except ValueError:
        error_msg = f"Invalid month format. Expected 'mm-yyyy', got: '{month}'"
        logger.error(error_msg)
        raise ValueError(error_msg)


@task(name="Establish BRM SSH Connection", retries=2, retry_delay_seconds=10)
def establish_brm_ssh_connection() -> SSH:
    """Establishes an SSH connection to the BRM server using a Prefect block."""
    logger = get_run_logger()
    try:
        ssh_config_block = InfrastructureConfig.load("brm-ssh")
        ssh_details = ssh_config_block.get_connection_details()
        host, username, password = (
            ssh_details["host"],
            ssh_details["username"],
            ssh_details["password"],
        )

        logger.info(f"Connecting to BRM server: {host}")
        ssh_connection = get_ssh_connection(host, username, password)

        if not ssh_connection or not ssh_connection.is_connected():
            raise ConnectionError("Failed to establish SSH connection to BRM server.")

        logger.info("Successfully connected to BRM server.")
        return ssh_connection
    except Exception as e:
        logger.error(f"Failed to connect to BRM server: {e}")
        raise


@task(
    name="Find Remote Log File",
    retries=2,
    retry_delay_seconds=20,
    cache_policy=NO_CACHE,
)
def find_remote_log_file(
    ssh_connection: SSH, month_info: Dict[str, str]
) -> Optional[str]:
    """
    Finds the partial billing log file using the absolute path of `ls` for maximum reliability.
    Selects the file with the highest index number when multiple files exist.
    """
    logger = get_run_logger()
    remote_dir = month_info["remote_log_directory"]
    log_pattern = month_info["log_pattern"]

    logger.info(
        f"Listing files in '{remote_dir}' to find a match for '{log_pattern}'..."
    )

    # Use ls -l to get detailed file information
    command = f"/bin/ls -l {remote_dir}"
    logger.info(f"Executing remote command: [{command}]")
    success, output = ssh_connection.execute_command(command, timeout=45)

    logger.info(f"Raw output from remote 'ls' command:\n---\n{output}\n---")

    if not success or not output.strip():
        logger.warning(f"Command '{command}' failed or returned no output.")
        return None

    # Base pattern without wildcards
    base_pattern = (
        f"PARTIAL_BILLING_.*_{month_info['month_num']}_{month_info['year']}\.log"
    )
    regex_pattern = re.compile(base_pattern)
    logger.info(f"Using base pattern: {regex_pattern.pattern}")

    # Collect all matching files with their full details
    candidate_files = []
    lines = output.strip().split("\n")
    for line in lines:
        if not line.strip():
            continue

        # Extract filename from ls output (last column)
        parts = line.split()
        if len(parts) < 9:
            continue

        filename = parts[-1]
        clean_filename = filename.strip()

        if regex_pattern.match(clean_filename):
            full_path = f"{remote_dir}/{clean_filename}"
            candidate_files.append(
                {"path": full_path, "filename": clean_filename, "line": line}
            )
            logger.info(f"Found candidate log file: {full_path}")

    if not candidate_files:
        logger.warning(f"No files matched the base pattern '{base_pattern}'.")
        return None

    # If we only have one file, return it immediately
    if len(candidate_files) == 1:
        selected_file = candidate_files[0]["path"]
        logger.info(f"Only one candidate file found: {selected_file}")
        return selected_file

    # Extract index numbers from filenames
    indexed_files = []
    for file_info in candidate_files:
        filename = file_info["filename"]
        base_name = filename.replace(
            f"_{month_info['month_num']}_{month_info['year']}.log", ""
        )

        # Extract index if present
        if "_" in base_name:
            try:
                # Get the last part after the last underscore
                index_part = base_name.split("_")[-1]

                # Try to parse as integer
                index_value = int(index_part) if index_part.isdigit() else 0
            except (ValueError, IndexError):
                index_value = 0
        else:
            index_value = 0

        indexed_files.append(
            {"path": file_info["path"], "filename": filename, "index": index_value}
        )

        logger.info(f"File: {filename} → Index: {index_value}")

    # Sort by index number (highest first)
    indexed_files.sort(key=lambda x: x["index"], reverse=True)

    # Select the file with the highest index
    selected_file = indexed_files[0]["path"]
    logger.info(f"Selected file with highest index: {selected_file}")
    return selected_file


@task(name="Read Remote Log Content", retries=1, cache_policy=NO_CACHE)
def read_remote_log_content(ssh_connection: SSH, log_file_path: str) -> str:
    """Reads the content of the remote log file using the reliable execute_command."""
    logger = get_run_logger()
    logger.info(f"Reading content from remote file: {log_file_path}")

    success, log_content = ssh_connection.execute_command(
        f"/bin/cat '{log_file_path}'", timeout=60
    )
    if not success:
        raise RuntimeError(f"Failed to read content from {log_file_path}")
    logger.info(f"Successfully read {len(log_content)} bytes from remote file.")
    return log_content


@task(
    name="Get Remote File Timestamps",
    retries=2,
    retry_delay_seconds=10,
    cache_policy=NO_CACHE,
)
def get_remote_file_timestamps(
    ssh_connection: SSH, log_file_path: str
) -> Dict[str, datetime]:
    """
    Get file timestamps from remote server via SSH with multiple fallback approaches.
    """
    logger = get_run_logger()

    # List of commands to try, in order of preference
    timestamp_commands = [
        # Method 1: stat with modification time (most common Linux)
        f"/bin/stat -c '%Y' '{log_file_path}'",
        # Method 2: stat in /usr/bin (some systems)
        f"/usr/bin/stat -c '%Y' '{log_file_path}'",
        # Method 3: stat without path (rely on PATH)
        f"stat -c '%Y' '{log_file_path}'",
        # Method 4: BSD/macOS format
        f"stat -f '%m' '{log_file_path}'",
        # Method 5: Use ls -l and parse (universal fallback)
        f"/bin/ls -l '{log_file_path}'",
    ]

    for i, cmd in enumerate(timestamp_commands, 1):
        try:
            logger.info(f"Trying timestamp method {i}: {cmd}")
            success, output = ssh_connection.execute_command(cmd, timeout=30)

            logger.info(f"Method {i} - Success: {success}")
            logger.info(f"Method {i} - Raw output: '{output}'")
            logger.info(f"Method {i} - Output stripped: '{output.strip()}'")

            if not success:
                logger.warning(f"Method {i} command failed, trying next approach...")
                continue

            output = output.strip()
            if not output:
                logger.warning(
                    f"Method {i} returned empty output, trying next approach..."
                )
                continue

            # Handle different output formats
            if cmd.endswith("ls -l"):
                # Parse ls -l output to extract timestamp
                modification_time = parse_ls_timestamp(output, logger)
            else:
                # Parse epoch timestamp from stat command
                try:
                    # Extract numeric timestamp (handle any extra output)
                    timestamp_match = re.search(r"(\d{10,})", output)
                    if timestamp_match:
                        mtime_epoch = int(timestamp_match.group(1))
                        modification_time = datetime.fromtimestamp(mtime_epoch)
                        logger.info(
                            f"Parsed epoch timestamp: {mtime_epoch} -> {modification_time}"
                        )
                    else:
                        logger.warning(
                            f"Could not extract epoch timestamp from: '{output}'"
                        )
                        continue
                except (ValueError, OSError) as e:
                    logger.warning(f"Could not parse epoch timestamp '{output}': {e}")
                    continue

            if modification_time:
                logger.info(
                    f"Successfully got timestamp using method {i}: {modification_time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                return {
                    "modification_time": modification_time,
                    "change_time": modification_time,
                    "creation_time": modification_time,
                }

        except Exception as e:
            logger.warning(f"Method {i} failed with exception: {e}")
            continue

    # If all methods fail, use current time as fallback
    logger.error("All timestamp methods failed, using current time as fallback")
    fallback_time = datetime.now()
    return {
        "modification_time": fallback_time,
        "change_time": fallback_time,
        "creation_time": fallback_time,
    }


def parse_ls_timestamp(ls_output: str, logger) -> Optional[datetime]:
    """Parse timestamp from ls -l output."""
    try:
        # Example: -rw-rw-r-- 1 brm brm 8810 Jun 29 16:23 PARTIAL_BILLING_29_06_2025.log
        lines = ls_output.strip().split("\n")
        for line in lines:
            if "PARTIAL_BILLING" in line:
                parts = line.split()
                if len(parts) >= 8:
                    # Extract date and time parts
                    month = parts[5]  # Jun
                    day = parts[6]  # 29
                    time_part = parts[7]  # 16:23

                    # Construct datetime - assume current year if not specified
                    current_year = datetime.now().year
                    if ":" in time_part:
                        # Format: Jun 29 16:23 (recent file)
                        date_str = f"{month} {day} {current_year} {time_part}"
                        timestamp = datetime.strptime(date_str, "%b %d %Y %H:%M")
                    else:
                        # Format: Jun 29 2025 (older file, year instead of time)
                        year = time_part
                        date_str = f"{month} {day} {year}"
                        timestamp = datetime.strptime(date_str, "%b %d %Y")

                    logger.info(f"Parsed timestamp from ls output: {timestamp}")
                    return timestamp

        logger.warning("Could not parse timestamp from ls output")
        return None

    except Exception as e:
        logger.error(f"Error parsing ls output: {e}")
        return None


@task(name="Parse Remote Log", retries=1)
def parse_remote_log(
    log_content: str, file_timestamps: Dict[str, datetime], month: str
) -> Dict[str, Any]:
    """
    Parses log content to find the earliest timestamp for the start time
    and uses the file's modification time for the end time.
    """
    logger = get_run_logger()

    # Look for timestamps in log content
    timestamps_found = re.findall(
        r"(\w{3} \w{3} \d{1,2} \d{2}:\d{2}:\d{2} \d{4})", log_content
    )

    parsed_timestamps = []
    for ts_str in timestamps_found:
        try:
            parsed_timestamps.append(datetime.strptime(ts_str, "%a %b %d %H:%M:%S %Y"))
        except ValueError:
            continue

    # Use earliest timestamp from log as start time, file timestamp as end time
    start_time = min(parsed_timestamps) if parsed_timestamps else None
    end_time = file_timestamps["modification_time"]

    if not start_time:
        logger.warning(
            "Could not determine start time from log content. Using file modification time as fallback."
        )
        start_time = end_time

    # Calculate duration
    duration = end_time - start_time
    duration_minutes = max(0, int(duration.total_seconds() / 60))

    hours, rem = divmod(duration.total_seconds(), 3600)
    minutes, seconds = divmod(rem, 60)
    duration_hhmmss = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    # Determine status based on log content
    status = "Success" if "Concluido" in log_content else "Failed"

    result = {
        "start_time": start_time,
        "end_time": end_time,
        "duration_minutes": duration_minutes,
        "duration_hhmmss": duration_hhmmss,
        "status": status,
        "month_faturacao": month,
        "log_size_bytes": len(log_content),
        "has_completion_marker": "Concluido" in log_content,
        "timestamps_from_content": len(parsed_timestamps) > 0,
    }

    logger.info(
        f"Parsed log summary: Start={start_time}, End={end_time}, Duration={duration_hhmmss}, Status={status}"
    )
    return result


@task(name="Store Partial Billing Data", retries=2, retry_delay_seconds=30)
def store_partial_billing_data(log_data: Dict[str, Any]) -> None:
    """Stores the partial billing step (step 0) in the database."""
    logger = get_run_logger()
    month = log_data["month_faturacao"]

    with PostgreSQLClient.from_block("postgresql-faturacao") as pg_client:
        # Delete existing partial billing record for this month
        delete_query = "DELETE FROM faturacao_process WHERE month_faturacao = %s AND process_step = 0"
        pg_client.execute_query(delete_query, (month,), fetch=False)
        logger.info(f"Removed any existing partial billing record for month {month}.")

        # Insert new partial billing record
        insert_query = """
        INSERT INTO faturacao_process (
            segment, company, process, process_step, init_dt, end_dt, 
            duration_mn, duration_hhmmss, status, month_faturacao
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        record = (
            0,  # segment (0 for partial billing)
            "DEFAULT",  # company
            "PARTIAL BILLING",  # process
            0,  # process_step (0 for partial billing)
            log_data["start_time"],  # init_dt
            log_data["end_time"],  # end_dt
            log_data["duration_minutes"],  # duration_mn
            log_data["duration_hhmmss"],  # duration_hhmmss
            log_data["status"],  # status
            month,  # month_faturacao
        )
        pg_client.execute_query(insert_query, record, fetch=False)
        logger.info(f"Successfully stored partial billing data for month {month}.")


@task(name="Trigger Billing Process Flow")
def trigger_billing_process_flow(month: str):
    """Triggers the main billing process ETL flow."""
    logger = get_run_logger()
    logger.info(f"Triggering main billing process ETL flow for month: {month}")

    try:
        result = billing_process_etl_flow(month=month)
        logger.info(f"Billing process flow completed successfully: {result}")
        return {"status": "success", "triggered_month": month, "result": result}
    except Exception as e:
        logger.error(f"Billing process flow failed: {e}")
        return {"status": "failed", "triggered_month": month, "error": str(e)}


@flow(name="Partial Billing Monitoring Flow", log_prints=True)
def partial_billing_monitoring_flow(month: str):
    """
    Monitors a remote partial billing log, records its duration, and triggers the main billing flow.
    """
    logger = get_run_logger()
    logger.info(f"Starting Partial Billing Monitoring for month: {month}")
    ssh_connection = None

    try:
        # Step 1: Validate month parameter
        month_info = validate_month_parameter(month)

        # Step 2: Establish SSH connection
        ssh_connection = establish_brm_ssh_connection()

        # Step 3: Find log file
        log_file_path = find_remote_log_file(ssh_connection, month_info)
        if not log_file_path:
            raise FileNotFoundError(
                f"No partial billing log file found for month {month}."
            )

        # Step 4: Read log content
        log_content = read_remote_log_content(ssh_connection, log_file_path)

        # Step 5: Get file timestamps
        file_timestamps = get_remote_file_timestamps(ssh_connection, log_file_path)

        # Step 6: Parse log data
        log_data = parse_remote_log(log_content, file_timestamps, month)

        # Step 7: Store partial billing data
        store_partial_billing_data(log_data)

        # Step 8: Trigger main billing flow if successful
        if log_data["status"] == "Success":
            trigger_result = trigger_billing_process_flow(month)
            logger.info("Partial billing successful. Triggered main billing flow.")
        else:
            logger.warning(
                "Partial billing status was not 'Success'. Main billing flow will not be triggered."
            )
            trigger_result = {"status": "skipped", "reason": "partial_billing_failed"}

        # Create success artifact
        summary = f"""
# Partial Billing Monitoring: SUCCESS

## Month: {month}
**Status**: ✅ COMPLETED

### Partial Billing Process
- **Status**: {log_data['status']}
- **Duration**: {log_data['duration_minutes']} minutes ({log_data['duration_hhmmss']})
- **Start Time**: {log_data['start_time'].strftime('%Y-%m-%d %H:%M:%S')}
- **End Time**: {log_data['end_time'].strftime('%Y-%m-%d %H:%M:%S')}
- **Log File**: `{Path(log_file_path).name}`
- **Log Size**: {log_data['log_size_bytes']:,} bytes
- **Timestamps Source**: {'Log Content' if log_data['timestamps_from_content'] else 'File System'}

### Database Storage
- **Process Step**: 0 (PARTIAL BILLING)
- **Segment**: 0 (No segment)

### Main Billing Flow
- **Triggered**: {'Yes' if trigger_result['status'] == 'success' else 'No'}
- **Reason**: {trigger_result.get('reason', 'Success' if trigger_result['status'] == 'success' else 'Failed')}

### Next Steps
{'The main billing process ETL flow has been automatically triggered.' if trigger_result['status'] == 'success' else 'Fix partial billing issues before running main billing flow.'}
"""

        create_markdown_artifact(
            key=f"partial-billing-summary-{month.replace('-', '')}",
            markdown=summary,
            description=f"Partial Billing Monitoring Summary for {month}",
        )

        logger.info(f"Flow completed successfully for month {month}.")

        return {
            "status": "success",
            "month": month,
            "partial_billing_status": log_data["status"],
            "duration_minutes": log_data["duration_minutes"],
            "log_file_path": log_file_path,
            "main_billing_triggered": trigger_result["status"] == "success",
        }

    except Exception as e:
        error_msg = f"Flow failed for month {month}: {e}"
        logger.error(error_msg, exc_info=True)

        create_markdown_artifact(
            key=f"partial-billing-error-{month.replace('-', '')}",
            markdown=f"""
# Partial Billing Monitoring: FAILED

## Month: {month}
**Status**: ❌ FAILED

### Error Details
```
{error_msg}
```

### Recommended Actions
1. Check SSH connectivity to BRM server
2. Verify log file exists and is accessible
3. Check database connectivity
4. Review error logs for detailed information

**Next Steps**: Resolve the error before attempting to run billing processes.
""",
            description=f"Partial Billing Monitoring Error for {month}",
        )

        return {
            "status": "failed",
            "month": month,
            "error": str(e),
            "main_billing_triggered": False,
        }
    finally:
        if ssh_connection:
            release_ssh_connection(ssh_connection)
            logger.info("SSH connection has been released.")


if __name__ == "__main__":
    partial_billing_monitoring_flow("11-2024")
    # start_dt = datetime(2023, 1, 1)
    # end_dt = datetime(2025, 6, 30)

    # def month_year_range(start: datetime, end: datetime):
    #     """Generate month-year strings from start to end date."""
    #     current = start
    #     while current <= end:
    #         yield current.strftime("%m-%Y")
    #         # Move to the next month
    #         if current.month == 12:
    #             current = datetime(current.year + 1, 1, 1)
    #         else:
    #             current = datetime(current.year, current.month + 1, 1)

    # for m_y in month_year_range(start_dt, end_dt):
    #     #billing_process_etl_flow("08-2024")
    #     partial_billing_monitoring_flow(m_y)  # Format as "mm-yyyy"
