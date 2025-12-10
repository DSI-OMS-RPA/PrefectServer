# flows/DSI/reseller_file_processing.py (Updated for SFTP)
import csv
from datetime import datetime, timedelta
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple, Union

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.task_runners import ConcurrentTaskRunner

from blocks.infrastructure import InfrastructureConfig
from common.database.pgsql_client import PostgreSQLClient
from common.utils.ftp import SFTPClient  # Changed from FTPClient
from common.utils.ftp import (CarregamentosRecord, FileMetadata,
                              ProcessingResult, VendasRecord)


@task(name="Parse Date Parameter", retries=1)
def parse_date_parameter(date_input: str) -> Tuple[datetime, datetime, List[str]]:
    """
    Parse date parameter and generate file patterns.

    Args:
        date_input: Either "06-2025" (month) or "14-06-2025" (day)

    Returns:
        Tuple of (start_date, end_date, file_patterns)
    """
    logger = get_run_logger()

    try:
        if len(date_input.split("-")) == 2:
            # Month format: "06-2025"
            month, year = date_input.split("-")
            start_date = datetime(int(year), int(month), 1)

            # Last day of month
            if int(month) == 12:
                end_date = datetime(int(year) + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(int(year), int(month) + 1, 1) - timedelta(days=1)

            # Generate patterns for the entire month
            patterns = []
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime("%Y%m%d")
                patterns.extend(
                    [
                        rf"rm_vendas_{date_str}\d+\.rmven",
                        rf"rm_carregamentos_{date_str}\d+\.rmcarr",
                    ]
                )
                current_date += timedelta(days=1)

            logger.info(
                f"Processing month {date_input}: {start_date.date()} to {end_date.date()}"
            )

        elif len(date_input.split("-")) == 3:
            # Day format: "14-06-2025"
            day, month, year = date_input.split("-")
            start_date = datetime(int(year), int(month), int(day))
            end_date = start_date

            # Generate patterns for specific day
            date_str = start_date.strftime("%Y%m%d")
            patterns = [
                rf"rm_vendas_{date_str}\d+\.rmven",
                rf"rm_carregamentos_{date_str}\d+\.rmcarr",
            ]

            logger.info(f"Processing day {date_input}: {start_date.date()}")

        else:
            raise ValueError(
                f"Invalid date format: {date_input}. Use 'MM-YYYY' or 'DD-MM-YYYY'"
            )

        return start_date, end_date, patterns

    except Exception as e:
        logger.error(f"Error parsing date parameter: {e}")
        raise


@task(name="Test SFTP Connection", retries=2, retry_delay_seconds=30)
def test_sftp_connection(block_name: str = "ftp-reseller-prd") -> Dict[str, Any]:
    """
    Test SFTP connection and return connection details.

    Args:
        block_name: Name of the SSH infrastructure block configured for SFTP

    Returns:
        Connection test results
    """
    logger = get_run_logger()

    try:
        # Get SSH configuration from block (for SFTP use)
        ssh_block = InfrastructureConfig.load(block_name)

        if ssh_block.type != "ssh":
            raise ValueError(
                f"Block '{block_name}' is not an SSH block (type: {ssh_block.type})"
            )

        # Get SSH config and enhance for SFTP
        ssh_config = ssh_block.get_ssh_config()

        # Add SFTP-specific settings from extra_params
        sftp_config = {
            **ssh_config,
            "base_path": ssh_block.extra_params.get("base_path", "/"),
            "host_key_verification": ssh_block.extra_params.get(
                "host_key_verification", "auto"
            ),
            "max_retries": ssh_block.extra_params.get("max_retries", 3),
            "retry_delay": ssh_block.extra_params.get("retry_delay", 5),
        }

        # Test connection using SFTP client
        with SFTPClient(sftp_config) as sftp_client:
            test_result = sftp_client.test_connection()

        if test_result["status"] == "success":
            logger.info(f"SFTP connection test successful: {test_result}")
        else:
            logger.error(f"SFTP connection test failed: {test_result}")

        return test_result

    except Exception as e:
        logger.error(f"Error testing SFTP connection: {e}")
        return {"status": "failed", "connected": False, "error": str(e)}


@task(name="Discover SFTP Files", retries=2, retry_delay_seconds=30)
def discover_sftp_files(
    patterns: List[str],
    file_types: Optional[List[str]] = None,
    block_name: str = "ftp-reseller-prd",
) -> List[FileMetadata]:
    """
    Discover files on SFTP server matching patterns.

    Args:
        patterns: List of regex patterns to match
        file_types: Optional list of file types to filter ['vendas', 'carregamentos']
        block_name: Name of the SSH infrastructure block configured for SFTP

    Returns:
        List of file metadata
    """
    logger = get_run_logger()

    try:
        # Get SSH configuration from block (for SFTP use)
        ssh_block = InfrastructureConfig.load(block_name)

        if ssh_block.type != "ssh":
            raise ValueError(
                f"Block '{block_name}' is not an SSH block (type: {ssh_block.type})"
            )

        # Get SSH config and enhance for SFTP
        ssh_config = ssh_block.get_ssh_config()

        # Add SFTP-specific settings from extra_params
        sftp_config = {
            **ssh_config,
            "base_path": ssh_block.extra_params.get("base_path", "/"),
            "host_key_verification": ssh_block.extra_params.get(
                "host_key_verification", "auto"
            ),
            "max_retries": ssh_block.extra_params.get("max_retries", 3),
            "retry_delay": ssh_block.extra_params.get("retry_delay", 5),
        }

        with SFTPClient(sftp_config) as sftp_client:
            all_files = []

            for pattern in patterns:
                try:
                    files = sftp_client.list_files(pattern)
                    all_files.extend(files)
                    logger.debug(f"Pattern '{pattern}': found {len(files)} files")
                except Exception as e:
                    logger.warning(f"Error with pattern '{pattern}': {e}")
                    continue

            # Remove duplicates based on filename
            unique_files = {}
            for file in all_files:
                unique_files[file.filename] = file
            all_files = list(unique_files.values())

            # Filter by file types if specified
            if file_types:
                all_files = [f for f in all_files if f.file_type in file_types]

            logger.info(f"Discovered {len(all_files)} unique files matching criteria")

            # Log files by type
            vendas_files = [f for f in all_files if f.file_type == "vendas"]
            carregamentos_files = [
                f for f in all_files if f.file_type == "carregamentos"
            ]

            logger.info(f"  Vendas files: {len(vendas_files)}")
            logger.info(f"  Carregamentos files: {len(carregamentos_files)}")

            # Log some example filenames for verification
            if all_files:
                logger.info("Sample discovered files:")
                for file in all_files[:5]:  # Show first 5 files
                    logger.info(
                        f"  {file.filename} ({file.file_type}, {file.size} bytes)"
                    )

            return all_files

    except Exception as e:
        logger.error(f"Error discovering SFTP files: {e}")
        raise


@task(name="Process Reseller File", retries=2, retry_delay_seconds=60)
def process_reseller_file(
    file_metadata: FileMetadata,
    force_reprocess: bool = False,
    block_name: str = "ftp-reseller-prd",
) -> ProcessingResult:
    """
    Process a single reseller file from SFTP to PostgreSQL.

    Args:
        file_metadata: Metadata of the file to process
        force_reprocess: Whether to reprocess even if file already exists
        block_name: Name of the SSH infrastructure block configured for SFTP

    Returns:
        Processing result with statistics
    """
    logger = get_run_logger()
    start_time = datetime.now()

    result = ProcessingResult(
        filename=file_metadata.filename,
        file_type=file_metadata.file_type,
        total_rows=0,
        valid_rows=0,
        invalid_rows=0,
        inserted_rows=0,
        processing_duration=0.0,
        error_messages=[],
        warnings=[],
    )

    try:
        logger.info(
            f"Processing file: {file_metadata.filename} ({file_metadata.size} bytes)"
        )

        # Get SSH configuration from block (for SFTP use)
        ssh_block = InfrastructureConfig.load(block_name)

        if ssh_block.type != "ssh":
            raise ValueError(
                f"Block '{block_name}' is not an SSH block (type: {ssh_block.type})"
            )

        # Get SSH config and enhance for SFTP
        ssh_config = ssh_block.get_ssh_config()

        # Add SFTP-specific settings from extra_params
        sftp_config = {
            **ssh_config,
            "base_path": ssh_block.extra_params.get("base_path", "/"),
            "host_key_verification": ssh_block.extra_params.get(
                "host_key_verification", "auto"
            ),
            "max_retries": ssh_block.extra_params.get("max_retries", 3),
            "retry_delay": ssh_block.extra_params.get("retry_delay", 5),
            "encoding": ssh_block.extra_params.get("encoding", "utf-8"),
        }

        with SFTPClient(sftp_config) as sftp_client:
            # Verify file still exists
            if not sftp_client.file_exists(file_metadata.filename):
                result.error_messages.append("File no longer exists on server")
                return result

            # Download content with encoding handling
            try:
                encoding = sftp_config.get("encoding", "utf-8")
                content = sftp_client.download_file_content(
                    file_metadata.filename, encoding=encoding
                )
            except UnicodeDecodeError:
                # Try different encodings if default fails
                logger.warning(
                    f"UTF-8 decoding failed for {file_metadata.filename}, trying latin-1"
                )
                content = sftp_client.download_file_content(
                    file_metadata.filename, encoding="latin-1"
                )

        if not content.strip():
            result.warnings.append("File is empty")
            return result

        # Parse CSV content
        csv_reader = csv.reader(StringIO(content), delimiter=";")
        records = []

        for row_num, row in enumerate(csv_reader, 1):
            result.total_rows += 1

            try:
                # Remove trailing empty columns (caused by ending semicolons)
                while row and row[-1] == "":
                    row.pop()

                if file_metadata.file_type == "vendas":
                    if len(row) != 13:
                        result.error_messages.append(
                            f"Row {row_num}: Expected 13 columns, got {len(row)}. Row content: {row[:5]}..."
                        )
                        result.invalid_rows += 1
                        continue

                    record = VendasRecord(
                        data=row[0],
                        operacao=row[1] or None,
                        id_u_transacao=row[2] or None,
                        id_p_transacao=row[3] or None,
                        canal=row[4] or None,
                        id_dealer=row[5] or None,
                        sap_user=row[6] or None,
                        mssisdn_dl=row[7] or None,
                        mssisdn_dt=row[8] or None,
                        ponto_venda=row[9] or None,
                        mod_pag=row[10] or None,
                        valor=row[11] or None,
                        comissao=row[12] or None,
                        source_file=file_metadata.filename,
                    )

                elif file_metadata.file_type == "carregamentos":
                    if len(row) != 11:
                        result.error_messages.append(
                            f"Row {row_num}: Expected 11 columns, got {len(row)}. Row content: {row[:5]}..."
                        )
                        result.invalid_rows += 1
                        continue

                    record = CarregamentosRecord(
                        data=row[0],
                        id_dealer=row[1] or None,
                        mssisdn=row[2] or None,
                        id_transacao=row[3] or None,
                        id_correlacao=row[4] or None,
                        canal=row[5] or None,
                        operacao=row[6] or None,
                        valor=row[7] or None,
                        comissao=row[8] or None,
                        tp_ordem=row[9] or None,
                        mdl_pag=row[10] or None,
                        source_file=file_metadata.filename,
                    )

                records.append(record)
                result.valid_rows += 1

            except Exception as e:
                result.error_messages.append(f"Row {row_num}: {str(e)}")
                result.invalid_rows += 1

        # Insert into PostgreSQL
        if records:
            result.inserted_rows = insert_records_to_db(
                records, file_metadata.file_type, force_reprocess
            )

        result.processing_duration = (datetime.now() - start_time).total_seconds()

        logger.info(
            f"Completed {file_metadata.filename}: "
            f"{result.valid_rows}/{result.total_rows} valid rows, "
            f"{result.inserted_rows} inserted in {result.processing_duration:.1f}s"
        )

        return result

    except Exception as e:
        result.error_messages.append(f"File processing failed: {str(e)}")
        result.processing_duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Error processing file {file_metadata.filename}: {e}")
        return result


def insert_records_to_db(
    records: List[Union[VendasRecord, CarregamentosRecord]],
    file_type: str,
    force_reprocess: bool = False,
) -> int:
    """Insert records into PostgreSQL database."""
    logger = get_run_logger()

    try:
        with PostgreSQLClient.from_block("postgresql-reseller-sap") as pg_client:
            table_name = f"reseller_file_{file_type}"
            source_file = records[0].source_file

            # Check for duplicates
            existing_count = pg_client.execute_query(
                f"SELECT COUNT(*) as count FROM {table_name} WHERE source_file = %s",
                (source_file,),
            )[0]["count"]

            if existing_count > 0:
                if force_reprocess:
                    logger.info(
                        f"Force reprocessing enabled: Deleting {existing_count} existing records for {source_file}"
                    )

                    # Use transaction for delete + insert operations
                    with pg_client.transaction():
                        # Delete existing records for this file
                        delete_result = pg_client.execute_query(
                            f"DELETE FROM {table_name} WHERE source_file = %s",
                            (source_file,),
                            fetch=False,
                        )
                        deleted_count = (
                            delete_result[0]["rowcount"] if delete_result else 0
                        )
                        logger.info(
                            f"Deleted {deleted_count} existing records for {source_file}"
                        )

                        # Proceed with insertion within the same transaction
                        inserted_count = _perform_batch_insert(
                            pg_client, records, file_type, table_name
                        )

                    return inserted_count
                else:
                    logger.warning(
                        f"File {source_file} already processed ({existing_count} records exist). Use force_reprocess=True to reprocess."
                    )
                    return 0

            # No duplicates, proceed with normal insertion
            return _perform_batch_insert(pg_client, records, file_type, table_name)

    except Exception as e:
        logger.error(f"Database insertion failed: {e}")
        raise


def _perform_batch_insert(
    pg_client: PostgreSQLClient,
    records: List[Union[VendasRecord, CarregamentosRecord]],
    file_type: str,
    table_name: str,
) -> int:
    """Perform the actual batch insert operation."""
    logger = get_run_logger()

    try:
        # Prepare insert query based on file type
        if file_type == "vendas":
            insert_query = """
            INSERT INTO reseller_file_vendas (
                data, operacao, id_u_transacao, id_p_transacao, canal, id_dealer,
                sap_user, mssisdn_dl, mssisdn_dt, ponto_venda, mod_pag, valor, comissao, source_file
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            insert_data = [
                (
                    r.data,
                    r.operacao,
                    r.id_u_transacao,
                    r.id_p_transacao,
                    r.canal,
                    r.id_dealer,
                    r.sap_user,
                    r.mssisdn_dl,
                    r.mssisdn_dt,
                    r.ponto_venda,
                    r.mod_pag,
                    r.valor,
                    r.comissao,
                    r.source_file,
                )
                for r in records
            ]

        else:  # carregamentos
            insert_query = """
            INSERT INTO reseller_file_carregamentos (
                data, id_dealer, mssisdn, id_transacao, id_correlacao, canal,
                operacao, valor, comissao, tp_ordem, mdl_pag, source_file
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            insert_data = [
                (
                    r.data,
                    r.id_dealer,
                    r.mssisdn,
                    r.id_transacao,
                    r.id_correlacao,
                    r.canal,
                    r.operacao,
                    r.valor,
                    r.comissao,
                    r.tp_ordem,
                    r.mdl_pag,
                    r.source_file,
                )
                for r in records
            ]

        # Batch insert
        inserted_count = pg_client.execute_many(insert_query, insert_data)
        logger.info(f"Inserted {inserted_count} records into {table_name}")

        return inserted_count

    except Exception as e:
        logger.error(f"Batch insert failed for {table_name}: {e}")
        raise


@flow(
    name="SFTP Reseller File Processing",  # Updated flow name
    task_runner=ConcurrentTaskRunner(max_workers=2),
    log_prints=True,
)
def sftp_reseller_file_processing_flow(
    date_input: str,
    file_types: Optional[List[str]] = None,
    force_reprocess: bool = False,
    sftp_block_name: str = "ftp-reseller-prd",
    test_connection: bool = True,
) -> Dict[str, Any]:
    """
    Main flow for processing reseller files from SFTP server.

    Args:
        date_input: Date to process in "MM-YYYY" or "DD-MM-YYYY" format
        file_types: Optional list of file types to process ['vendas', 'carregamentos']
        force_reprocess: If True, reprocess files even if they already exist in database
        sftp_block_name: Name of the SSH infrastructure block configured for SFTP
        test_connection: If True, test SFTP connection before processing

    Returns:
        Processing summary with statistics
    """
    logger = get_run_logger()
    flow_start_time = datetime.now()

    logger.info(
        f"Starting SFTP Reseller File Processing for: {date_input} (force_reprocess={force_reprocess})"
    )
    logger.info(f"Using SSH block configured for SFTP: {sftp_block_name}")

    # Test SFTP connection if requested
    if test_connection:
        connection_test = test_sftp_connection(sftp_block_name)
        if connection_test["status"] != "success":
            logger.error(f"SFTP connection test failed: {connection_test}")
            return {
                "status": "failed",
                "error": "SFTP connection test failed",
                "connection_test": connection_test,
                "date_input": date_input,
            }
        logger.info("SFTP connection test passed")

    # Parse date parameter
    start_date, end_date, patterns = parse_date_parameter(date_input)

    # Discover files
    discovered_files = discover_sftp_files(patterns, file_types, sftp_block_name)

    if not discovered_files:
        logger.warning("No files found matching criteria")
        return {
            "status": "completed",
            "message": "No files found",
            "date_input": date_input,
            "files_discovered": 0,
            "files_processed": 0,
            "total_records": 0,
            "sftp_block_used": sftp_block_name,
        }

    # Process files in parallel
    processing_futures = []
    for file_metadata in discovered_files:
        future = process_reseller_file.submit(
            file_metadata, force_reprocess, sftp_block_name
        )
        processing_futures.append(future)

    # Collect results
    results = []
    failed_files = []

    for i, future in enumerate(processing_futures):
        try:
            result = future.result()
            results.append(result)

            # Log individual file completion
            if result.error_messages:
                logger.warning(
                    f"File {result.filename} completed with {len(result.error_messages)} errors"
                )
                failed_files.append(result.filename)
            else:
                logger.info(
                    f"File {result.filename} processed successfully: {result.inserted_rows} records inserted"
                )

        except Exception as e:
            logger.error(f"Failed to get result from future {i}: {e}")
            failed_files.append(
                discovered_files[i].filename
                if i < len(discovered_files)
                else f"Unknown file {i}"
            )

    # Calculate summary statistics
    total_files_discovered = len(discovered_files)
    total_files_processed = len(results)
    total_records = sum(r.total_rows for r in results)
    total_valid_records = sum(r.valid_rows for r in results)
    total_inserted_records = sum(r.inserted_rows for r in results)
    total_errors = sum(len(r.error_messages) for r in results)

    processing_duration = (datetime.now() - flow_start_time).total_seconds()

    # Generate artifacts
    create_processing_summary_artifact(
        date_input,
        discovered_files,
        results,
        processing_duration,
        force_reprocess,
        sftp_block_name,
    )

    create_file_details_artifact(results)

    if total_errors > 0 or failed_files:
        create_error_report_artifact(results, failed_files)

    # Return summary
    summary = {
        "status": "completed",
        "date_input": date_input,
        "date_range": f"{start_date.date()} to {end_date.date()}",
        "sftp_block_used": sftp_block_name,
        "files_discovered": total_files_discovered,
        "files_processed": total_files_processed,
        "files_failed": len(failed_files),
        "failed_files": failed_files,
        "total_records": total_records,
        "valid_records": total_valid_records,
        "inserted_records": total_inserted_records,
        "error_count": total_errors,
        "processing_duration_seconds": processing_duration,
        "throughput_records_per_second": total_valid_records / processing_duration
        if processing_duration > 0
        else 0,
        "force_reprocess": force_reprocess,
        "success_rate": (total_files_processed - len(failed_files))
        / total_files_processed
        * 100
        if total_files_processed > 0
        else 0,
    }

    logger.info(f"SFTP Reseller File Processing completed: {summary}")

    return summary


def create_processing_summary_artifact(
    date_input: str,
    discovered_files: List[FileMetadata],
    results: List[ProcessingResult],
    processing_duration: float,
    force_reprocess: bool = False,
    sftp_block_name: str = "ftp-reseller-prd",
):
    """Create processing summary artifact."""

    # Group results by file type
    vendas_results = [r for r in results if r.file_type == "vendas"]
    carregamentos_results = [r for r in results if r.file_type == "carregamentos"]

    summary_data = [
        {
            "Metric": "Date Input",
            "Value": date_input,
            "Details": f"Force Reprocess: {'Yes' if force_reprocess else 'No'}",
        },
        {
            "Metric": "SFTP Block",
            "Value": sftp_block_name,
            "Details": "Infrastructure configuration used",
        },
        {
            "Metric": "Files Discovered",
            "Value": len(discovered_files),
            "Details": f"Vendas: {len([f for f in discovered_files if f.file_type == 'vendas'])}, "
            f"Carregamentos: {len([f for f in discovered_files if f.file_type == 'carregamentos'])}",
        },
        {
            "Metric": "Files Processed",
            "Value": len(results),
            "Details": f"Vendas: {len(vendas_results)}, Carregamentos: {len(carregamentos_results)}",
        },
        {
            "Metric": "Total Records",
            "Value": sum(r.total_rows for r in results),
            "Details": f"Valid: {sum(r.valid_rows for r in results)}, "
            f"Invalid: {sum(r.invalid_rows for r in results)}",
        },
        {
            "Metric": "Inserted Records",
            "Value": sum(r.inserted_rows for r in results),
            "Details": f"Vendas: {sum(r.inserted_rows for r in vendas_results)}, "
            f"Carregamentos: {sum(r.inserted_rows for r in carregamentos_results)}",
        },
        {
            "Metric": "Processing Duration",
            "Value": f"{processing_duration:.1f}s",
            "Details": f"Avg per file: {processing_duration/len(results):.1f}s"
            if results
            else "",
        },
        {
            "Metric": "Throughput",
            "Value": f"{sum(r.valid_rows for r in results)/processing_duration:.0f} records/sec"
            if processing_duration > 0
            else "N/A",
            "Details": f"Total filename chars: {sum(len(f.filename) for f in discovered_files)}",
        },
        {
            "Metric": "Error Count",
            "Value": sum(len(r.error_messages) for r in results),
            "Details": f"Files with errors: {len([r for r in results if r.error_messages])}",
        },
    ]

    create_table_artifact(
        key="sftp-reseller-file-processing-summary",  # Updated key
        table=summary_data,
        description=f"SFTP Reseller File Processing Summary - {date_input}",
    )


def create_file_details_artifact(results: List[ProcessingResult]):
    """Create detailed file processing results artifact."""

    file_details = [
        {
            "Filename": r.filename,
            "Type": r.file_type,
            "Total Rows": r.total_rows,
            "Valid Rows": r.valid_rows,
            "Invalid Rows": r.invalid_rows,
            "Inserted Rows": r.inserted_rows,
            "Success Rate": f"{(r.valid_rows/r.total_rows*100):.1f}%"
            if r.total_rows > 0
            else "0%",
            "Duration (s)": f"{r.processing_duration:.1f}",
            "Throughput (rows/s)": f"{r.valid_rows/r.processing_duration:.0f}"
            if r.processing_duration > 0
            else "N/A",
            "Errors": len(r.error_messages),
            "Warnings": len(r.warnings),
        }
        for r in results
    ]

    create_table_artifact(
        key="sftp-reseller-file-details",
        table=file_details,
        description="Detailed SFTP File Processing Results",
    )


def create_error_report_artifact(
    results: List[ProcessingResult], failed_files: List[str]
):
    """Create error report artifact."""

    error_report_content = "# SFTP File Processing Error Report\n\n"

    if failed_files:
        error_report_content += f"## Failed Files ({len(failed_files)})\n\n"
        for filename in failed_files:
            error_report_content += f"- {filename}\n"
        error_report_content += "\n"

    files_with_errors = [r for r in results if r.error_messages or r.warnings]

    if not files_with_errors:
        error_report_content += "No processing errors or warnings found.\n"
    else:
        error_report_content += (
            f"## Files with Processing Issues ({len(files_with_errors)})\n\n"
        )

        for result in files_with_errors:
            error_report_content += f"### {result.filename}\n\n"

            if result.error_messages:
                error_report_content += "**Errors:**\n"
                for error in result.error_messages:
                    error_report_content += f"- {error}\n"
                error_report_content += "\n"

            if result.warnings:
                error_report_content += "**Warnings:**\n"
                for warning in result.warnings:
                    error_report_content += f"- {warning}\n"
                error_report_content += "\n"

    create_markdown_artifact(
        key="sftp-reseller-file-error-report",
        markdown=error_report_content,
        description="SFTP File Processing Error and Warning Report",
    )


# Backward compatibility alias
ftp_reseller_file_processing_flow = sftp_reseller_file_processing_flow


if __name__ == "__main__":

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

    # Test the flow
    # Normal processing
    # sftp_reseller_file_processing_flow("01-2025")
    start_dt = datetime(2025, 1, 1)
    end_dt = datetime(2025, 9, 30)
    for m_y in month_year_range(start_dt, end_dt):
        # billing_process_etl_flow("08-2024")
        sftp_reseller_file_processing_flow(m_y)  # Format as "mm-yyyy"

    # Force reprocessing with specific SFTP block
    # sftp_reseller_file_processing_flow("15-01-2025", force_reprocess=True, sftp_block_name="sftp-reseller-dev")

    # Process only specific file types
    # sftp_reseller_file_processing_flow("06-2025", file_types=["vendas"])
