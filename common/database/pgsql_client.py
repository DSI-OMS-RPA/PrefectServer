import psycopg2
import psycopg2.extras
from typing import Dict, List, Optional, Union, Tuple
from contextlib import contextmanager
from common.logging import get_logger
from common.config import config
from blocks.infrastructure import InfrastructureConfig


class PostgreSQLClient:
    """PostgreSQL client for CDR migration progress tracking."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_config: bool = False,
        use_block: str = None,
    ):
        """
        Initialize PostgreSQL client.

        Args:
            host: PostgreSQL hostname
            port: PostgreSQL port
            database: Database name
            username: Username
            password: Password
            use_config: Whether to load from config
            use_block: Whether to use Prefect block
        """
        self.logger = get_logger()

        if use_config:
            self.host = host or config("POSTGRESQL_HOST", "localhost")
            self.port = int(port or config("POSTGRESQL_PORT", 5432))
            self.database = database or config("POSTGRESQL_DATABASE", "cdr_migration")
            self.username = username or config("POSTGRESQL_USERNAME", "postgres")
            self.password = password or config("POSTGRESQL_PASSWORD")
        elif use_block:
            config_block = InfrastructureConfig.load(use_block)
            config_details = config_block.get_connection_details()

            if config_details["type"] != "postgresql":
                raise ValueError(
                    f"Config '{use_block}' is not a PostgreSQL configuration"
                )

            self.host = host or config_details["host"]
            self.port = int(port or config_details["port"] or 5432)
            self.database = database or config_details["database"]
            self.username = username or config_details["username"]
            self.password = password or config_details["password"]
        else:
            self.host = host
            self.port = port
            self.database = database
            self.username = username
            self.password = password

        self.connection = None

    @classmethod
    def from_block(cls, block_name, **kwargs):
        """Create PostgreSQLClient from Prefect block."""
        return cls(use_block=block_name, **kwargs)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        if self.connection is not None:
            return

        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            self.connection.autocommit = True
            self.logger.info(
                f"Connected to PostgreSQL: {self.host}:{self.port}/{self.database}"
            )
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def close(self) -> None:
        """Close PostgreSQL connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("PostgreSQL connection closed")

    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        if not self.connection:
            self.connect()

        old_autocommit = self.connection.autocommit
        self.connection.autocommit = False

        try:
            yield self.connection
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            self.connection.autocommit = old_autocommit

    def execute_query(
        self,
        query: str,
        params: Optional[Union[Dict, Tuple, List]] = None,
        fetch: bool = True,
    ) -> Optional[List[Dict]]:
        """
        Execute a SQL query.

        Args:
            query: SQL query to execute
            params: Query parameters
            fetch: Whether to fetch results

        Returns:
            Query results if fetch=True, None otherwise
        """
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)

                if fetch and cursor.description:
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
                elif not fetch:
                    return [{"rowcount": cursor.rowcount}]
                else:
                    return []
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise

    def execute_many(self, query: str, params_list: List[Union[Dict, Tuple]]) -> int:
        """
        Execute query with multiple parameter sets.

        Args:
            query: SQL query to execute
            params_list: List of parameter sets

        Returns:
            Number of affected rows
        """
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(query, params_list)
                return cursor.rowcount
        except Exception as e:
            self.logger.error(f"Error executing batch query: {e}")
            raise

    def create_schema(self, schema_sql: str) -> None:
        """
        Create database schema from SQL script.

        Args:
            schema_sql: SQL script content
        """
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(schema_sql)
            self.logger.info("Database schema created successfully")
        except Exception as e:
            self.logger.error(f"Error creating schema: {e}")
            raise

    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Name of the table
            schema: Schema name (default: public)

        Returns:
            True if table exists, False otherwise
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = %s
        );
        """
        result = self.execute_query(query, (schema, table_name))
        return result[0]["exists"] if result else False

    # CDR Migration specific methods

    def initialize_migration_progress(self, date_str: str) -> None:
        """Initialize migration progress for a date."""
        query = """
        INSERT INTO migration_progress 
        (date_str, status, started_at, version, error_count)
        VALUES (%s, 'pending', NOW(), 1, 0)
        ON CONFLICT (date_str) DO NOTHING;
        """
        self.execute_query(query, (date_str,), fetch=False)

    def update_migration_status(self, date_str: str, status: str, **kwargs) -> bool:
        # Build dynamic update query
        set_clauses = ["status = %s", "updated_at = NOW()"]
        params = [status]

        for key, value in kwargs.items():
            if key in [
                "extraction_id",
                "source_records",
                "extracted_records",
                "target_collections",  # This is a dictionary!
                "error_count",
                "error_message",
            ]:
                # Handle JSON fields explicitly
                if key in ["target_collections", "error_message"] and isinstance(
                    value, dict
                ):
                    adapted_value = psycopg2.extras.Json(value)
                else:
                    adapted_value = value

                set_clauses.append(f"{key} = %s")
                params.append(adapted_value)

        if status == "completed":
            set_clauses.append("completed_at = NOW()")

        params.append(date_str)

        query = f"""
        UPDATE migration_progress 
        SET {', '.join(set_clauses)}
        WHERE date_str = %s;
        """

        result = self.execute_query(query, params, fetch=False)
        return result[0]["rowcount"] > 0 if result else False

    def get_migration_status(self, date_str: str) -> Optional[Dict]:
        """Get migration status for a date."""
        query = """
        SELECT * FROM migration_progress 
        WHERE date_str = %s;
        """
        result = self.execute_query(query, (date_str,))
        return result[0] if result else None
