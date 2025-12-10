import platform
from typing import List, Dict, Any, Optional, Union, Tuple

from prefect.variables import Variable

from common.logging import get_logger
from common.config import config
from blocks.infrastructure import InfrastructureConfig
import oracledb


if platform.system() == "Windows":
    oracledb.init_oracle_client(config("ORACLE_CLIENT_DIR_WIN"))
elif platform.system() == "Linux":
    client_dir = Variable.get("oracle_client_dir")
    oracledb.init_oracle_client(client_dir)


class OracleClient:
    """Oracle database client for ETL operations."""

    def __init__(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            service_name: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            use_config: bool = False,
            use_block: str = None,
    ):
        """
        Initialize Oracle client.

        Args:
            host: Oracle hostname
            port: Oracle port (default: 1521)
            service_name: Oracle service name
            username: Oracle username
            password: Oracle password
            use_config: Whether to try loading values from config if params are None
            use_block: Whether to use Prefect block to retrieve secret information
        """
        self.logger = get_logger()

        if use_config:
            self.host = host or config("ORACLE_HOST")
            self.port = int(port or config("ORACLE_PORT", 1521))
            self.service_name = service_name or config("ORACLE_SERVICE_NAME")
            self.username = username or config("ORACLE_USERNAME")
            self.password = password or config("ORACLE_PWD")
        elif use_block:
            try:
                config_block = InfrastructureConfig.load(use_block)
                config_details = config_block.get_connection_details()

                if config_details["type"] != "oracle":
                    raise ValueError(
                        f"Config '{use_block}' is not an Oracle configuration"
                    )

                self.host = host or config_details["host"] or config("ORACLE_HOST")
                self.port = int(
                    port or config_details.get("port") or config("ORACLE_PORT", 1521)
                )
                self.service_name = (
                        service_name
                        or config_details.get("database")
                        or config("ORACLE_SERVICE_NAME")
                )
                self.username = (
                        username
                        or config_details["username"]
                        or config("ORACLE_USERNAME")
                )
                self.password = (
                        password or config_details["password"] or config("ORACLE_PWD")
                )
            except Exception as e:
                self.logger.error(f"Failed to load from block {use_block}: {e}")
                raise
        else:
            self.host = host
            self.port = port or 1521
            self.service_name = service_name
            self.username = username
            self.password = password

        self.connection = None

    @classmethod
    def from_block(cls, block_name, **kwargs):
        """Create an OracleClient from a Prefect block configuration."""
        return cls(use_block=block_name, **kwargs)

    def __enter__(self):
        """Support for context manager protocol."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection when exiting context."""
        self.close()

    def get_connection_string(self) -> str:
        """
        Generate the connection string for Oracle.

        Returns:
            Connection string for oracledb
        """
        return f"{self.username}/{self.password}@{self.host}:{self.port}/{self.service_name}"

    def connect(self) -> None:
        """
        Establish connection to the Oracle database.

        Raises:
            oracledb.Error: If connection fails
            ValueError: If required connection parameters are missing
        """
        if self.connection is not None:
            return

        # Validate connection parameters
        if not all([self.host, self.service_name, self.username, self.password]):
            missing = []
            if not self.host:
                missing.append("host")
            if not self.service_name:
                missing.append("service_name")
            if not self.username:
                missing.append("username")
            if not self.password:
                missing.append("password")
            raise ValueError(
                f"Missing required connection parameters: {', '.join(missing)}"
            )

        try:
            # Create connection string
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(SERVER=DEDICATED)(SID={self.service_name})))"

            self.connection = oracledb.connect(
                user=self.username,
                password=self.password,
                dsn=dsn
            )

            self.logger.info(f"Connected to Oracle: {self.host}:{self.port}/{self.service_name}")
        except oracledb.Error as e:
            self.logger.error(f"Failed to connect to Oracle: {e}")
            raise

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.debug("Oracle connection closed")

    def execute_query(
            self, query: str, params: Optional[Union[Dict[str, Any], Tuple, List]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries.

        Args:
            query: SQL query to execute
            params: Query parameters (optional) as dict, tuple or list

        Returns:
            List of dictionaries with query results

        Raises:
            oracledb.Error: If query execution fails
        """
        if not self.connection:
            self.connect()

        try:
            # Log query (excluding sensitive parameters)
            truncated_query = query[:1000] + "..." if len(query) > 1000 else query
            self.logger.debug(f"Executing Oracle query: {truncated_query}")

            cursor = self.connection.cursor()

            try:
                # Execute query with parameters if provided
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                if cursor.description:
                    # Get column names
                    column_names = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    results = []
                    for row in rows:
                        result = dict(zip(column_names, row))
                        results.append(result)

                    self.logger.info(f"Query returned {len(results)} rows")
                    return results
                else:
                    # For non-SELECT queries
                    self.connection.commit()
                    row_count = cursor.rowcount
                    self.logger.info(f"Query affected {row_count} rows")
                    return [{"rowcount": row_count}]

            finally:
                cursor.close()

        except oracledb.Error as e:
            self.logger.error(f"Error executing Oracle query: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    def execute_templated_query(
            self, template: str, params: Dict[str, Any], model_class=None
    ) -> List[Dict[str, Any]]:
        """
        Execute a query using string templating with the provided parameters.

        Args:
            template: SQL query template with placeholders
            params: Parameters to format into the template
            model_class: Optional class to convert results to (must have from_dict method)

        Returns:
            List of dictionaries (or model instances if model_class provided)
        """
        try:
            # Format the query with provided parameters
            query = template.format(**params)

            # Execute the query
            results = self.execute_query(query)

            # Convert to model instances if requested
            if model_class and hasattr(model_class, "from_dict"):
                return [model_class.from_dict(result) for result in results]

            return results

        except Exception as e:
            self.logger.error(f"Error executing templated query: {e}")
            raise