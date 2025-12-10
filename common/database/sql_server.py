import os
import platform
from typing import List, Dict, Any, Optional, Union, Tuple
from common.logging import get_logger
from common.config import config
from blocks.infrastructure import InfrastructureConfig
import pyodbc


class SQLServerClient:
    """SQL Server database client for ETL operations."""

    def __init__(
        self,
        host: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: Optional[str] = None,
        use_config: bool = False,
        use_block: str = None,
    ):
        """
        Initialize SQL Server client.

        Args:
            host: SQL Server hostname
            database: Database name
            username: SQL Server username
            password: SQL Server password
            driver: Optional ODBC driver name
            use_config: Whether to try loading values from config if params are None
            use_block: Whether to use Prefect block to retrieve secret informations
        """
        self.logger = get_logger()

        if use_config:
            self.host = host or config("SQLSERVER_HOST")
            self.database = database or config("SQLSERVER_DATABASE")
            self.username = username or config("SQLSERVER_USERNAME")
            self.password = password or config("SQLSERVER_PWD")

            default_driver = self._get_default_driver()
            self.driver = driver or config("SQLSERVER_DRIVER") or default_driver
        elif use_block:
            try:
                config_block = InfrastructureConfig.load(use_block)
                config_details = config_block.get_connection_details()

                if config_details["type"] != "sql_server":
                    raise ValueError(
                        f"Config '{use_block}' is not a SQL Server configuration"
                    )

                self.host = host or config_details["host"] or config("SQLSERVER_HOST")
                self.username = (
                    username
                    or config_details["username"]
                    or config("SQLSERVER_USERNAME")
                )
                self.password = (
                    password or config_details["password"] or config("SQLSERVER_PWD")
                )
                self.database = (
                    database
                    or config_details["database"]
                    or config("SQLSERVER_DATABASE")
                )
                self.driver = (
                    driver or config("SQLSERVER_DRIVER") or self._get_default_driver()
                )
                self.port = 1433 or config_details["port"]
            except Exception as e:
                self.logger.error(f"Failed to load from block {use_block}: {e}")
                raise
        else:
            self.host = host
            self.database = database
            self.username = username
            self.password = password
            self.driver = driver if driver else self._get_default_driver()

        self.connection = None
        self.cursor = None

    @classmethod
    def from_block(cls, block_name, **kwargs):
        """Create a SQLServerClient from a blocks configuration."""
        return cls(use_block=block_name, **kwargs)

    def _get_default_driver(self) -> str:
        """Select appropriate driver based on platform and distribution."""
        os_type = platform.system().lower()
        if os_type == "windows":
            return "ODBC Driver 17 for SQL Server"
        else:
            distro = self._get_linux_distro()
            if distro == "ubuntu":
                return "ODBC Driver 17 for SQL Server"
            elif distro == "centos":
                return "FreeTDS"
            else:
                return "FreeTDS"

    @staticmethod
    def _get_linux_distro() -> str:
        """Detect if system is running CentOS or Ubuntu."""
        if platform.system() != "Linux":
            return ""

        try:
            if os.path.exists("/etc/os-release"):
                with open("/etc/os-release", "r") as f:
                    content = f.read().lower()
                    if "centos" in content:
                        return "centos"
                    if "ubuntu" in content:
                        return "ubuntu"
        except (IOError, PermissionError):
            pass

        if os.path.exists("/etc/centos-release"):
            return "centos"

        try:
            if os.path.exists("/etc/lsb-release"):
                with open("/etc/lsb-release") as f:
                    if "Ubuntu" in f.read():
                        return "ubuntu"
        except (IOError, PermissionError):
            pass

        if hasattr(platform, "linux_distribution"):
            dist = platform.linux_distribution()[0].lower()
            if "centos" in dist:
                return "centos"
            if "ubuntu" in dist:
                return "ubuntu"

        return ""

    def __enter__(self):
        """Support for context manager protocol."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection when exiting context."""
        self.close()

    def get_connection_string(self) -> str:
        """
        Generate the connection string based on platform.

        Returns:
            Connection string for pyodbc
        """
        os_type = platform.system().lower()

        if os_type == "windows":
            return f"DRIVER={{{self.driver}}};SERVER={self.host};DATABASE={self.database};UID={self.username};PWD={self.password};TrustServerCertificate=yes;"
        else:  # Linux/Mac
            return f"DRIVER={{{self.driver}}};SERVER={self.host};PORT=1433;DATABASE={self.database};UID={self.username};PWD={self.password};TDS_Version=8.0;TrustServerCertificate=yes;"

    def connect(self) -> None:
        """
        Establish connection to the SQL Server database.

        Raises:
            pyodbc.Error: If connection fails
            ValueError: If required connection parameters are missing
        """
        if self.connection is not None:
            return

        # Validate connection parameters
        if not all([self.host, self.database, self.username, self.password]):
            missing = []
            if not self.host:
                missing.append("server")
            if not self.database:
                missing.append("database")
            if not self.username:
                missing.append("username")
            if not self.password:
                missing.append("password")
            raise ValueError(
                f"Missing required connection parameters: {', '.join(missing)}"
            )

        conn_str = self.get_connection_string()

        try:
            self.connection = pyodbc.connect(conn_str)
            self.cursor = self.connection.cursor()
            self.logger.info(f"Connected to SQL Server: {self.host}/{self.database}")
        except pyodbc.Error as e:
            self.logger.error(f"Failed to connect to SQL Server: {e}")
            raise

    def close(self) -> None:
        """Close the database connection and cursor."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
            self.logger.debug("Cursor closed")

        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.debug("Connection closed")

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
            pyodbc.Error: If query execution fails
        """
        if not self.connection:
            self.connect()

        try:
            # Log query (excluding sensitive parameters)
            truncated_query = query[:1000] + "..." if len(query) > 1000 else query
            self.logger.debug(f"Executing SQL query: {truncated_query}")

            # Execute query with parameters if provided
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            if self.cursor.description:
                column_names = [column[0] for column in self.cursor.description]
                rows = self.cursor.fetchall()

                results = []
                for row in rows:
                    result = dict(zip(column_names, row))
                    results.append(result)

                self.logger.info(f"Query returned {len(results)} rows")
                return results
            else:
                self.connection.commit()
                row_count = self.cursor.rowcount
                self.logger.info(f"Query affected {row_count} rows")
                return [{"rowcount": row_count}]

        except pyodbc.Error as e:
            self.logger.error(f"Error executing SQL query: {e}")
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
