from prefect.blocks.core import Block
from pydantic import SecretStr, Field
from typing import Optional, Dict, Literal

class InfrastructureConfig(Block):
    """Block to store infrastructure configuration for various server types."""

    _block_type_name = "Infrastructure Configuration"
    _block_type_slug = "infrastructure-config"
    _description = "Configuration for connecting to various infrastructure services"

    # Indicates the type of service this configuration is for
    type: Literal["sql_server", "mongodb", "postgresql", "ssh", "api", "oracle", "ftp"] = Field(
        ..., description="Type of infrastructure service"
    )

    # Basic connection details - mark as secret based on your security needs
    host: str = Field(..., description="Hostname or IP address")
    port: Optional[int] = Field(None, description="Port number")
    username: str = Field(..., description="Username for authentication")
    password: SecretStr = Field(..., description="Password for authentication")

    # Database-specific fields
    database: Optional[str] = Field(
        None, description="Database name (for database servers)"
    )

    # Additional configuration parameters specific to each server type
    extra_params: Dict = Field(
        default_factory=dict,
        description="Additional configuration parameters specific to the server type",
    )

    # Secret additional parameters that should be obfuscated
    secret_params: Dict = Field(
        default_factory=dict,
        description="Secret configuration parameters that should not be logged",
    )

    def get_connection_details(self):
        """Return connection details as a dictionary with secrets resolved."""
        details = {
            "type": self.type,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password.get_secret_value(),
        }

        if self.database:
            details["database"] = self.database

        # Include any extra parameters
        details.update(self.extra_params)

        # Include any secret parameters (revealing their values)
        for key, value in self.secret_params.items():
            if isinstance(value, SecretStr):
                details[key] = value.get_secret_value()
            else:
                details[key] = value

        return details

    def get_ftp_config(self):
        """Get FTP-specific configuration with defaults."""
        if self.type != "ftp":
            raise ValueError("This block is not configured for FTP connections")
        
        base_config = self.get_connection_details()
        
        # FTP-specific defaults
        ftp_config = {
            "host": base_config["host"],
            "port": base_config["port"] or 21,
            "username": base_config["username"],
            "password": base_config["password"],
            "passive": self.extra_params.get("passive", True),
            "timeout": self.extra_params.get("timeout", 30),
            "encoding": self.extra_params.get("encoding", "utf-8"),
            "secure": self.extra_params.get("secure", False),  # FTPS
            "base_path": self.extra_params.get("base_path", "/"),
        }
        
        return ftp_config

    def get_ssh_config(self):
        """Get SSH-specific configuration with defaults."""
        if self.type != "ssh":
            raise ValueError("This block is not configured for SSH connections")
        
        base_config = self.get_connection_details()
        
        # SSH-specific defaults
        ssh_config = {
            "host": base_config["host"],
            "port": base_config["port"] or 22,
            "username": base_config["username"],
            "password": base_config["password"],
            "timeout": self.extra_params.get("timeout", 30),
            "key_filename": self.extra_params.get("key_filename"),
            "key_password": self.secret_params.get("key_password"),
            "look_for_keys": self.extra_params.get("look_for_keys", True),
            "allow_agent": self.extra_params.get("allow_agent", True),
            "compress": self.extra_params.get("compress", False),
        }
        
        return ssh_config