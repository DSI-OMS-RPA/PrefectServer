from blocks.infrastructure import InfrastructureConfig
from common.config.config import config
from pydantic import SecretStr

# sql_server_dmk = InfrastructureConfig(
#     type="sql_server",
#     host=config("SQLSERVER_HOST"),
#     port=1433,
#     username=config("SQLSERVER_USERNAME"),
#     password=config("SQLSERVER_PWD"),
#     database=config("SQLSERVER_DATABASE"),
# )
# sql_server_dmk.save("sql-server-dmk")
#
# mongodb_imei = InfrastructureConfig(
#     type="mongodb",
#     host=config("MONGODB_HOST"),
#     port=27017,
#     username=config("MONGODB_USERNAME"),
#     password=config("MONGODB_PWD"),
#     database=config("MONGODB_DBNAME"),
# )
# mongodb_imei.save("mongodb-imei")
#
# ssh_mongodb = InfrastructureConfig(
#     type="ssh",
#     host=config("SSH_HOST"),
#     port=22,
#     username=config("SSH_USERNAME"),
#     password=config("SSH_PWD"),
# )
#
# ssh_mongodb.save("ssh-mongodb")
#
# sql_server_mediation = InfrastructureConfig(
#     type="sql_server",
#     host=config("SQLSERVER_MEDIATION_HOST"),
#     port=1433,
#     username=config("SQLSERVER_MEDIATION_USERNAME"),
#     password=config("SQLSERVER_MEDIATION_PWD"),
#     database=config("SQLSERVER_MEDIATION_DBNAME"),
# )
# sql_server_mediation.save("sql-server-mediation")

# brm_oracle = InfrastructureConfig(
#     type="oracle",
#     host="10.16.10.103",
#     port=1536,
#     username=config("BRM_DB_USERNAME"),
#     password=SecretStr(config("BRM_DB_PWD")),
#     database="CVTBRPRD3",
# )
# brm_oracle.save("brm-oracle")


# FTP block
# ftp_block = InfrastructureConfig(
#     type="ftp",
#     host="10.16.29.26",
#     port=22,
#     username="reseller1",
#     password=SecretStr("$4khurasQ"),
#     extra_params={
#         "passive": True,
#         "timeout": 30,
#         "base_path": "/var/opt/ptin/oitf/events/out"
#     }
# )
# ftp_block.save("ftp-reseller-prd")

# PostgreSQL block  
# pg_block = InfrastructureConfig(
#     type="postgresql",
#     host="192.168.87.59", 
#     port=5432,
#     username="postgres",
#     password=SecretStr("P$23fg#98"),
#     database="reseller_sap"
# )
# pg_block.save("postgresql-reseller-sap")


sftp_reseller_prd_password = InfrastructureConfig(
    type="ssh",  # Use existing SSH type for SFTP
    host=config("SFTP_RESELLER_HOST"),
    port=int(config("SFTP_RESELLER_PORT", "22")),
    username=config("SFTP_RESELLER_USERNAME"),
    password=SecretStr(config("SFTP_RESELLER_PASSWORD")),
    extra_params={
        # SSH/SFTP specific settings
        "look_for_keys": False,  # Don't search for SSH keys when using password
        "allow_agent": False,    # Don't use SSH agent for password auth
        "compress": True,        # Enable compression
        "timeout": 30,           # Connection timeout
        
        # SFTP specific settings
        "base_path": config("SFTP_RESELLER_BASE_PATH", "/var/opt/ptin/oitf/events/out"),
        "host_key_verification": "auto",  # More lenient for password auth
        "max_retries": 3,        # Connection retry attempts
        "retry_delay": 5,        # Delay between retries
        "encoding": "utf-8"      # Default file encoding
    }
)
sftp_reseller_prd_password.save("ftp-reseller-prd")