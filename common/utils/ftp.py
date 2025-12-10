# common/utils/ftp.py
from datetime import datetime
import re
import stat
from typing import List, Dict, Any, Optional
import time
import threading

import paramiko
from prefect import get_run_logger
from pydantic import BaseModel, field_validator
from decimal import Decimal


# Data Models
class VendasRecord(BaseModel):
    """Model for rm_vendas*.rmven files (13 columns)"""
    data: datetime
    operacao: Optional[str] = None
    id_u_transacao: Optional[str] = None
    id_p_transacao: Optional[str] = None
    canal: Optional[str] = None
    id_dealer: Optional[str] = None
    sap_user: Optional[str] = None
    mssisdn_dl: Optional[str] = None
    mssisdn_dt: Optional[str] = None
    ponto_venda: Optional[str] = None
    mod_pag: Optional[str] = None
    valor: Optional[Decimal] = None
    comissao: Optional[Decimal] = None
    source_file: str

    @field_validator('data', mode='before')
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, "%d/%m/%Y %H:%M:%S")
        return v

    @field_validator('valor', 'comissao', mode='before')
    @classmethod
    def parse_decimal(cls, v):
        if v is None or v == '':
            return None
        return Decimal(str(v))


class CarregamentosRecord(BaseModel):
    """Model for rm_carregamentos*.rmcarr files (11 columns)"""
    data: datetime
    id_dealer: Optional[str] = None
    mssisdn: Optional[str] = None
    id_transacao: Optional[str] = None
    id_correlacao: Optional[str] = None
    canal: Optional[str] = None
    operacao: Optional[str] = None
    valor: Optional[Decimal] = None
    comissao: Optional[Decimal] = None
    tp_ordem: Optional[str] = None
    mdl_pag: Optional[str] = None
    source_file: str

    @field_validator('data', mode='before')
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, "%d/%m/%Y %H:%M:%S")
        return v

    @field_validator('valor', 'comissao', mode='before')
    @classmethod
    def parse_decimal(cls, v):
        if v is None or v == '':
            return None
        return Decimal(str(v))


class FileMetadata(BaseModel):
    """Metadata for SFTP files"""
    filename: str
    size: int
    modified_time: datetime
    file_type: str  # 'vendas' or 'carregamentos'
    permissions: Optional[str] = None
    owner: Optional[str] = None
    group: Optional[str] = None


class ProcessingResult(BaseModel):
    """Result of processing a single file"""
    filename: str
    file_type: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    inserted_rows: int
    processing_duration: float
    error_messages: List[str]
    warnings: List[str]


class SFTPConnectionError(Exception):
    """Custom exception for SFTP connection errors"""
    pass


class SFTPAuthenticationError(Exception):
    """Custom exception for SFTP authentication errors"""
    pass


class SFTPClient:
    """SFTP client with multiple authentication methods and robust error handling"""

    # Class-level connection throttling
    _last_connection_time = 0
    _connection_lock = None
    _min_connection_interval = 0.5  # Minimum 500ms between connections

    def __init__(self, config: Dict[str, Any]):
        # Initialize lock on first instance
        if SFTPClient._connection_lock is None:
            SFTPClient._connection_lock = threading.Lock()

        self.config = config
        self.ssh_client = None
        self.sftp_client = None
        self.logger = get_run_logger()

        # Set default values
        self.host = config['host']
        self.port = config.get('port', 22)
        self.username = config['username']
        self.timeout = config.get('timeout', 30)
        self.base_path = config.get('base_path', '/')
        self.compression = config.get('compression', True)
        self.host_key_verification = config.get('host_key_verification', 'auto')

        # Authentication configuration
        self.password = config.get('password')
        self.private_key_path = config.get('private_key_path')
        self.private_key_passphrase = config.get('private_key_passphrase')
        self.use_ssh_agent = config.get('use_ssh_agent', False)  # Changed default to False

        # Connection retry configuration
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 5)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        time.sleep(0.3)  # Allow server to clean up

    def connect(self):
        """Establish SFTP connection with retry logic and rate limiting"""
        # Throttle connections globally
        with SFTPClient._connection_lock:
            elapsed = time.time() - SFTPClient._last_connection_time
            if elapsed < self._min_connection_interval:
                sleep_time = self._min_connection_interval - elapsed
                self.logger.debug(f"Throttling connection, waiting {sleep_time:.2f}s")
                time.sleep(sleep_time)

            SFTPClient._last_connection_time = time.time()

        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                self._establish_connection()
                self.logger.info(f"Connected to SFTP server: {self.host}:{self.port}")
                return

            except Exception as e:
                last_exception = e
                if attempt < self.max_retries:
                    self.logger.warning(
                        f"Connection attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {self.retry_delay} seconds..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"All connection attempts failed: {e}")

        raise SFTPConnectionError(f"Failed to connect after {self.max_retries + 1} attempts: {last_exception}")

    def _establish_connection(self):
        """Establish the actual SSH/SFTP connection"""
        # Create SSH client
        self.ssh_client = paramiko.SSHClient()

        # Configure host key verification
        if self.host_key_verification == 'disabled':
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        elif self.host_key_verification == 'auto':
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        elif self.host_key_verification == 'strict':
            self.ssh_client.load_system_host_keys()

        # Prepare connection parameters
        connect_kwargs = {
            'hostname': self.host,
            'port': self.port,
            'username': self.username,
            'timeout': self.timeout,
            'compress': self.compression,
            'look_for_keys': False,  # Disable by default
            'allow_agent': False  # Disable by default
        }

        # Try different authentication methods
        auth_success = False
        auth_errors = []

        # Method 1: SSH Agent authentication (only if explicitly enabled)
        if self.use_ssh_agent and not auth_success:
            try:
                connect_kwargs['allow_agent'] = True
                self.ssh_client.connect(**connect_kwargs)
                auth_success = True
                self.logger.info("Authenticated using SSH agent")
            except Exception as e:
                auth_errors.append(f"SSH agent auth failed: {e}")
                connect_kwargs['allow_agent'] = False

        # Method 2: Private key authentication
        if self.private_key_path and not auth_success:
            try:
                private_key = self._load_private_key(self.private_key_path, self.private_key_passphrase)
                connect_kwargs['pkey'] = private_key
                self.ssh_client.connect(**connect_kwargs)
                auth_success = True
                self.logger.info("Authenticated using private key")
            except Exception as e:
                auth_errors.append(f"Private key auth failed: {e}")
                connect_kwargs.pop('pkey', None)

        # Method 3: Password authentication
        if self.password and not auth_success:
            try:
                connect_kwargs['password'] = self.password
                self.ssh_client.connect(**connect_kwargs)
                auth_success = True
                self.logger.info("Authenticated using password")
            except Exception as e:
                auth_errors.append(f"Password auth failed: {e}")
                raise SFTPAuthenticationError(f"All authentication methods failed: {'; '.join(auth_errors)}")

        if not auth_success:
            raise SFTPAuthenticationError(f"All authentication methods failed: {'; '.join(auth_errors)}")

        # Open SFTP session
        self.sftp_client = self.ssh_client.open_sftp()

        # Change to base path if specified
        if self.base_path and self.base_path != '/':
            try:
                self.sftp_client.chdir(self.base_path)
                self.logger.info(f"Changed to directory: {self.base_path}")
            except Exception as e:
                self.logger.warning(f"Could not change to base path {self.base_path}: {e}")

    def _load_private_key(self, key_path: str, passphrase: Optional[str] = None):
        """Load private key from file"""
        try:
            # Try different key types
            for key_class in [paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key]:
                try:
                    return key_class.from_private_key_file(key_path, password=passphrase)
                except paramiko.SSHException:
                    continue
            raise ValueError("Could not load private key - unknown key type")
        except Exception as e:
            raise SFTPAuthenticationError(f"Failed to load private key from {key_path}: {e}")

    def disconnect(self):
        """Close SFTP and SSH connections"""
        if self.sftp_client:
            try:
                self.sftp_client.close()
                self.logger.debug("SFTP client closed")
            except Exception as e:
                self.logger.warning(f"Error closing SFTP client: {e}")
            finally:
                self.sftp_client = None

        if self.ssh_client:
            try:
                self.ssh_client.close()
                self.logger.debug("SSH client closed")
            except Exception as e:
                self.logger.warning(f"Error closing SSH client: {e}")
            finally:
                self.ssh_client = None

        self.logger.info("Disconnected from SFTP server")

    def list_files(self, pattern: Optional[str] = None) -> List[FileMetadata]:
        """List files matching pattern with detailed metadata"""
        if not self.sftp_client:
            raise SFTPConnectionError("Not connected to SFTP server")

        try:
            files = []

            # Get file list with attributes
            file_attrs = self.sftp_client.listdir_attr('.')

            for file_attr in file_attrs:
                filename = file_attr.filename

                # Skip directories
                if stat.S_ISDIR(file_attr.st_mode):
                    continue

                # Apply pattern filter
                if pattern and not re.match(pattern, filename):
                    continue

                # Determine file type
                file_type = None
                if filename.startswith('rm_vendas') and filename.endswith('.rmven'):
                    file_type = 'vendas'
                elif filename.startswith('rm_carregamentos') and filename.endswith('.rmcarr'):
                    file_type = 'carregamentos'

                if file_type:
                    # Convert modification time
                    modified_time = datetime.fromtimestamp(file_attr.st_mtime)

                    # Convert permissions
                    permissions = stat.filemode(file_attr.st_mode)

                    files.append(FileMetadata(
                        filename=filename,
                        size=file_attr.st_size or 0,
                        modified_time=modified_time,
                        file_type=file_type,
                        permissions=permissions
                    ))

            self.logger.info(f"Found {len(files)} files matching pattern")
            return files

        except Exception as e:
            self.logger.error(f"Failed to list files: {e}")
            raise

    def download_file_content(self, filename: str, encoding: str = 'utf-8') -> str:
        """Download file content as string with proper encoding"""
        if not self.sftp_client:
            raise SFTPConnectionError("Not connected to SFTP server")

        try:
            self.logger.debug(f"Downloading file: {filename}")

            # Download file to memory
            with self.sftp_client.open(filename, 'r') as remote_file:
                content = remote_file.read()

            # Handle encoding
            if isinstance(content, bytes):
                content = content.decode(encoding)

            self.logger.debug(f"Downloaded {len(content)} characters from {filename}")
            return content

        except Exception as e:
            self.logger.error(f"Failed to download file {filename}: {e}")
            raise

    def download_file_binary(self, filename: str) -> bytes:
        """Download file content as binary data"""
        if not self.sftp_client:
            raise SFTPConnectionError("Not connected to SFTP server")

        try:
            self.logger.debug(f"Downloading binary file: {filename}")

            with self.sftp_client.open(filename, 'rb') as remote_file:
                content = remote_file.read()

            self.logger.debug(f"Downloaded {len(content)} bytes from {filename}")
            return content

        except Exception as e:
            self.logger.error(f"Failed to download binary file {filename}: {e}")
            raise

    def file_exists(self, filename: str) -> bool:
        """Check if file exists on remote server"""
        if not self.sftp_client:
            raise SFTPConnectionError("Not connected to SFTP server")

        try:
            self.sftp_client.stat(filename)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            self.logger.error(f"Error checking if file exists {filename}: {e}")
            raise

    def get_file_info(self, filename: str) -> Optional[FileMetadata]:
        """Get detailed information about a specific file"""
        if not self.sftp_client:
            raise SFTPConnectionError("Not connected to SFTP server")

        try:
            file_attr = self.sftp_client.stat(filename)

            # Determine file type
            file_type = None
            if filename.startswith('rm_vendas') and filename.endswith('.rmven'):
                file_type = 'vendas'
            elif filename.startswith('rm_carregamentos') and filename.endswith('.rmcarr'):
                file_type = 'carregamentos'

            if file_type:
                modified_time = datetime.fromtimestamp(file_attr.st_mtime)
                permissions = stat.filemode(file_attr.st_mode)

                return FileMetadata(
                    filename=filename,
                    size=file_attr.st_size or 0,
                    modified_time=modified_time,
                    file_type=file_type,
                    permissions=permissions
                )

            return None

        except Exception as e:
            self.logger.error(f"Failed to get file info for {filename}: {e}")
            raise

    def test_connection(self) -> Dict[str, Any]:
        """Test SFTP connection and return connection details"""
        try:
            if not self.sftp_client:
                self.connect()

            # Test basic operations
            current_dir = self.sftp_client.getcwd() or '/'
            files_count = len(self.sftp_client.listdir('.'))

            # Get server info
            transport = self.ssh_client.get_transport()
            server_version = transport.remote_version if transport else "Unknown"

            return {
                "status": "success",
                "connected": True,
                "server_version": server_version,
                "current_directory": current_dir,
                "files_in_directory": files_count,
                "compression": transport.use_compression if transport else False,
                "authentication_method": "SSH"
            }

        except Exception as e:
            return {
                "status": "failed",
                "connected": False,
                "error": str(e)
            }


# Configuration helper
def create_sftp_config(
        host: str,
        username: str,
        port: int = 22,
        password: Optional[str] = None,
        private_key_path: Optional[str] = None,
        private_key_passphrase: Optional[str] = None,
        base_path: str = "/",
        timeout: int = 30,
        compression: bool = True,
        host_key_verification: str = "auto",
        use_ssh_agent: bool = False,
        max_retries: int = 3,
        retry_delay: int = 5
) -> Dict[str, Any]:
    """Helper function to create SFTP configuration dictionary"""

    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "private_key_path": private_key_path,
        "private_key_passphrase": private_key_passphrase,
        "base_path": base_path,
        "timeout": timeout,
        "compression": compression,
        "host_key_verification": host_key_verification,
        "use_ssh_agent": use_ssh_agent,
        "max_retries": max_retries,
        "retry_delay": retry_delay
    }


# Backward compatibility alias
FTPClient = SFTPClient