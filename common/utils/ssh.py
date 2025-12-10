import socket
import threading
import time
import queue
from typing import Tuple

import paramiko

from common.logging import get_logger


class SSHPool:
    """
    SSH connection pool to manage and reuse connections.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(SSHPool, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        self.logger = get_logger()
        self.connections = {}  # key: (host, username), value: list of available SSH instances
        self.active = {}       # key: SSH instance, value: last access time
        self.pool_lock = threading.Lock()
        self.max_idle_time = 60  # seconds
        
        # Start cleanup thread
        self.shutdown = False
        self.cleanup_thread = threading.Thread(target=self._cleanup_idle, daemon=True)
        self.cleanup_thread.start()
        
        self._initialized = True
        
    def get_connection(self, address: str, username: str, password: str) -> 'SSH':
        """Get an existing connection or create a new one."""
        key = (address, username)
        
        with self.pool_lock:
            # Check for available connection
            if key in self.connections and self.connections[key]:
                ssh = self.connections[key].pop()
                if ssh.is_connected():
                    self.active[ssh] = time.time()
                    return ssh
                else:
                    # Connection is dead, clean it up
                    ssh._cleanup_resources()
        
        # Create new connection
        ssh = SSH(address, username, password)
        if ssh.is_connected():
            with self.pool_lock:
                self.active[ssh] = time.time()
        return ssh
    
    def release_connection(self, ssh: 'SSH'):
        """Return a connection to the pool."""
        if not ssh.is_connected():
            return
            
        key = (ssh.address, ssh.username)
        
        with self.pool_lock:
            # Remove from active
            if ssh in self.active:
                del self.active[ssh]
            
            # Add to available connections
            if key not in self.connections:
                self.connections[key] = []
            self.connections[key].append(ssh)
    
    def _cleanup_idle(self):
        """Periodically clean up idle connections."""
        while not self.shutdown:
            time.sleep(10)  # Check every 10 seconds
            
            current_time = time.time()
            to_close = []
            
            with self.pool_lock:
                # Find idle connections in the pool
                for key, conns in list(self.connections.items()):
                    for i, ssh in enumerate(conns):
                        if not ssh.is_connected() or current_time - ssh.last_used > self.max_idle_time:
                            to_close.append(ssh)
                            conns.pop(i)
                    
                    # Clean up empty lists
                    if not conns:
                        del self.connections[key]
            
            # Close connections outside of lock
            for ssh in to_close:
                ssh.closeConnection(return_to_pool=False)
    
    def shutdown_pool(self):
        """Shutdown the connection pool."""
        self.shutdown = True
        
        # Close all connections
        with self.pool_lock:
            # Close active connections
            for ssh in list(self.active.keys()):
                ssh.closeConnection(return_to_pool=False)
            self.active.clear()
            
            # Close pooled connections
            for key in list(self.connections.keys()):
                for ssh in self.connections[key]:
                    ssh.closeConnection(return_to_pool=False)
                self.connections[key].clear()
            self.connections.clear()


class SSH:
    """
    Improved SSH client with connection pooling, buffer management,
    and better thread and resource handling.
    """
    # Maximum buffer size (bytes)
    MAX_BUFFER_SIZE = 1024 * 1024  # 1MB
    
    def __init__(self, address: str, username: str, password: str):
        self.logger = get_logger()
        self.address = address
        self.username = username
        self.password = password
        self.logger.info(f"Connecting to server on IP {address}.")
        
        # Initialize variables
        self.client = paramiko.client.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        self.transport = None
        self.shell = None
        self.strdata = ""
        self.fulldata = ""
        self._output_queue = queue.Queue()
        self._thread = None
        self._connected = False
        self.last_used = time.time()

        try:
            # Disable ssh-agent explicitly
            self.client.connect(
                address, 
                username=username, 
                password=password, 
                look_for_keys=False,
                allow_agent=False,  # Explicitly disable ssh-agent
                timeout=10
            )
            
            self.transport = self.client.get_transport()
            self.shell = self.transport.open_session()
            self.shell.get_pty()
            self.shell.settimeout(5)
            self.shell.invoke_shell()
            
            # Start processing thread
            self._thread_stop = threading.Event()
            self._thread = threading.Thread(target=self._process)
            self._thread.daemon = True
            self._thread.start()
            
            self._connected = True
            
        except paramiko.AuthenticationException:
            self.logger.error("Authentication failed. Please check your credentials.")
        except paramiko.SSHException as e:
            self.logger.error(f"SSH connection error: {str(e)}")
        except socket.error as e:
            self.logger.error(f"Socket error: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error during SSH connection: {str(e)}")
            
        # If connection failed, clean up resources
        if not self._connected:
            self._cleanup_resources()

    def is_connected(self) -> bool:
        """Return True if the SSH connection is established and active."""
        return (self._connected and 
                self.transport is not None and 
                self.transport.is_active() and
                self.shell is not None)

    def closeConnection(self, return_to_pool: bool = True):
        """
        Close the SSH connection and clean up resources.
        If return_to_pool is True, the connection will be returned to the pool.
        """
        self._connected = False
        
        # Signal thread to stop
        if self._thread is not None and self._thread.is_alive():
            self._thread_stop.set()
            self._thread.join(timeout=1)  # Wait up to 1 second for thread to finish
        
        self._cleanup_resources()
        
        # Return to pool is handled by the caller

    def _cleanup_resources(self):
        """Clean up SSH resources."""
        if self.shell is not None:
            try:
                self.shell.close()
            except Exception as e:
                self.logger.error(f"Error closing shell: {str(e)}")
            self.shell = None
            
        if self.transport is not None:
            try:
                self.transport.close()
            except Exception as e:
                self.logger.error(f"Error closing transport: {str(e)}")
            self.transport = None
            
        if self.client is not None:
            try:
                self.client.close()
            except Exception as e:
                self.logger.error(f"Error closing SSH client: {str(e)}")
            
        # Clear data buffers
        self.strdata = ""
        self.fulldata = ""
        
        # Clear queue
        while not self._output_queue.empty():
            try:
                self._output_queue.get_nowait()
            except queue.Empty:
                break

    def sendShell(self, command: str) -> bool:
        """
        Send a command to the shell.
        Returns True if command was sent successfully, False otherwise.
        """
        self.last_used = time.time()
        
        if not self.is_connected():
            self.logger.error("Cannot send command: SSH connection is not active.")
            return False
            
        try:
            # Reset buffers before sending new command
            self.strdata = ""
            
            # Limit fulldata size by truncating if needed
            if len(self.fulldata) > self.MAX_BUFFER_SIZE:
                self.fulldata = self.fulldata[-self.MAX_BUFFER_SIZE//2:]
                
            self.shell.send(command + "\n")
            return True
        except Exception as e:
            self.logger.error(f"Error sending command: {str(e)}")
            self._connected = False
            return False

    def _process(self):
        """Process incoming data from the SSH shell."""
        while not self._thread_stop.is_set() and self._connected:
            try:
                if self.shell and not self._thread_stop.is_set():
                    if self.shell.recv_ready():
                        alldata = self.shell.recv(1024)
                        if not alldata:
                            self._connected = False
                            break
                            
                        decoded_data = alldata.decode("utf-8", errors="replace")
                        self.strdata = self.strdata + decoded_data
                        
                        # Manage buffer size
                        if len(self.fulldata) > self.MAX_BUFFER_SIZE:
                            # Keep only the last half of the buffer
                            self.fulldata = self.fulldata[-self.MAX_BUFFER_SIZE//2:]
                        
                        self.fulldata = self.fulldata + decoded_data
                        
                        # Process lines for logging
                        self.strdata = self._log_lines(self.strdata)
                    else:
                        # Small sleep to prevent CPU spinning
                        self._thread_stop.wait(0.1)
                else:
                    # Shell not ready, small wait
                    self._thread_stop.wait(0.1)
            except socket.timeout:
                # This is normal, just continue
                pass
            except EOFError:
                self.logger.warning("SSH connection closed by server.")
                self._connected = False
                break
            except Exception as e:
                self.logger.error(f"Error processing SSH data: {str(e)}")
                self._connected = False
                break

    def _log_lines(self, data: str) -> str:
        """Process and log lines from the received data."""
        last_line = data
        if "\n" in data:
            lines = data.splitlines()
            for line in lines[:-1]:
                self.logger.debug(f"SSH output: {line}")
            last_line = lines[-1]
            if data.endswith("\n"):
                self.logger.debug(f"SSH output: {last_line}")
                last_line = ""
        return last_line
    
    # Add this new method to the SSH class
    def execute_command(self, command: str, timeout: int = 30) -> Tuple[bool, str]:
        """Execute a command and return its output without using background thread"""
        self.last_used = time.time()
        
        if not self.is_connected():
            return False, "Not connected"
        
        try:
            # Create a new channel for this specific command
            channel = self.transport.open_session()
            channel.settimeout(timeout)
            channel.exec_command(command)
            
            # Read output directly from channel
            output = b''
            while not channel.exit_status_ready():
                if channel.recv_ready():
                    output += channel.recv(4096)
                time.sleep(0.1)
            
            # Get any remaining data after command completes
            while channel.recv_ready():
                output += channel.recv(4096)
            
            # Get exit status
            exit_status = channel.recv_exit_status()
            channel.close()
            
            return exit_status == 0, output.decode('utf-8', errors='replace')
        
        except Exception as e:
            self.logger.error(f"Command execution failed: {str(e)}")
            return False, str(e)
    
    # def execute_command(self, command: str, timeout: int = 30) -> Tuple[bool, str]:
    #     """
    #     Execute a command and wait for completion.
    #     Returns (success, output)
    #     """
    #     self.last_used = time.time()
        
    #     if not self.is_connected():
    #         return False, "Not connected"
            
    #     # Reset buffer before command
    #     self.fulldata = ""
        
    #     if not self.sendShell(command):
    #         return False, "Failed to send command"
            
    #     # Wait for command output
    #     start_time = time.time()
    #     last_size = 0
    #     stable_count = 0
        
    #     while time.time() - start_time < timeout:
    #         time.sleep(0.1)
            
    #         # If output size hasn't changed for a while, assume command completed
    #         if len(self.fulldata) == last_size:
    #             stable_count += 1
    #             if stable_count >= 20:  # ~2 seconds of stability
    #                 break
    #         else:
    #             stable_count = 0
    #             last_size = len(self.fulldata)
                
    #     return True, self.fulldata


def get_ssh_connection(address: str, username: str, password: str) -> SSH:
    """
    Get an SSH connection from the pool or create a new one.
    """
    pool = SSHPool()
    return pool.get_connection(address, username, password)


def release_ssh_connection(ssh: SSH):
    """
    Release an SSH connection back to the pool.
    """
    if ssh:
        pool = SSHPool()
        pool.release_connection(ssh)


def shutdown_ssh_pool():
    """
    Shutdown the SSH connection pool.
    """
    pool = SSHPool()
    pool.shutdown_pool()