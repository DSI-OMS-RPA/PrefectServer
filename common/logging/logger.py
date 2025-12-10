from datetime import datetime
from functools import lru_cache
from pathlib import Path
import os

from loguru import logger


class LogManager:
    """Centralized logging management for the application."""

    def __init__(self, base_log_dir: str = "logs", use_cwd: bool = True):
        """
        Initialize the log manager.

        Args:
            base_log_dir: Base directory name for logs
            use_cwd: If True, logs will be created relative to the current working directory
                     If False, logs will be created relative to the SharedUtils package
        """
        if use_cwd:
            # Use the current working directory (ETL project directory)
            self.root_dir = Path(os.getcwd())
        else:
            # Use the SharedUtils directory (previous behavior)
            self.root_dir = Path(__file__).parent.parent.parent

        self.log_folder = self.root_dir / base_log_dir

        # Create log directories
        self.log_folder.mkdir(exist_ok=True)

        # Configure main logger
        self._configure_logger()

    def _configure_logger(self) -> None:
        """Configure the main application logger."""
        logger.remove()  # Remove any existing handlers

        # Add daily rotating file handler
        log_file = self.log_folder / f"log_{datetime.now().strftime('%Y%m%d')}.log"
        logger.add(
            log_file,
            rotation="00:00",
            retention="30 days",
            level="INFO",
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {file}:{function}:{line} | {message}",
            backtrace=True,
            diagnose=True,
            enqueue=True,  # Thread-safe logging
        )

        # Add console output
        logger.add(
            sink=lambda msg: print(msg),
            level="INFO",
            format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <cyan>{message}</cyan>",
            colorize=True,
        )

    def info(self, message: str) -> None:
        logger.info(message)

    def error(self, message: str) -> None:
        logger.error(message)

    def warning(self, message: str) -> None:
        logger.warning(message)

    def success(self, message: str) -> None:
        logger.success(message)

    def debug(self, message: str) -> None:
        logger.debug(message)


@lru_cache()
def get_logger(base_log_dir: str = "logs", use_cwd: bool = True) -> LogManager:
    """
    Get or create a LogManager instance (cached).

    Args:
        base_log_dir: Base directory for logs
        use_cwd: If True, logs will be in the current working directory
                 If False, logs will be in the SharedUtils directory

    Returns:
        LogManager instance
    """
    return LogManager(base_log_dir=base_log_dir, use_cwd=use_cwd)
