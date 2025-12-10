"""Configuration utilities for ETL projects."""

import os
from configparser import ConfigParser, NoOptionError, NoSectionError
from pathlib import Path
from typing import Optional, Dict, Any

from dotenv import dotenv_values


def find_config_file(filename: str, max_levels: int = 4) -> Optional[Path]:
    """
    Search for a configuration file by looking up the directory tree.

    Args:
        filename: The name of the file to find (e.g., '.env', 'config.ini')
        max_levels: Maximum number of directory levels to search upwards

    Returns:
        Path to the found file or None if not found
    """
    current_dir = Path.cwd()

    # Try the current directory first
    if (current_dir / filename).exists():
        return current_dir / filename

    # Search up to max_levels directories up
    for _ in range(max_levels):
        current_dir = current_dir.parent
        if (current_dir / filename).exists():
            return current_dir / filename

    return None


class ConfigManager:
    """
    Manages configuration from multiple sources (env files, ini files, environment variables).

    This class implements a lazy-loading pattern to only read configuration files
    when they're first needed, and caches the results for subsequent calls.
    """

    def __init__(self):
        self._env_vars = None
        self._ini_config = None

    @property
    def env_vars(self) -> Dict[str, str]:
        """
        Load and cache environment variables from .env file.
        Falls back to OS environment variables if file not found.
        """
        if self._env_vars is None:
            env_file = find_config_file(".env")
            if env_file:
                self._env_vars = dotenv_values(env_file)
            else:
                # Fallback to OS environment variables
                self._env_vars = dict(os.environ)

        return self._env_vars

    @property
    def ini_config(self) -> ConfigParser:
        """
        Load and cache configuration from config.ini file.
        Returns an empty ConfigParser if file not found.
        """
        if self._ini_config is None:
            self._ini_config = ConfigParser()
            config_file = find_config_file("config.ini")
            if config_file:
                self._ini_config.read(config_file)

        return self._ini_config

    def get_env(self, name: str, default: Any = None) -> Any:
        """
        Get a configuration value from environment variables.

        Args:
            name: The name of the environment variable
            default: Default value if not found

        Returns:
            The configuration value or default
        """
        return self.env_vars.get(name, default)

    def get_ini(self, section: str, option: str, default: Any = None) -> Any:
        """
        Get a configuration value from an INI file.

        Args:
            section: The configuration section
            option: The option name in the section
            default: Default value if not found

        Returns:
            The configuration value or default
        """
        try:
            return self.ini_config.get(section, option)
        except (KeyError, NoSectionError, NoOptionError):
            return default


# Create a singleton instance for global use
_config_manager = ConfigManager()


def config(env_name: str, default_val: Any = None) -> Any:
    """
    Get a configuration value from environment variables.

    Args:
        env_name: The name of the environment variable
        default_val: Default value if not found

    Returns:
        The configuration value or default
    """
    return _config_manager.get_env(env_name, default_val)


def get_ini_config(section: str, option: str, default_val: Any = None) -> Any:
    """
    Get a configuration value from an INI file.

    Args:
        section: The configuration section
        option: The option name in the section
        default_val: Default value if not found

    Returns:
        The configuration value or default
    """
    return _config_manager.get_ini(section, option, default_val)
