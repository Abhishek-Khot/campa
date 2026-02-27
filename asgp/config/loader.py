"""
Configuration Loader with Environment Variable Support
Supports all 5 NoSQL database credential fields
"""
import os
import re
from pathlib import Path
from typing import Dict, Any
import yaml
from copy import deepcopy

from asgp.config.schemas import ASGPConfig


class ConfigLoader:
    """Loads and validates configuration files"""

    # All fields that may contain ${ENV_VAR} placeholders
    _ENV_FIELDS = [
        'uri', 'database', 'username', 'password',
        'contact_points', 'keyspace',
        'access_key', 'secret_key', 'endpoint_url', 'region',
        'connection_string', 'bucket',
    ]

    @staticmethod
    def load_file(file_path: Path) -> ASGPConfig:
        """
        Load, validate, and resolve configuration file

        Steps:
        1. Load YAML
        2. Validate structure (with placeholders intact)
        3. Substitute environment variables IN PLACE
        4. Return resolved config
        """
        try:
            # Step 1: Load YAML
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data = yaml.safe_load(f)

            if not raw_data:
                raise ValueError(f"Empty configuration file: {file_path}")

            # Step 2: Validate with placeholders intact
            config = ASGPConfig(**raw_data)

            # Step 3: Substitute environment variables IN PLACE
            ConfigLoader._substitute_env_vars_inplace(config)

            return config

        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {file_path}: {e}")
        except Exception as e:
            raise ValueError(f"Config validation failed in {file_path}: {e}")

    @staticmethod
    def _substitute_env_vars_inplace(config: ASGPConfig) -> None:
        """
        Substitute ${ENV_VAR} placeholders with actual values
        Modifies the config object in place for all credential fields
        """
        for source in config.source_config:
            for field_name in ConfigLoader._ENV_FIELDS:
                value = getattr(source, field_name, None)
                if value:
                    resolved = ConfigLoader._resolve_placeholder(value)
                    setattr(source, field_name, resolved)

    @staticmethod
    def _resolve_placeholder(value: str) -> str:
        """
        Replace ${ENV_VAR} with environment variable value
        """
        if not value or not isinstance(value, str):
            return value

        # Match ${VAR_NAME}
        pattern = r'\$\{([^}]+)\}'

        def replacer(match):
            var_name = match.group(1)
            env_value = os.getenv(var_name)

            if env_value is None:
                raise ValueError(
                    f"Environment variable '{var_name}' not set. "
                    f"Add it to your .env file or set it in your environment."
                )

            return env_value

        return re.sub(pattern, replacer, value)


def validate_credentials(config: ASGPConfig) -> None:
    """
    Additional validation helper (optional)
    """
    pass