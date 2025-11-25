import yaml
import os
import re
from typing import Dict, Any

def _substitute_env_vars(config: Any) -> Any:
    """
    Recursively substitute environment variables in config
    Supports ${ENV_VAR_NAME} or ${ENV_VAR_NAME:default_value} syntax
    """
    if isinstance(config, dict):
        return {key: _substitute_env_vars(value) for key, value in config.items()}
    elif isinstance(config, list):
        return [_substitute_env_vars(item) for item in config]
    elif isinstance(config, str):
        # Match ${VAR} or ${VAR:default}
        pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'

        def replace_env_var(match):
            var_name = match.group(1)
            default_value = match.group(2)

            value = os.environ.get(var_name)
            if value is None:
                if default_value is not None:
                    return default_value
                else:
                    raise ValueError(
                        f"Environment variable '{var_name}' not found and no default provided. "
                        f"Set the variable or provide a default with ${{VAR:default}}"
                    )
            return value

        return re.sub(pattern, replace_env_var, config)
    else:
        return config

def load_config(config_file: str) -> Dict[str, Any]:
    """Load YAML configuration file with environment variable substitution"""
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return _substitute_env_vars(config)

def _validate_primary_keys(source_name: str, source_config: Dict[str, Any]) -> None:
    """
    Validate primary_keys configuration if present

    Ensures:
    - primary_keys is a dictionary
    - Each value is a non-empty list of strings
    """
    if 'primary_keys' not in source_config:
        return  # Optional section

    primary_keys = source_config['primary_keys']

    if not isinstance(primary_keys, dict):
        raise ValueError(
            f"Source '{source_name}': 'primary_keys' must be a dictionary, got {type(primary_keys).__name__}"
        )

    for table_name, pk_columns in primary_keys.items():
        if not isinstance(pk_columns, list):
            raise ValueError(
                f"Source '{source_name}', table '{table_name}': "
                f"primary key columns must be a list, got {type(pk_columns).__name__}"
            )

        if len(pk_columns) == 0:
            raise ValueError(
                f"Source '{source_name}', table '{table_name}': "
                f"primary key columns list cannot be empty"
            )

        for column in pk_columns:
            if not isinstance(column, str):
                raise ValueError(
                    f"Source '{source_name}', table '{table_name}': "
                    f"column names must be strings, got {type(column).__name__}"
                )


def get_source_config(source_name: str) -> Dict[str, Any]:
    """Get configuration for a specific source"""
    config_dir = os.environ.get('CONFIG_DIR')
    if not config_dir:
        raise ValueError("CONFIG_DIR environment variable must be set")

    config = load_config(f'{config_dir}/sources.yaml')
    if source_name not in config['sources']:
        raise ValueError(f"Source '{source_name}' not found in configuration")

    source_config = config['sources'][source_name]

    # Validate primary_keys section if present
    _validate_primary_keys(source_name, source_config)

    return source_config

def get_kafka_config(cluster_name: str = 'primary') -> Dict[str, Any]:
    """Get Kafka cluster configuration"""
    config_dir = os.environ.get('CONFIG_DIR')
    if not config_dir:
        raise ValueError("CONFIG_DIR environment variable must be set")

    config = load_config(f'{config_dir}/kafka_clusters.yaml')
    if cluster_name not in config['kafka_clusters']:
        raise ValueError(f"Kafka cluster '{cluster_name}' not found")
    return config['kafka_clusters'][cluster_name]
