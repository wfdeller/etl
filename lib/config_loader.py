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

def load_config_raw(config_file: str) -> Dict[str, Any]:
    """Load YAML configuration file without environment variable substitution"""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def load_config(config_file: str) -> Dict[str, Any]:
    """Load YAML configuration file with environment variable substitution"""
    config = load_config_raw(config_file)
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


def _validate_bulk_load_config(source_name: str, source_config: Dict[str, Any]) -> None:
    """
    Validate bulk_load configuration section if present

    Ensures:
    - bulk_load is a dictionary
    - parallel_tables is a positive integer (JDBC partitions per table)
    - parallel_workers is a positive integer (concurrent table processing)
    - batch_size is a positive integer (JDBC fetch size)
    - skip_empty_tables is a boolean
    - handle_long_columns is one of: exclude, skip_table, error (Oracle only)
    - checkpoint_enabled is a boolean
    - max_retries is a non-negative integer
    - retry_backoff is a positive number
    - large_table_threshold is a positive integer
    """
    if 'bulk_load' not in source_config:
        return  # Optional section

    bulk_load = source_config['bulk_load']

    if not isinstance(bulk_load, dict):
        raise ValueError(
            f"Source '{source_name}': 'bulk_load' must be a dictionary, got {type(bulk_load).__name__}"
        )

    # Validate parallel_tables
    if 'parallel_tables' in bulk_load:
        partitions = bulk_load['parallel_tables']
        if not isinstance(partitions, int) or partitions <= 0:
            raise ValueError(
                f"Source '{source_name}': 'parallel_tables' must be a positive integer, got {partitions}"
            )

    # Validate parallel_workers
    if 'parallel_workers' in bulk_load:
        workers = bulk_load['parallel_workers']
        if not isinstance(workers, int) or workers <= 0:
            raise ValueError(
                f"Source '{source_name}': 'parallel_workers' must be a positive integer, got {workers}"
            )

    # Validate batch_size
    if 'batch_size' in bulk_load:
        batch_size = bulk_load['batch_size']
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError(
                f"Source '{source_name}': 'batch_size' must be a positive integer, got {batch_size}"
            )

    # Validate skip_empty_tables
    if 'skip_empty_tables' in bulk_load:
        skip_empty = bulk_load['skip_empty_tables']
        if not isinstance(skip_empty, bool):
            raise ValueError(
                f"Source '{source_name}': 'skip_empty_tables' must be a boolean, got {type(skip_empty).__name__}"
            )

    # Validate handle_long_columns (Oracle only)
    if 'handle_long_columns' in bulk_load:
        handle_long = bulk_load['handle_long_columns']
        valid_options = ['exclude', 'skip_table', 'error']
        if handle_long not in valid_options:
            raise ValueError(
                f"Source '{source_name}': 'handle_long_columns' must be one of {valid_options}, got '{handle_long}'"
            )

    # Validate checkpoint_enabled
    if 'checkpoint_enabled' in bulk_load:
        checkpoint = bulk_load['checkpoint_enabled']
        if not isinstance(checkpoint, bool):
            raise ValueError(
                f"Source '{source_name}': 'checkpoint_enabled' must be a boolean, got {type(checkpoint).__name__}"
            )

    # Validate max_retries
    if 'max_retries' in bulk_load:
        retries = bulk_load['max_retries']
        if not isinstance(retries, int) or retries < 0:
            raise ValueError(
                f"Source '{source_name}': 'max_retries' must be a non-negative integer, got {retries}"
            )

    # Validate retry_backoff
    if 'retry_backoff' in bulk_load:
        backoff = bulk_load['retry_backoff']
        if not isinstance(backoff, (int, float)) or backoff <= 0:
            raise ValueError(
                f"Source '{source_name}': 'retry_backoff' must be a positive number, got {backoff}"
            )

    # Validate large_table_threshold
    if 'large_table_threshold' in bulk_load:
        threshold = bulk_load['large_table_threshold']
        if not isinstance(threshold, int) or threshold <= 0:
            raise ValueError(
                f"Source '{source_name}': 'large_table_threshold' must be a positive integer, got {threshold}"
            )


def _validate_data_quality_config(source_name: str, source_config: Dict[str, Any]) -> None:
    """
    Validate data_quality configuration section if present

    Ensures:
    - data_quality is a dictionary
    - check_row_count is a boolean
    - row_count_tolerance is a float between 0 and 1
    - required_columns is a list of strings (if present)
    """
    if 'data_quality' not in source_config:
        return  # Optional section

    data_quality = source_config['data_quality']

    if not isinstance(data_quality, dict):
        raise ValueError(
            f"Source '{source_name}': 'data_quality' must be a dictionary, got {type(data_quality).__name__}"
        )

    # Validate check_row_count
    if 'check_row_count' in data_quality:
        check = data_quality['check_row_count']
        if not isinstance(check, bool):
            raise ValueError(
                f"Source '{source_name}': 'check_row_count' must be a boolean, got {type(check).__name__}"
            )

    # Validate row_count_tolerance
    if 'row_count_tolerance' in data_quality:
        tolerance = data_quality['row_count_tolerance']
        if not isinstance(tolerance, (int, float)) or tolerance < 0 or tolerance > 1:
            raise ValueError(
                f"Source '{source_name}': 'row_count_tolerance' must be a float between 0 and 1, got {tolerance}"
            )

    # Validate required_columns
    if 'required_columns' in data_quality:
        columns = data_quality['required_columns']
        if not isinstance(columns, dict):
            raise ValueError(
                f"Source '{source_name}': 'required_columns' must be a dictionary (table -> columns), "
                f"got {type(columns).__name__}"
            )

        for table_name, column_list in columns.items():
            if not isinstance(column_list, list):
                raise ValueError(
                    f"Source '{source_name}', table '{table_name}': "
                    f"required columns must be a list, got {type(column_list).__name__}"
                )

            for column in column_list:
                if not isinstance(column, str):
                    raise ValueError(
                        f"Source '{source_name}', table '{table_name}': "
                        f"column names must be strings, got {type(column).__name__}"
                    )


def get_source_config(source_name: str) -> Dict[str, Any]:
    """Get configuration for a specific source

    Only validates environment variables for the requested source,
    not all sources in the config file.
    """
    config_dir = os.environ.get('CONFIG_DIR')
    if not config_dir:
        raise ValueError("CONFIG_DIR environment variable must be set")

    # Load raw config without env var substitution
    config = load_config_raw(f'{config_dir}/sources.yaml')
    if source_name not in config['sources']:
        raise ValueError(f"Source '{source_name}' not found in configuration")

    # Extract only the requested source and substitute env vars for it
    source_config = config['sources'][source_name]
    source_config = _substitute_env_vars(source_config)

    # Validate configuration sections
    _validate_primary_keys(source_name, source_config)
    _validate_bulk_load_config(source_name, source_config)
    _validate_data_quality_config(source_name, source_config)

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
