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

def get_source_config(source_name: str) -> Dict[str, Any]:
    """Get configuration for a specific source"""
    config_dir = os.environ.get('CONFIG_DIR')
    if not config_dir:
        raise ValueError("CONFIG_DIR environment variable must be set")

    config = load_config(f'{config_dir}/sources.yaml')
    if source_name not in config['sources']:
        raise ValueError(f"Source '{source_name}' not found in configuration")
    return config['sources'][source_name]

def get_kafka_config(cluster_name: str = 'primary') -> Dict[str, Any]:
    """Get Kafka cluster configuration"""
    config_dir = os.environ.get('CONFIG_DIR')
    if not config_dir:
        raise ValueError("CONFIG_DIR environment variable must be set")

    config = load_config(f'{config_dir}/kafka_clusters.yaml')
    if cluster_name not in config['kafka_clusters']:
        raise ValueError(f"Kafka cluster '{cluster_name}' not found")
    return config['kafka_clusters'][cluster_name]
