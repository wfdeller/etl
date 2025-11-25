"""
Extractor factory
"""

from .oracle_extractor import OracleTableExtractor
from .postgres_extractor import PostgresTableExtractor


def get_extractor(spark, jdbc_config, db_type, config):
    """
    Factory function to get appropriate extractor for database type
    
    Args:
        spark: SparkSession
        jdbc_config: JDBC connection config
        db_type: 'oracle' or 'postgres'
        config: bulk_load configuration
        
    Returns:
        BaseTableExtractor subclass instance
    """
    
    extractors = {
        'oracle': OracleTableExtractor,
        'postgres': PostgresTableExtractor
    }
    
    if db_type not in extractors:
        raise ValueError(f"Unsupported database type: {db_type}. Supported: {list(extractors.keys())}")
    
    return extractors[db_type](spark, jdbc_config, config)


__all__ = ['get_extractor', 'OracleTableExtractor', 'PostgresTableExtractor']
