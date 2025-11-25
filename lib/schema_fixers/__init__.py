"""
Schema fixer factory
"""

from .oracle_schema_fixer import OracleSchemFixer
from .postgres_schema_fixer import PostgresSchemFixer


def get_schema_fixer(db_type):
    """
    Factory function to get appropriate schema fixer for database type
    
    Args:
        db_type: 'oracle' or 'postgres'
        
    Returns:
        BaseSchemFixer subclass
    """
    
    fixers = {
        'oracle': OracleSchemFixer,
        'postgres': PostgresSchemFixer
    }
    
    if db_type not in fixers:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    return fixers[db_type]


__all__ = ['get_schema_fixer', 'OracleSchemFixer', 'PostgresSchemFixer']
