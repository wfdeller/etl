"""
Base schema fixer
"""

from abc import ABC, abstractmethod


class BaseSchemFixer(ABC):
    """Base class for database-specific schema fixes"""
    
    @staticmethod
    @abstractmethod
    def fix_schema(df):
        """
        Fix database-specific schema issues
        
        Args:
            df: PySpark DataFrame
            
        Returns:
            DataFrame with fixed schema
        """
        pass
