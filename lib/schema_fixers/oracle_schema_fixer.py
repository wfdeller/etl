"""
Oracle schema type fixes
"""

import logging
from pyspark.sql.types import DecimalType, DoubleType
from pyspark.sql.functions import col
from .base_schema_fixer import BaseSchemFixer

logger = logging.getLogger(__name__)


class OracleSchemFixer(BaseSchemFixer):
    """Fix Oracle-specific type issues for Spark/Iceberg compatibility"""
    
    @staticmethod
    def fix_schema(df):
        """
        Fix all Oracle schema issues
        
        - Convert problematic NUMBER types to DoubleType
        - Handle other Oracle-specific types as needed
        """
        
        df = OracleSchemFixer._fix_number_types(df)
        return df
    
    @staticmethod
    def _fix_number_types(df):
        """
        Fix Oracle NUMBER columns that Spark/Iceberg can't handle.
        Oracle NUMBER without precision comes through as DecimalType(38,127) which is invalid.
        Convert all such columns to DoubleType.
        """
        
        for field in df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                # Check for problematic DecimalType
                if (field.dataType.precision is None or 
                    field.dataType.scale is None or
                    field.dataType.precision > 38 or 
                    field.dataType.scale > 38 or 
                    field.dataType.scale < 0):
                    logger.info(f"  Converting {field.name} from {field.dataType} to DoubleType")
                    df = df.withColumn(field.name, col(field.name).cast(DoubleType()))
        
        return df
