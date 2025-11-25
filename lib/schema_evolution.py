"""
Schema evolution management for CDC pipelines
Handles automatic schema evolution using Iceberg's built-in capabilities
"""

import logging
from typing import List, Dict, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, DataType,
    ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, DecimalType,
    StringType, BinaryType, BooleanType,
    TimestampType, DateType
)

logger = logging.getLogger(__name__)


class SchemaEvolutionManager:
    """
    Manages automatic schema evolution for Iceberg tables

    Determines which schema changes are safe to apply automatically:
    - Column additions (always safe if nullable)
    - Type promotions (e.g., int -> long, float -> double)
    - Column removals (configurable, requires manual intervention by default)
    """

    # Type promotion map: smaller type -> larger type (safe promotions)
    TYPE_PROMOTIONS = {
        'ByteType': ['ShortType', 'IntegerType', 'LongType'],
        'ShortType': ['IntegerType', 'LongType'],
        'IntegerType': ['LongType'],
        'FloatType': ['DoubleType'],
    }

    def __init__(self, spark: SparkSession, allow_column_additions: bool = True,
                 allow_type_promotions: bool = True, allow_column_removals: bool = False):
        """
        Initialize schema evolution manager

        Args:
            spark: SparkSession
            allow_column_additions: Allow automatic addition of new columns
            allow_type_promotions: Allow safe type promotions (e.g., int -> long)
            allow_column_removals: Allow column removals (NOT RECOMMENDED)
        """
        self.spark = spark
        self.allow_column_additions = allow_column_additions
        self.allow_type_promotions = allow_type_promotions
        self.allow_column_removals = allow_column_removals

    @staticmethod
    def _get_type_name(data_type: DataType) -> str:
        """Get the simple type name from a DataType"""
        return type(data_type).__name__

    @staticmethod
    def _is_safe_type_promotion(old_type: DataType, new_type: DataType) -> bool:
        """
        Check if a type change is a safe promotion

        Args:
            old_type: Original data type
            new_type: New data type

        Returns:
            bool: True if promotion is safe
        """
        old_type_name = SchemaEvolutionManager._get_type_name(old_type)
        new_type_name = SchemaEvolutionManager._get_type_name(new_type)

        # Check if promotion is in allowed list
        if old_type_name in SchemaEvolutionManager.TYPE_PROMOTIONS:
            return new_type_name in SchemaEvolutionManager.TYPE_PROMOTIONS[old_type_name]

        return False

    def analyze_schema_changes(self, current_schema: StructType, new_schema: StructType) -> Dict[str, List[Dict]]:
        """
        Analyze differences between current and new schemas

        Args:
            current_schema: Current table schema
            new_schema: New schema from source

        Returns:
            dict: Categorized changes with keys:
                - additions: List of new columns
                - removals: List of removed columns
                - modifications: List of type changes
                - safe_evolutions: List of changes that can be applied automatically
                - unsafe_changes: List of changes requiring manual intervention
        """
        current_fields = {field.name: field for field in current_schema.fields}
        new_fields = {field.name: field for field in new_schema.fields}

        additions = []
        removals = []
        modifications = []
        safe_evolutions = []
        unsafe_changes = []

        # Detect additions
        for col_name, field in new_fields.items():
            if col_name not in current_fields:
                change = {
                    'type': 'addition',
                    'column': col_name,
                    'data_type': self._get_type_name(field.dataType),
                    'nullable': field.nullable
                }
                additions.append(change)

                # Column additions are safe if nullable or if additions are allowed
                if field.nullable and self.allow_column_additions:
                    safe_evolutions.append(change)
                elif not field.nullable:
                    unsafe_changes.append({**change, 'reason': 'non-nullable column addition requires default value'})

        # Detect removals
        for col_name, field in current_fields.items():
            if col_name not in new_fields:
                change = {
                    'type': 'removal',
                    'column': col_name,
                    'data_type': self._get_type_name(field.dataType)
                }
                removals.append(change)

                # Column removals are generally unsafe (data loss)
                if self.allow_column_removals:
                    safe_evolutions.append(change)
                else:
                    unsafe_changes.append({**change, 'reason': 'column removal causes data loss'})

        # Detect modifications (type changes)
        for col_name in set(current_fields.keys()) & set(new_fields.keys()):
            old_field = current_fields[col_name]
            new_field = new_fields[col_name]
            old_type = old_field.dataType
            new_type = new_field.dataType

            if self._get_type_name(old_type) != self._get_type_name(new_type):
                change = {
                    'type': 'modification',
                    'column': col_name,
                    'old_type': self._get_type_name(old_type),
                    'new_type': self._get_type_name(new_type)
                }
                modifications.append(change)

                # Check if this is a safe type promotion
                if self.allow_type_promotions and self._is_safe_type_promotion(old_type, new_type):
                    safe_evolutions.append(change)
                else:
                    unsafe_changes.append({**change, 'reason': 'type change may cause data loss or incompatibility'})

        return {
            'additions': additions,
            'removals': removals,
            'modifications': modifications,
            'safe_evolutions': safe_evolutions,
            'unsafe_changes': unsafe_changes
        }

    def can_auto_evolve(self, current_schema: StructType, new_schema: StructType) -> Tuple[bool, Dict]:
        """
        Determine if schema can be automatically evolved

        Args:
            current_schema: Current table schema
            new_schema: New schema from source

        Returns:
            tuple: (can_evolve: bool, analysis: dict)
        """
        analysis = self.analyze_schema_changes(current_schema, new_schema)

        # Can auto-evolve if there are no unsafe changes
        can_evolve = len(analysis['unsafe_changes']) == 0

        return can_evolve, analysis

    def apply_schema_evolution(self, table_name: str, safe_evolutions: List[Dict]) -> bool:
        """
        Apply safe schema evolution changes to Iceberg table

        Note: Iceberg handles schema evolution automatically when mergeSchema=true
        This method logs the changes that will be applied

        Args:
            table_name: Fully qualified table name
            safe_evolutions: List of safe changes to apply

        Returns:
            bool: True if evolution should proceed
        """
        if not safe_evolutions:
            logger.debug(f"{table_name}: No schema evolution needed")
            return True

        logger.info(f"{table_name}: Applying automatic schema evolution ({len(safe_evolutions)} changes)")

        for change in safe_evolutions:
            change_type = change['type']
            column = change['column']

            if change_type == 'addition':
                logger.info(f"{table_name}: AUTO-EVOLVE: Adding column '{column}' "
                          f"(type: {change['data_type']}, nullable: {change['nullable']})")

            elif change_type == 'modification':
                logger.info(f"{table_name}: AUTO-EVOLVE: Promoting column '{column}' type "
                          f"from {change['old_type']} to {change['new_type']}")

            elif change_type == 'removal':
                logger.warning(f"{table_name}: AUTO-EVOLVE: Removing column '{column}' "
                             f"(type: {change['data_type']}) - DATA LOSS POSSIBLE")

        return True

    def log_unsafe_changes(self, table_name: str, unsafe_changes: List[Dict]):
        """
        Log unsafe schema changes that require manual intervention

        Args:
            table_name: Table name
            unsafe_changes: List of unsafe changes
        """
        if not unsafe_changes:
            return

        logger.error(f"{table_name}: Schema contains {len(unsafe_changes)} UNSAFE change(s) requiring manual intervention:")

        for change in unsafe_changes:
            change_type = change['type']
            column = change['column']
            reason = change.get('reason', 'unknown')

            if change_type == 'addition':
                logger.error(f"{table_name}: UNSAFE: Cannot add non-nullable column '{column}' - {reason}")

            elif change_type == 'modification':
                logger.error(f"{table_name}: UNSAFE: Cannot change column '{column}' type "
                           f"from {change['old_type']} to {change['new_type']} - {reason}")

            elif change_type == 'removal':
                logger.error(f"{table_name}: UNSAFE: Cannot remove column '{column}' - {reason}")

    @staticmethod
    def merge_schemas(base_schema: StructType, new_schema: StructType) -> StructType:
        """
        Merge two schemas, adding new fields from new_schema to base_schema

        Args:
            base_schema: Base schema
            new_schema: Schema with potential new fields

        Returns:
            StructType: Merged schema
        """
        base_fields = {field.name: field for field in base_schema.fields}
        new_fields_list = list(base_schema.fields)

        # Add new fields that don't exist in base
        for field in new_schema.fields:
            if field.name not in base_fields:
                new_fields_list.append(field)
                logger.debug(f"Adding field to merged schema: {field.name} ({field.dataType})")

        return StructType(new_fields_list)


class SchemaEvolutionPolicy:
    """
    Configurable policies for schema evolution
    """

    # Permissive: allow additions and type promotions
    PERMISSIVE = {
        'allow_column_additions': True,
        'allow_type_promotions': True,
        'allow_column_removals': False
    }

    # Strict: no automatic changes
    STRICT = {
        'allow_column_additions': False,
        'allow_type_promotions': False,
        'allow_column_removals': False
    }

    # Additive only: only allow new columns
    ADDITIVE_ONLY = {
        'allow_column_additions': True,
        'allow_type_promotions': False,
        'allow_column_removals': False
    }

    @staticmethod
    def get_policy(policy_name: str) -> Dict:
        """
        Get a predefined policy by name

        Args:
            policy_name: Policy name ('permissive', 'strict', 'additive_only')

        Returns:
            dict: Policy configuration
        """
        policies = {
            'permissive': SchemaEvolutionPolicy.PERMISSIVE,
            'strict': SchemaEvolutionPolicy.STRICT,
            'additive_only': SchemaEvolutionPolicy.ADDITIVE_ONLY
        }

        return policies.get(policy_name.lower(), SchemaEvolutionPolicy.PERMISSIVE)
