"""
Data quality checks for ETL pipelines
Validates data integrity between source and destination
"""

import logging
import hashlib
from typing import Dict, List, Optional, Any, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, md5, concat_ws, lit
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """
    Performs data quality validations on ETL operations

    Checks:
    - Row count validation (source vs destination)
    - Column-level null checks for required fields
    - Data type validation
    - Checksum comparison for critical tables
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize data quality checker

        Args:
            spark: SparkSession
        """
        self.spark = spark

    def validate_row_count(self, source_count: int, dest_count: int,
                          table_name: str, tolerance: float = 0.0) -> Tuple[bool, Dict]:
        """
        Validate row counts match between source and destination

        Args:
            source_count: Source table row count
            dest_count: Destination table row count
            table_name: Table name
            tolerance: Acceptable difference percentage (0.0 = exact match)

        Returns:
            tuple: (passed: bool, details: dict)
        """
        if source_count == 0 and dest_count == 0:
            return True, {
                'check': 'row_count',
                'status': 'passed',
                'source_count': source_count,
                'dest_count': dest_count,
                'message': 'Both tables are empty'
            }

        # Calculate difference
        diff = abs(source_count - dest_count)
        diff_pct = (diff / source_count * 100) if source_count > 0 else 0

        passed = diff_pct <= (tolerance * 100)

        details = {
            'check': 'row_count',
            'status': 'passed' if passed else 'failed',
            'source_count': source_count,
            'dest_count': dest_count,
            'difference': diff,
            'difference_pct': diff_pct,
            'tolerance_pct': tolerance * 100
        }

        if passed:
            logger.info(f"{table_name}: Row count validation PASSED - "
                       f"source: {source_count}, dest: {dest_count}")
        else:
            logger.error(f"{table_name}: Row count validation FAILED - "
                        f"source: {source_count}, dest: {dest_count}, diff: {diff} ({diff_pct:.2f}%)")

        return passed, details

    def validate_null_constraints(self, df: DataFrame, required_columns: List[str],
                                  table_name: str) -> Tuple[bool, Dict]:
        """
        Validate that required columns have no null values

        Args:
            df: DataFrame to validate
            required_columns: List of column names that cannot be null
            table_name: Table name

        Returns:
            tuple: (passed: bool, details: dict)
        """
        null_counts = {}
        total_rows = df.count()

        for col_name in required_columns:
            if col_name not in df.columns:
                logger.warning(f"{table_name}: Required column '{col_name}' not found in DataFrame")
                continue

            null_count = df.filter(col(col_name).isNull()).count()
            null_counts[col_name] = null_count

        # Check if any required columns have nulls
        failed_columns = {col_name: count for col_name, count in null_counts.items() if count > 0}
        passed = len(failed_columns) == 0

        details = {
            'check': 'null_constraints',
            'status': 'passed' if passed else 'failed',
            'total_rows': total_rows,
            'required_columns_checked': len(required_columns),
            'null_counts': null_counts,
            'failed_columns': failed_columns if not passed else {}
        }

        if passed:
            logger.info(f"{table_name}: Null constraint validation PASSED - "
                       f"{len(required_columns)} columns checked")
        else:
            for col_name, null_count in failed_columns.items():
                logger.error(f"{table_name}: Null constraint FAILED - "
                           f"column '{col_name}' has {null_count} null values")

        return passed, details

    def validate_data_types(self, df: DataFrame, expected_schema: StructType,
                           table_name: str) -> Tuple[bool, Dict]:
        """
        Validate DataFrame schema matches expected data types

        Args:
            df: DataFrame to validate
            expected_schema: Expected schema
            table_name: Table name

        Returns:
            tuple: (passed: bool, details: dict)
        """
        actual_schema = df.schema
        actual_fields = {field.name: str(field.dataType) for field in actual_schema.fields}
        expected_fields = {field.name: str(field.dataType) for field in expected_schema.fields}

        # Check for mismatches
        mismatches = []
        for col_name, expected_type in expected_fields.items():
            if col_name not in actual_fields:
                mismatches.append({
                    'column': col_name,
                    'expected_type': expected_type,
                    'actual_type': 'MISSING',
                    'issue': 'column_missing'
                })
            elif actual_fields[col_name] != expected_type:
                mismatches.append({
                    'column': col_name,
                    'expected_type': expected_type,
                    'actual_type': actual_fields[col_name],
                    'issue': 'type_mismatch'
                })

        # Check for extra columns
        extra_columns = set(actual_fields.keys()) - set(expected_fields.keys())
        for col_name in extra_columns:
            mismatches.append({
                'column': col_name,
                'expected_type': 'N/A',
                'actual_type': actual_fields[col_name],
                'issue': 'extra_column'
            })

        passed = len(mismatches) == 0

        details = {
            'check': 'data_types',
            'status': 'passed' if passed else 'failed',
            'columns_checked': len(expected_fields),
            'mismatches': mismatches
        }

        if passed:
            logger.info(f"{table_name}: Data type validation PASSED - "
                       f"{len(expected_fields)} columns validated")
        else:
            logger.error(f"{table_name}: Data type validation FAILED - "
                        f"{len(mismatches)} issue(s) found")
            for mismatch in mismatches:
                logger.error(f"{table_name}: {mismatch['issue']}: column '{mismatch['column']}' - "
                           f"expected: {mismatch['expected_type']}, actual: {mismatch['actual_type']}")

        return passed, details

    def calculate_table_checksum(self, df: DataFrame, key_columns: List[str],
                                 value_columns: Optional[List[str]] = None) -> str:
        """
        Calculate a checksum for a table based on key and value columns

        Args:
            df: DataFrame
            key_columns: Columns to use as keys (typically primary keys)
            value_columns: Columns to include in checksum (default: all columns)

        Returns:
            str: MD5 checksum of the table
        """
        if value_columns is None:
            value_columns = [col for col in df.columns if col not in key_columns]

        # Concatenate all columns and calculate MD5 hash per row
        all_columns = key_columns + value_columns
        df_with_hash = df.select(
            md5(concat_ws('|', *[col(c).cast('string') for c in all_columns])).alias('row_hash')
        )

        # Aggregate all row hashes into a single checksum
        row_hashes = df_with_hash.select('row_hash').rdd.flatMap(lambda x: x).collect()
        row_hashes.sort()  # Sort for consistent checksums

        combined_hash = hashlib.md5(''.join(row_hashes).encode('utf-8')).hexdigest()
        return combined_hash

    def compare_table_checksums(self, source_df: DataFrame, dest_df: DataFrame,
                               key_columns: List[str], table_name: str,
                               value_columns: Optional[List[str]] = None) -> Tuple[bool, Dict]:
        """
        Compare checksums between source and destination tables

        Args:
            source_df: Source DataFrame
            dest_df: Destination DataFrame
            key_columns: Key columns for checksum
            table_name: Table name
            value_columns: Optional value columns to include

        Returns:
            tuple: (passed: bool, details: dict)
        """
        logger.info(f"{table_name}: Calculating table checksums...")

        source_checksum = self.calculate_table_checksum(source_df, key_columns, value_columns)
        dest_checksum = self.calculate_table_checksum(dest_df, key_columns, value_columns)

        passed = source_checksum == dest_checksum

        details = {
            'check': 'table_checksum',
            'status': 'passed' if passed else 'failed',
            'source_checksum': source_checksum,
            'dest_checksum': dest_checksum,
            'key_columns': key_columns,
            'value_columns_count': len(value_columns) if value_columns else len(source_df.columns)
        }

        if passed:
            logger.info(f"{table_name}: Checksum validation PASSED - checksums match")
        else:
            logger.error(f"{table_name}: Checksum validation FAILED - "
                        f"source: {source_checksum}, dest: {dest_checksum}")

        return passed, details

    def validate_column_statistics(self, source_df: DataFrame, dest_df: DataFrame,
                                   numeric_columns: List[str], table_name: str,
                                   tolerance: float = 0.01) -> Tuple[bool, Dict]:
        """
        Compare column statistics (sum, count, etc.) between source and destination

        Args:
            source_df: Source DataFrame
            dest_df: Destination DataFrame
            numeric_columns: Numeric columns to compare
            table_name: Table name
            tolerance: Acceptable difference percentage (default: 1%)

        Returns:
            tuple: (passed: bool, details: dict)
        """
        statistics = {}
        all_passed = True

        for col_name in numeric_columns:
            if col_name not in source_df.columns or col_name not in dest_df.columns:
                logger.warning(f"{table_name}: Column '{col_name}' not found in both DataFrames")
                continue

            # Calculate sum for both
            source_sum = source_df.agg(spark_sum(col(col_name))).first()[0] or 0
            dest_sum = dest_df.agg(spark_sum(col(col_name))).first()[0] or 0

            # Calculate difference
            diff = abs(source_sum - dest_sum)
            diff_pct = (diff / source_sum * 100) if source_sum != 0 else 0

            passed = diff_pct <= (tolerance * 100)
            all_passed = all_passed and passed

            statistics[col_name] = {
                'source_sum': source_sum,
                'dest_sum': dest_sum,
                'difference': diff,
                'difference_pct': diff_pct,
                'passed': passed
            }

            if not passed:
                logger.error(f"{table_name}: Column '{col_name}' statistics FAILED - "
                           f"source_sum: {source_sum}, dest_sum: {dest_sum}, diff: {diff_pct:.2f}%")

        details = {
            'check': 'column_statistics',
            'status': 'passed' if all_passed else 'failed',
            'columns_checked': len(numeric_columns),
            'statistics': statistics
        }

        if all_passed:
            logger.info(f"{table_name}: Column statistics validation PASSED - "
                       f"{len(numeric_columns)} columns validated")

        return all_passed, details

    def run_all_checks(self, source_df: DataFrame, dest_df: DataFrame,
                      table_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run all configured data quality checks

        Args:
            source_df: Source DataFrame
            dest_df: Destination DataFrame
            table_name: Table name
            config: Configuration dict with check parameters

        Returns:
            dict: Results of all checks
        """
        results = {
            'table_name': table_name,
            'checks_run': [],
            'all_passed': True
        }

        # Row count check
        if config.get('check_row_count', True):
            source_count = source_df.count()
            dest_count = dest_df.count()
            passed, details = self.validate_row_count(
                source_count, dest_count, table_name,
                tolerance=config.get('row_count_tolerance', 0.0)
            )
            results['checks_run'].append(details)
            results['all_passed'] = results['all_passed'] and passed

        # Null constraints check
        if config.get('required_columns'):
            passed, details = self.validate_null_constraints(
                dest_df, config['required_columns'], table_name
            )
            results['checks_run'].append(details)
            results['all_passed'] = results['all_passed'] and passed

        # Data type check
        if config.get('expected_schema'):
            passed, details = self.validate_data_types(
                dest_df, config['expected_schema'], table_name
            )
            results['checks_run'].append(details)
            results['all_passed'] = results['all_passed'] and passed

        # Checksum check (for critical tables only - expensive)
        if config.get('check_checksum', False) and config.get('key_columns'):
            passed, details = self.compare_table_checksums(
                source_df, dest_df, config['key_columns'], table_name,
                value_columns=config.get('checksum_columns')
            )
            results['checks_run'].append(details)
            results['all_passed'] = results['all_passed'] and passed

        # Column statistics check
        if config.get('numeric_columns'):
            passed, details = self.validate_column_statistics(
                source_df, dest_df, config['numeric_columns'], table_name,
                tolerance=config.get('statistics_tolerance', 0.01)
            )
            results['checks_run'].append(details)
            results['all_passed'] = results['all_passed'] and passed

        return results
