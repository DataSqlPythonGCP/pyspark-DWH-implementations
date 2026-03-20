"""
Data Archival Script for BigQuery
---------------------------------
This script automates the process of creating quarterly snapshots of tables in BigQuery.
It creates copy tables, populates them with partition data, creates snapshot tables,
and cleans up intermediate tables.

Features:
- Quarterly snapshot creation based on previous quarter
- Support for partitioned tables
- Configurable expiration for snapshots
- Comprehensive logging and error handling
- Environment-based configuration
"""

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
from google.api_core.exceptions import GoogleAPIError
import os
import datetime
import csv
import json
import logging
import sys
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('data_archival.log')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Configuration class for the archival process"""
    project_id: str
    source_dataset_id: str
    target_dataset_id: str
    source_table_list_path: str
    snapshot_expiration_days: int = 365
    copy_table_suffix: str = '_copy'
    snapshot_table_suffix: str = '_snapshot'
    quarter_offset_months: int = -3  # Previous quarter

    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables"""
        required_vars = [
            'BQ_PROJECT_ID',
            'BQ_SOURCE_DATASET_ID',
            'BQ_TARGET_DATASET_ID',
            'BQ_SOURCE_TABLE_LIST_PATH'
        ]

        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

        return cls(
            project_id=os.getenv('BQ_PROJECT_ID'),
            source_dataset_id=os.getenv('BQ_SOURCE_DATASET_ID'),
            target_dataset_id=os.getenv('BQ_TARGET_DATASET_ID'),
            source_table_list_path=os.getenv('BQ_SOURCE_TABLE_LIST_PATH'),
            snapshot_expiration_days=int(os.getenv('BQ_SNAPSHOT_EXPIRATION_DAYS', '365')),
            quarter_offset_months=int(os.getenv('BQ_QUARTER_OFFSET_MONTHS', '-3'))
        )


class BigQueryArchiver:
    """Handles BigQuery archival operations"""

    def __init__(self, config: Config):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        self.qstart_date: Optional[datetime.date] = None
        self.qend_date: Optional[datetime.date] = None
        self.source_tables: List[str] = []
        self.copy_tables: List[str] = []

    def get_quarter_dates(self) -> None:
        """
        Calculate previous quarter start and end dates.
        Uses SQL for consistent date handling with BigQuery.
        """
        sql_query = """
            SELECT 
                DATE_SUB(LAST_DAY(DATE_ADD(CURRENT_DATE(), INTERVAL -1 QUARTER), QUARTER), INTERVAL 3 MONTH) as qstart_date,
                LAST_DAY(DATE_ADD(CURRENT_DATE(), INTERVAL -1 QUARTER), QUARTER) as qend_date
        """

        try:
            query_job = self.client.query(sql_query)
            results = query_job.result()

            for row in results:
                self.qstart_date = row.qstart_date
                self.qend_date = row.qend_date

            logger.info(f"Quarter start date: {self.qstart_date}")
            logger.info(f"Quarter end date: {self.qend_date}")

        except GoogleAPIError as e:
            logger.error(f"Failed to get quarter dates: {e}")
            raise

    def load_source_tables(self) -> None:
        """
        Load source table list from CSV file.
        Expected CSV format: header row with table names in first column.
        """
        try:
            file_path = Path(self.config.source_table_list_path)
            if not file_path.exists():
                raise FileNotFoundError(f"Table list file not found: {file_path}")

            with open(file_path, 'r') as file:
                csv_reader = csv.reader(file)
                header = next(csv_reader)  # Skip header

                for row in csv_reader:
                    if row and row[0].strip():
                        table_name = row[0].strip()
                        self.source_tables.append(table_name)
                        logger.info(f"Loaded table: {table_name}")

            logger.info(f"Loaded {len(self.source_tables)} tables for archival")

        except Exception as e:
            logger.error(f"Failed to load source tables: {e}")
            raise

    def get_partition_column(self, table_name: str) -> Optional[str]:
        """
        Get the partitioning column for a table, if any.

        Args:
            table_name: Name of the table

        Returns:
            Partition column name or None if not partitioned
        """
        sql_query = f"""
            SELECT column_name
            FROM `{self.config.project_id}.{self.config.source_dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
                AND is_partitioning_column = 'YES'
        """

        try:
            query_job = self.client.query(sql_query)
            results = query_job.result()

            for row in results:
                return row.column_name
            return None

        except GoogleAPIError as e:
            logger.error(f"Failed to get partition column for {table_name}: {e}")
            return None

    def create_copy_tables(self) -> None:
        """
        Create empty copy tables with the same schema as source tables.
        """
        date_suffix = f"{self.qstart_date.strftime('%Y%m%d')}_{self.qend_date.strftime('%Y%m%d')}"

        for table_name in self.source_tables:
            copy_table_name = f"{table_name}{self.config.copy_table_suffix}_{date_suffix}"
            self.copy_tables.append(copy_table_name)

            create_query = f"""
                CREATE OR REPLACE TABLE `{self.config.project_id}.{self.config.target_dataset_id}.{copy_table_name}`
                LIKE `{self.config.project_id}.{self.config.source_dataset_id}.{table_name}`
            """

            try:
                logger.info(f"Creating copy table: {copy_table_name}")
                query_job = self.client.query(create_query)
                query_job.result()  # Wait for completion
                logger.info(f"Successfully created copy table: {copy_table_name}")

            except GoogleAPIError as e:
                logger.error(f"Failed to create copy table {copy_table_name}: {e}")
                raise

    def copy_partition_data(self) -> None:
        """
        Copy data for the specified quarter range into copy tables.
        """
        date_suffix = f"{self.qstart_date.strftime('%Y%m%d')}_{self.qend_date.strftime('%Y%m%d')}"
        start_date_str = self.qstart_date.strftime('%Y-%m-%d')
        end_date_str = self.qend_date.strftime('%Y-%m-%d')

        for idx, source_table in enumerate(self.source_tables):
            copy_table = self.copy_tables[idx]
            partition_column = self.get_partition_column(source_table)

            if partition_column:
                # Copy only quarter's data
                insert_query = f"""
                    INSERT INTO `{self.config.project_id}.{self.config.target_dataset_id}.{copy_table}`
                    SELECT *
                    FROM `{self.config.project_id}.{self.config.source_dataset_id}.{source_table}`
                    WHERE DATE({partition_column}) BETWEEN '{start_date_str}' AND '{end_date_str}'
                """
            else:
                # Copy all data for non-partitioned tables
                insert_query = f"""
                    INSERT INTO `{self.config.project_id}.{self.config.target_dataset_id}.{copy_table}`
                    SELECT *
                    FROM `{self.config.project_id}.{self.config.source_dataset_id}.{source_table}`
                """

            try:
                logger.info(f"Copying data for {source_table} to {copy_table}")
                query_job = self.client.query(insert_query)
                query_job.result()  # Wait for completion

                row_count = query_job.num_dml_affected_rows
                logger.info(f"Successfully copied {row_count} rows to {copy_table}")

            except GoogleAPIError as e:
                logger.error(f"Failed to copy data for {source_table}: {e}")
                raise

    def create_snapshots(self) -> None:
        """
        Create snapshot tables from copy tables with expiration.
        """
        date_suffix = f"{self.qstart_date.strftime('%Y%m%d')}_{self.qend_date.strftime('%Y%m%d')}"

        for idx, source_table in enumerate(self.source_tables):
            copy_table = self.copy_tables[idx]
            snapshot_table = f"{source_table}{self.config.snapshot_table_suffix}_{date_suffix}"

            # Calculate expiration timestamp
            expiration = datetime.datetime.utcnow() + datetime.timedelta(days=self.config.snapshot_expiration_days)
            expiration_timestamp = expiration.strftime('%Y-%m-%d %H:%M:%S UTC')

            snapshot_query = f"""
                CREATE SNAPSHOT TABLE `{self.config.project_id}.{self.config.target_dataset_id}.{snapshot_table}`
                CLONE `{self.config.project_id}.{self.config.target_dataset_id}.{copy_table}`
                OPTIONS (
                    expiration_timestamp = TIMESTAMP('{expiration_timestamp}')
                )
            """

            try:
                logger.info(f"Creating snapshot: {snapshot_table}")
                query_job = self.client.query(snapshot_query)
                query_job.result()  # Wait for completion
                logger.info(
                    f"Successfully created snapshot: {snapshot_table} (expires in {self.config.snapshot_expiration_days} days)")

            except GoogleAPIError as e:
                logger.error(f"Failed to create snapshot for {source_table}: {e}")
                raise

    def cleanup_copy_tables(self) -> None:
        """
        Drop intermediate copy tables after snapshots are created.
        """
        for copy_table in self.copy_tables:
            drop_query = f"""
                DROP TABLE IF EXISTS `{self.config.project_id}.{self.config.target_dataset_id}.{copy_table}`
            """

            try:
                logger.info(f"Dropping copy table: {copy_table}")
                query_job = self.client.query(drop_query)
                query_job.result()  # Wait for completion
                logger.info(f"Successfully dropped: {copy_table}")

            except GoogleAPIError as e:
                logger.error(f"Failed to drop copy table {copy_table}: {e}")
                # Don't raise here - continue cleanup for other tables
                continue

    def verify_snapshot_exists(self, snapshot_table: str) -> bool:
        """
        Verify that a snapshot table exists.

        Args:
            snapshot_table: Full table name with dataset

        Returns:
            True if table exists, False otherwise
        """
        try:
            table_ref = f"{self.config.project_id}.{self.config.target_dataset_id}.{snapshot_table}"
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False
        except GoogleAPIError as e:
            logger.error(f"Failed to verify snapshot {snapshot_table}: {e}")
            return False

    def run_archival(self) -> Dict[str, any]:
        """
        Execute the complete archival process.

        Returns:
            Dictionary with execution summary
        """
        start_time = datetime.datetime.utcnow()
        logger.info("=" * 60)
        logger.info("Starting BigQuery data archival process")
        logger.info("=" * 60)

        results = {
            "status": "success",
            "snapshots_created": [],
            "errors": [],
            "start_time": start_time.isoformat()
        }

        try:
            # Step 1: Calculate quarter dates
            logger.info("Step 1: Calculating quarter dates")
            self.get_quarter_dates()

            # Step 2: Load source tables
            logger.info("Step 2: Loading source tables")
            self.load_source_tables()

            if not self.source_tables:
                logger.warning("No source tables to archive")
                results["status"] = "no_tables"
                return results

            # Step 3: Create copy tables
            logger.info("Step 3: Creating copy tables")
            self.create_copy_tables()

            # Step 4: Copy partition data
            logger.info("Step 4: Copying data to copy tables")
            self.copy_partition_data()

            # Step 5: Create snapshots
            logger.info("Step 5: Creating snapshots")
            self.create_snapshots()

            # Step 6: Cleanup copy tables
            logger.info("Step 6: Cleaning up copy tables")
            self.cleanup_copy_tables()

            # Record successful snapshots
            date_suffix = f"{self.qstart_date.strftime('%Y%m%d')}_{self.qend_date.strftime('%Y%m%d')}"
            for source_table in self.source_tables:
                snapshot_table = f"{source_table}{self.config.snapshot_table_suffix}_{date_suffix}"
                if self.verify_snapshot_exists(snapshot_table):
                    results["snapshots_created"].append({
                        "source_table": source_table,
                        "snapshot_table": snapshot_table,
                        "quarter_start": self.qstart_date.isoformat(),
                        "quarter_end": self.qend_date.isoformat()
                    })

            end_time = datetime.datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"Successfully archived {len(results['snapshots_created'])} tables")
            logger.info(f"Total execution time: {duration:.2f} seconds")

        except Exception as e:
            logger.error(f"Archival process failed: {e}")
            results["status"] = "failed"
            results["errors"].append(str(e))
            raise

        finally:
            results["end_time"] = datetime.datetime.utcnow().isoformat()
            logger.info("=" * 60)
            logger.info("Data archival process completed")
            logger.info("=" * 60)

        return results


def main():
    """Main entry point for the script"""
    try:
        # Load configuration
        config = Config.from_env()

        # Validate target dataset exists
        client = bigquery.Client(project=config.project_id)
        dataset_ref = f"{config.project_id}.{config.target_dataset_id}"

        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            logger.warning(f"Target dataset {dataset_ref} does not exist. Creating it...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set appropriate location
            client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset: {dataset_ref}")

        # Run archival
        archiver = BigQueryArchiver(config)
        results = archiver.run_archival()

        # Log summary
        logger.info("\nArchival Summary:")
        logger.info(f"Status: {results['status']}")
        logger.info(f"Snapshots created: {len(results['snapshots_created'])}")

        if results.get('errors'):
            logger.error(f"Errors encountered: {len(results['errors'])}")
            for error in results['errors']:
                logger.error(f"  - {error}")

        # Return exit code based on success
        sys.exit(0 if results['status'] == 'success' else 1)

    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()