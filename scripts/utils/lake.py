from pyspark.sql import SparkSession
import polars as pl
import logging
import json
import os
import json

# Custom modules
from utils.config import get_spark_jars

# Variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

class Lake:
    TARGET_BATCH_SIZE_BYTES = 15 * 1024 * 1024 # In MB's
    MAX_BATCH_SIZE = 200_000
    MIN_BATCH_SIZE = 1000

    def __init__(self, config: dict):
        # Initialize configuration
        self.config = config

        # Create Spark session
        self.session = None
        self.session = self.get_session()

    def __del__(self):
        # Stop Spark session
        if self.session is not None:
            try:
                self.session.stop()
            except Exception as e:
                logger.error(e)
                raise Exception(f"Error stopping Spark session")

    def get_session(self):
        # Initiate spark builder
        builder = SparkSession.builder.appName("DataLake-ETL")
        
        # Define spark as local
        if self.config['APP_LOCAL']:
            builder = builder \
            .master("local[*]") \
            .config("spark.jars", get_spark_jars())

        # Create Spark session
        session = builder \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.session.timeZone", "Asia/Jerusalem") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.sql.codegen.wholeStage", "false") \
            .config(f"spark.sql.catalog.{self.config['ICEBERG_NAME']}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{self.config['ICEBERG_NAME']}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
            .config(f"spark.sql.catalog.{self.config['ICEBERG_NAME']}.uri", self.config['ICEBERG_JDBC_URL']) \
            .config(f"spark.sql.catalog.{self.config['ICEBERG_NAME']}.jdbc.user", self.config['ICEBERG_JDBC_USER']) \
            .config(f"spark.sql.catalog.{self.config['ICEBERG_NAME']}.jdbc.password", self.config['ICEBERG_JDBC_PASSWORD']) \
            .config(f"spark.sql.catalog.{self.config['ICEBERG_NAME']}.warehouse", self.config['ICEBERG_S3_PATH']) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", self.config['ICEBERG_S3_ENDPOINT']) \
            .config("spark.hadoop.fs.s3a.access.key", self.config['ICEBERG_S3_ACCESS_KEY']) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config['ICEBERG_S3_SECRET_KEY']) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .getOrCreate()

        logger.info(f"Spark session created successfully. ID: {session.sparkContext.applicationId}, version: {session.version}")

        return session
    
    def get_batch_size(self, total_records: int, sample_record: dict):
        """
        Rougly calculating the size of the batch from a record.
        Assuming all records are roughly the same size.
        Calculating the amount of batches required to load the records into the table.
        """
        # Create a single-row DataFrame to measure size more accurately
        sample_df = self.session.createDataFrame([sample_record])
        
        # Get size estimate using DataFrame row string representation
        # This is more accurate than JSON for Spark's internal format
        record_size = len(str(sample_df.collect()[0]))
        records_per_batch = int(self.TARGET_BATCH_SIZE_BYTES / record_size)
        
        # Ensure reasonable batch sizes
        batch_size = max(self.MIN_BATCH_SIZE, min(records_per_batch, self.MAX_BATCH_SIZE))

        # Calcualte amount of total batches
        total_batches = (total_records + batch_size - 1) // batch_size
        
        return total_batches, batch_size, record_size

    def remove_duplicates(self, records: list[dict], unique_columns: list[str] = None):
        """
        Remove duplicates from records based on unique columns.
        """
        # Create a DataFrame from the records
        dataframe = pl.DataFrame(records)

        # Count initial number of records
        initial_count = len(dataframe)

        # Drop exact duplicates
        dataframe = dataframe.unique(keep="last")
        after_exact_duplicates = len(dataframe)

        # Log the number of exact duplicates removed
        exact_duplicates = initial_count - after_exact_duplicates
        if exact_duplicates > 0:
            logger.warning(f"Removed {exact_duplicates} exact duplicate records.")

        if unique_columns is not None:
            # Drop duplicates based on unique columns
            dataframe = dataframe.unique(subset=unique_columns, keep="last")
            after_column_duplicates = len(dataframe)

            # Log the number of duplicates removed based on unique columns
            columns_duplicates = after_column_duplicates - after_exact_duplicates
            if columns_duplicates > 0:
                logger.warning(f"Removed {columns_duplicates} duplicates based on unique columns.")

        # Convert back to list of dictionaries
        return dataframe.to_dicts()
    
    def merge_records(self, target_table: str, records: list[dict], merge_columns: list = None, set_columns: list = None):
        """
        Load bulk data onto a table using merge statement.
        Handles dropping duplicates and creating the sql query itself.

        target_table: str - The name of the target table in Iceberg.
        records: list - The records to be merged into the target table.
        merge_columns: list - Optional, The columns to be used for merging(those should be unique to only one record). If not passed, all columns will be used.
        set_columns: list - Optional, The columns to be updated in the target table. If not passed, records that matched will be ignored.
        """
        if not len(records):
            raise Exception('No records to merge')
        
        try:
            records_columns = records[0].keys()

            # Define merge columns if not provided
            # If not provided - We are essentially doing an insert statement, since all columns are unique
            if merge_columns is None:
                merge_columns = records_columns

            # Remove duplicated records
            records = self.remove_duplicates(records=records, unique_columns=merge_columns)

            # Create merge statement for query
            merge_statement = " AND ".join([f"target.{col} = source.{col}" for col in merge_columns])
            
            # Create set statement for query - if columns are provided
            # If not provided - We are only inserting new records
            set_statement = ""
            if set_columns:
                set_statement = ", ".join([f"{col} = source.{col}" for col in set_columns])

            # Additional strings for the query
            raw_columns = ','.join(records_columns)
            source_columns = ','.join([f"source.{col}" for col in records_columns])

            # Calculate rougly batch size for the records
            # We split the records into batches to avoid memory issues
            total_batches, batch_size, record_size = self.get_batch_size(
                total_records=len(records), 
                sample_record=records[0]
            )

            # Calcualte size of dataframe in MB's
            dataframe_size = len(records) * record_size / 1024 / 1024
            logger.info(f"Merging {len(records)} records into {target_table} table. Total size: {dataframe_size:.4f} MB's.")
            logger.info(f"Average record size: {record_size} bytes. Total batches: {total_batches}, Batch size: {batch_size}")

            for batch_num in range(total_batches):
                # Merge current batch of records
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(records))
                batch_records = records[start_idx:end_idx]

                # Create DataFrame from records
                dataframe = self.session.createDataFrame(batch_records)

                # Create temporary view for the DataFrame
                SOURCE_BATCH = f"records_source_{batch_num}"
                dataframe.createOrReplaceTempView(SOURCE_BATCH)
                    
                # Create the SQL query
                query = f"""
                    MERGE INTO {self.config['ICEBERG_NAME']}.{target_table} AS target
                    USING {SOURCE_BATCH} AS source
                    ON {merge_statement}
                    {f'WHEN MATCHED THEN UPDATE SET {set_statement}' if set_statement else ''}
                    WHEN NOT MATCHED THEN 
                        INSERT ({raw_columns})
                        VALUES ({source_columns})
                """

                logger.info(f"Batch {batch_num + 1}/{total_batches} merged into {target_table}")

                # Execute the SQL query
                self.session.sql(query)

                # Drop the temporary view
                self.session.catalog.dropTempView(SOURCE_BATCH)
            
            logger.info(f"Successfully merged all {len(records)} records into {target_table}")
        except Exception as e:
            logger.error(e)
            raise Exception(f"Cannot merge records into {target_table} table")
    
    def insert_records(self, target_table: str, records: list[dict]):
        """
        Insert records into an Iceberg table efficiently with deduplication.

        Parameters:
        target_table: str - The name of the target table in Iceberg.
        records: list - The records to be merged into the target table.
        """
        if not len(records):
            raise Exception("No records to insert")

        try:
            # Remove duplicated records
            records = self.remove_duplicates(records=records)
                                                 
            # Calculate rougly batch size for the records
            # We split the records into batches to avoid memory issues
            total_batches, batch_size, record_size = self.get_batch_size(
                total_records=len(records), 
                sample_record=records[0]
            )

            # Calcualte size of dataframe in MB's
            dataframe_size = len(records) * record_size / 1024 / 1024
            logger.info(f"Merging {len(records)} records into {target_table} table. Total size: {dataframe_size:.4f} MB's.")
            logger.info(f"Average record size: {record_size} bytes. Total batches: {total_batches}, Batch size: {batch_size}")

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(records))
                batch_records = records[start_idx:end_idx]

                # Create DataFrame
                dataframe = self.session.createDataFrame(batch_records)

                # Write to Iceberg table
                dataframe.writeTo(f"{self.config['ICEBERG_NAME']}.{target_table}").append()

                logger.info(f"Batch {batch_num + 1}/{total_batches} inserted into {target_table}")

            logger.info(f"Successfully inserted all {len(records)} records into {target_table}")
        except Exception as e:
            logger.error(e)
            raise Exception(f"Cannot insert records into {target_table} table")
    
    @staticmethod
    def get_decimal(value: str, precision: int):
        if value is not None:
            try:
                number = float(value)
                decimal = float(f"{number:.{precision}f}")
                return decimal
            except:
                raise Exception(f"Cannot convert {value} to decimal with precision {precision}")
        else:
            return None
    
    @staticmethod
    def get_integer(value: str):
        if value is not None:
            try:
                return int(value)
            except:
                raise Exception(f"Cannot convert {value} to integer")
        else:
            return None
    
    @staticmethod
    def get_string(value: str):
        if value is not None:
            return str(value).strip()
        else:
            return None
    
    @staticmethod
    def get_boolean(value: str):
        if value is not None:
            try:
                return bool(value)
            except:
                raise Exception(f"Cannot convert {value} to boolean")
        else:
            return None
    
    @staticmethod
    def get_json(value: str):
        if value is not None:
            try:
                return json.loads(value)
            except:
                raise Exception(f"Cannot convert {value} to json")
        else:
            return None