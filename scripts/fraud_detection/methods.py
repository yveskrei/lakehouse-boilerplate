import polars as pl
import logging
import os

# Custom modules
from utils.lake import Lake

# Variables
logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class FraudDetectionMethods():
    def __init__(self, config: dict):
        self.config = config

    @staticmethod
    def get_transactions(*args, **kwargs):
        def format_record(data):
            try:
                return {
                    "step": Lake.get_integer(data.get("step", None)),
                    "type": Lake.get_string(data.get("type", None)),
                    "amount": Lake.get_decimal(data.get("amount", None), 8),
                    "origin_id": Lake.get_string(data.get("origin_id", None)),
                    "origin_old_balance": Lake.get_decimal(data.get("origin_old_balance", None), 8),
                    "origin_new_balance": Lake.get_decimal(data.get("origin_new_balance", None), 8),
                    "destination_id": Lake.get_string(data.get("destination_id", None)),
                    "destination_old_balance": Lake.get_decimal(data.get("destination_old_balance", None), 8),
                    "destination_new_balance": Lake.get_decimal(data.get("destination_new_balance", None), 8),
                    "fraud": Lake.get_boolean(data.get("fraud", None))
                }
            except:
                return None

        # Read the CSV
        df = pl.read_csv(f"{BASE_DIR}/fraud_dataset.csv", has_header=True)

        # Select specific columns
        df_columns = df.columns
        df = df.select(df_columns[df_columns.index('step'):df_columns.index('isFraud') + 1])

        # Rename and select only required columns with correct order
        df = df.select([
            pl.col("step").cast(pl.Int32),
            pl.col("type").cast(pl.Utf8),
            pl.col("amount").cast(pl.Float64),
            pl.col("nameOrig").alias("origin_id").cast(pl.Utf8),
            pl.col("oldbalanceOrg").alias("origin_old_balance").cast(pl.Float64),
            pl.col("newbalanceOrig").alias("origin_new_balance").cast(pl.Float64),
            pl.col("nameDest").alias("destination_id").cast(pl.Utf8),
            pl.col("oldbalanceDest").alias("destination_old_balance").cast(pl.Float64),
            pl.col("newbalanceDest").alias("destination_new_balance").cast(pl.Float64),
            pl.col("isFraud").alias("fraud").cast(pl.Boolean)
        ])

        # Format records to fit the schema
        records = df.to_dicts()
        records = list(map(format_record, records))
        records = list(filter(lambda x: x is not None, records))

        logger.info(f"Loaded {len(records)} records from the dataset.")

        return records