from datetime import datetime
import logging

# Custom modules
from utils.lake import Lake
from fraud_detection.methods import FraudDetectionMethods
import utils.general as general
from utils.config import get_config

# Variables
logger = logging.getLogger(__name__)

class FraudDetectionETL():
    def __init__(self):
        # Initiate configuration
        self.config = get_config()

        # Initiate lake connection
        self.lake = Lake(self.config)

        # Initiate methods
        self.methods = FraudDetectionMethods(self.config)
    
    def load_transactions(self, fetch_start: datetime, fetch_end: datetime):
        """
            Load transactions from the source to the lake.
        """
        def load_data(start_date: datetime, end_date: datetime):
            records = self.methods.get_transactions(start_date=start_date, end_date=end_date)
            if len(records):
                # Insert records into the lake
                self.lake.insert_records(
                    target_table='monitor.transactions',
                    records=records
                )
        
        # Load data in batches
        general.load_in_batch(
            fetch_start=fetch_start,
            fetch_end=fetch_end,
            load_data=load_data
        )

# Define main function for spark job
if __name__ == "__main__":
    general.load_etl_task(FraudDetectionETL)