from datetime import datetime, timedelta
import logging
import inspect
import argparse

# Variables
logger = logging.getLogger(__name__)

def get_intervals(fetch_start: datetime, fetch_end: datetime, batch_interval: timedelta = timedelta(days=1)):
    """
        Generate intervals between two dates with a specified interval.
    """
    current_start = fetch_start

    # Calculate intervals
    while current_start < fetch_end:
        current_end = min(current_start + batch_interval, fetch_end)
        yield (current_start, current_end)
        current_start = current_end

def load_in_batch(fetch_start: datetime, fetch_end: datetime, load_data: callable, batch_interval: timedelta = timedelta(hours=4), **kwargs):
    """
        Load data in batches of the specified interval.
    """
    
    intervals = get_intervals(fetch_start, fetch_end, batch_interval)
    error_count = 0

    for start, end in intervals:
        while True:
            try:
                load_data(
                    start_date=start, 
                    end_date=end,
                    **kwargs
                )
                break
            except Exception as e:
                error_count += 1

                if error_count % 5 == 0:
                    logger.error(e)
                    logger.error(f"Error loading data from {start.strftime('%Y-%m-%d %H:%M:%S')} to {end.strftime('%Y-%m-%d %H:%M:%S')}, trying again...")

def is_user_defined_method(cls: type, name: str):
    """
        Check if a method belongs to a class and is a user-defined method.
    """
    if not hasattr(cls, name):
        return False
    
    attr = getattr(cls, name)
    return inspect.isfunction(attr) and attr.__qualname__.startswith(cls.__name__ + ".")

def load_etl_task(cls: type):
    try:
        # Parse arguments
        parser = argparse.ArgumentParser(description="Load ETL task")
        parser.add_argument("--method", type=str, default=None, help="ETL method to execute")
        parser.add_argument("--fetch_start", type=str, default=None, help="Fetch start date in format YYYY-MM-DDTHH:MM:SS")
        parser.add_argument("--fetch_end", type=str, default=None, help="Fetch end date in format YYYY-MM-DDTHH:MM:SS")
        arguments = parser.parse_args()

        # Parse arguments
        etl_method = arguments.method
        fetch_start = datetime.strptime(arguments.fetch_start, "%Y-%m-%dT%H:%M:%S")
        fetch_end = datetime.strptime(arguments.fetch_end, "%Y-%m-%dT%H:%M:%S")

        # Initialize ETL class
        ETL = cls()

        if not is_user_defined_method(cls, etl_method):
            raise Exception(f"Method {etl_method} not found in ETL class {cls.__name__}")
        else:
            # Call the ETL method
            getattr(ETL, etl_method)(fetch_start, fetch_end)
    except Exception as e:
        logger.error(e)
        raise Exception(f"Error starting ETL process")
