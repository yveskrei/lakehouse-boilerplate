import logging
import os

# Constants
APP_PROD = 'prod'
APP_NP = 'np'
ENV_VARIABLES = [
    'ICEBERG_NAME',
    'ICEBERG_JDBC_URL',
    'ICEBERG_JDBC_USER',
    'ICEBERG_JDBC_PASSWORD',
    'ICEBERG_S3_PATH',
    'ICEBERG_S3_ENDPOINT',
    'ICEBERG_S3_ACCESS_KEY',
    'ICEBERG_S3_SECRET_KEY'
]
APP_VARIABLES = {
    'APP_LOCAL': True,
    'APP_ENV': APP_NP
}

# Variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

# Set up logging configuration
logging_handlers = [logging.StreamHandler()]
if APP_VARIABLES['APP_LOCAL']:
    logging_handlers.append(logging.FileHandler('app.log'))
    
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s][%(levelname)s][%(module)s] %(message)s',
    handlers=logging_handlers
)

def get_airflow_vars():
    """
    Get Airflow variables from environment variables.
    This function is used to retrieve the Airflow variables for the database connection.
    """
    from airflow.models import Variable
    
    variables = {}
    for var in ENV_VARIABLES:
        try:
            var_value = Variable.get(var, None)

            if var_value is None:
                raise Exception(f"Variable '{var}' not found.")
            else:
                variables[var] = var_value
        except Exception as e:
            logger.error(e)
            raise Exception(f"Error retrieving Airflow variable '{var}': {e}")
    
    return variables
    
def get_local_vars(ENV: str):
    """
    Get variables from a local .env file.
    This function is used to retrieve variables for the database connection from a .env file.
    """
    # Load environment variables from .env file
    from dotenv import load_dotenv
    load_dotenv(f"{BASE_DIR}/secrets/{ENV.lower()}.env")

    variables = {}
    for var in ENV_VARIABLES:
        try:
            var_value = os.getenv(var, None)

            if var_value is None:
                raise Exception(f"Variable '{var}' not found in .env file.")
            else:
                variables[var] = var_value
        except Exception as e:
            logger.error(e)
            raise Exception(f"Error retrieving local variable '{var}': {e}")
    
    return variables

def get_spark_jars():
    """
    Get the list of Spark JAR files required for Iceberg and JDBC.
    This function is used to locate the JAR files required for the Spark session.
    """
    jar_dir = f"{BASE_DIR}/spark_jars/"
    jar_files = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")]
    jars_string = ",".join(jar_files)

    return jars_string

def get_config():
    """
    Get configuration variables based on the environment.
    This function is used to retrieve the configuration variables
    """
    variables = {}

    # Append application variables
    variables.update(APP_VARIABLES)

    # Append environment variables
    if variables['APP_LOCAL']:
        variables.update(get_local_vars(variables['APP_ENV']))
    else:
        variables.update(get_airflow_vars())

    logger.info(f"Started application on {'local machine' if variables['APP_LOCAL'] else 'Apache Airflow'}, {variables['APP_ENV'].upper()} environment. Loaded {len(variables)} variables.")
    
    return variables

