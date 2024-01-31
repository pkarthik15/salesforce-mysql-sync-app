import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv


load_dotenv()


class Configurations:
    SALESFORCE_ENVIRONMENT = os.environ.get('SALESFORCE_ENVIRONMENT', "")
    SALESFORCE_USERNAME = os.environ.get('SALESFORCE_USERNAME', "")
    SALESFORCE_PASSWORD = os.environ.get('SALESFORCE_PASSWORD', "")
    SALESFORCE_CLIENT_ID = os.environ.get('SALESFORCE_CLIENT_ID', "")
    SALESFORCE_CLIENT_SECRET = os.environ.get('SALESFORCE_CLIENT_SECRET', "")
    MYSQL_USERNAME = os.environ.get('MYSQL_USERNAME', "")
    MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', "")
    MYSQL_SERVER = os.environ.get('MYSQL_SERVER', "")
    MYSQL_PORT = os.environ.get('MYSQL_PORT', "")
    MYSQL_DB = os.environ.get('MYSQL_DB', "")


def get_date_time_n_mins_ago_utc(n:int) -> str:
    current_time = datetime.now(timezone.utc)
    time_15mins_ago = current_time - timedelta(minutes = n)
    time_15mins_ago = time_15mins_ago.replace(second = 0, microsecond=0)
    time_15mins_ago_str = time_15mins_ago.isoformat().replace('+00:00', 'Z')
    return time_15mins_ago_str


def convert_time(x):
    dt = datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f%z")
    return dt