import argparse
from datetime import datetime
import logging
from typing import Tuple

from environs import Env
from dotenv import load_dotenv
import os


def get_environment_variables(env: Env) -> Tuple:
    """
    Get the AWS/InfluxDB/Elastic credentials from the .env file
    :param env: os.environ, the environment the variables will be extracted from
    :return: Tuple, tuple of dictionary containing the credentials for AWS, InfluxDB and Elastic Cloud
    """
    try:
        logging.info(f"Loading environment variables")
        load_dotenv()

        aws_storage_options = {}
        influxdb_creds = {}
        elastic_creds = {}

        aws_storage_options["key"] = os.getenv("AWS_ACCESS_KEY_ID")
        aws_storage_options["secret"] = os.getenv("AWS_SECRET_ACCESS_KEY")

        influxdb_creds["token"] = os.getenv("INFLUXDB_TOKEN")
        influxdb_creds["org"] = os.getenv("INFLUXDB_ORG")
        influxdb_creds["bucket"] = os.getenv("INFLUXDB_BUCKET")

        elastic_creds["user"] = os.getenv("ELASTIC_USER")
        elastic_creds["password"] = os.getenv("ELASTIC_PASSWORD")
        elastic_creds["cloud_id"] = os.getenv("ELASTIC_CLOUD_ID")
        elastic_creds["index_name"] = os.getenv("ELASTIC_INDEX")

        return aws_storage_options, influxdb_creds, elastic_creds
    except Exception as e:
        logging.exception(f"Could not load environment variables: {e}")


def parse_arguments() -> argparse.Namespace:
    """
    Parse input arguments
    :return:  of input arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--s3_path', '-s3', required=False, default="s3://csci88-sales-data/monthly/",
                        help="the S3 location of the raw data files")
    parser.add_argument('--run_full_pipeline', '-fp', required=False, default=True,
                        help="should the raw data be read, aggregated and stored in the database?")
    parser.add_argument('--index_name', '-index_name', required=False, default='hourly_sales_index',
                        help="The name of the Elasticsearch index where the data will be saved")
    return parser.parse_args()


def get_preferred_timestamp(timestamp: str, current_format: str, preferred_format:str) -> str:
    """
    Returns a shorter version of given timestamp
    :param timestamp: string: The original timestamp (e.g. '2020-04-24 11:50:39')
    :param current_format: string: The current format of the timestamp (e.g. '%Y-%m-%d %H:%M:%S')
    :param preferred_format: string: The shortened format of the timestamp (eg. '%Y-%m-%d %H')
    :return: string: the timestamp in the desired format
    """
    try:
        ft = datetime.strptime(timestamp, current_format)
        return datetime.strftime(ft, preferred_format)
    except Exception as e:
        logging.exception(f'Could not convert timestamp: {e}')
        return None
