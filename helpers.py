import argparse
import logging
from typing import Dict, Tuple

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

        # aws_storage_options = {
        #     'key': env_var_dict["aws_access_key_id"],
        #     'secret': env_var_dict["aws_secret_access_key"]
        # }

        # influxdb_creds = {
        #     'token': env_var_dict["influxdb_token"],
        #     'org': env_var_dict["influxdb_org"],
        #     'bucket': env_var_dict["influxdb_bucket"]
        # }
        #
        # elastic_creds = {
        #     'user': env_var_dict["elastic_user"],
        #     'password': env_var_dict["elastic_pw"],
        #     'cloud_id': env_var_dict["elastic_cloudid"]
        # }

        # aws_storage_options["region"] = os.getenv("AWS_DEFAULT_REGION")
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


def parse_arguments():
    '''

    :return:
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--s3_path', '-s3', required=False, default="s3://csci88-sales-data/monthly/",
                        help="the S3 location of the raw data files")
    parser.add_argument('--run_full_pipeline', '-r', required=False, default=False,
                        help="should the raw data be read, aggregated and stored in the database?")
    parser.add_argument('--index_name', '-r', required=False, default='default_index',
                        help="The name of the Elasticsearch index where the data will be saved")
    return parser.parse_args()