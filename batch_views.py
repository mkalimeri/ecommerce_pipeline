import json
import logging
from typing import Dict, List

import pandas as pd
from influxdb_client import InfluxDBClient

# Implements functions to importing and exporting of data to/from InfluxDB


# def create_db_bucket(client, bucket_name, org):
#     try:
#         with client.buckets_api() as bucket_api:
#             my_bucket = client.domain.bucket.Bucket(name=bucket_name, retention_rules=[], org_id=org)
#             bucket_api.create_bucket(my_bucket)
#
#     except:
#         print('InfluxDB bucket could not be created')


def write_to_database(
        influxdb_client: InfluxDBClient,
        db_creds: Dict,
        record: pd.DataFrame,
        measurement: str,
        tags: list
):
    """
    Uses influxdb_client to import data to InfluxDB bucket
    :param influxdb_client: InfluxDBClient: the influxDB client
    :param db_creds: Dict: the dictionary containing the credentials to
    connect to the InfluxDB bucket (org name, bucket name and token)
    :param record: pd.Dataframe, dataframe containing the data to be insterted in the database
    :param measurement: the column that will be inserted in the database as the measured value
    :param tags: list of columns to be inserted in the database as tags. The remaining columns will be inserted as fields
    """
    try:
        logging.info(f'Inserting data to bucket {db_creds["bucket"]}')
        with influxdb_client.write_api() as writer:
            writer.write(
                bucket=db_creds['bucket'],
                org=db_creds['org'],
                record=record,
                data_frame_measurement_name=measurement,
                data_frame_tag_columns=tags
            )
    except Exception as e:
        logging.exception(f'Could not transfer record to database: {e}')


def query_database(influxdb_client: InfluxDBClient, query: str, db_creds: Dict) -> List:
    """
    Query the bucket
    :param influxdb_client: InfluxDBClient: The influxDB client
    :param query: str: the flux query to be run on the bucket data
    :param db_creds: Dict: the dictionary containing the credentials to
    connect to the InfluxDB bucket (org name, bucket name and token)
    :return: List of json containing the results of the query
    """
    try:
        logging.info(f"Querying the data")
        tables = influxdb_client.query_api().query(query, org=db_creds['org'])
        # Serialize to JSON
        return json.loads(tables.to_json())
    except Exception as e:
        logging.exception(f"Could not run the query on the data: {e}")
