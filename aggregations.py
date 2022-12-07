from datetime import datetime
import logging
from typing import Dict

import pandas as pd

import dask.dataframe as dd


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


def import_data(s3_path: str, storage_options: Dict)-> dd:
    '''
    Reads parquet files from filepath
    :param s3_path: string: The path to the S3 bucket the files are located in
    :param storage_options: dictionary: The AWS credentials needed to connect to the bucket
    :return: Dask dataframe containing the data
    '''
    try:
        logging.info(f'Reading historical data')
        return dd.read_parquet(s3_path, storage_options=storage_options)
    except Exception as e:
        logging.exception(f'Could not import master data files: {e}')
        return None


def prepare_dataframe(ddf: dd) -> dd:
    '''
    Add timestamp_hour column to the dataframe, to prepare it for hourly aggregations
    :param ddf: dd: Dask dataframe to be processed
    :return: dd: Dask dataframe with added columns
    '''
    ddf = ddf.reset_index()
    ddf['timestamp_hour'] = ddf['event_time'].apply(
        get_preferred_timestamp,
        current_format='%Y-%m-%d %H:%M:%S %Z',
        preferred_format='%Y-%m-%d %H',
        meta=pd.Series(dtype=object,
                       name='timestamp_hour'
                       )
    )
    return ddf


def get_hourly_sales_per_brand_and_category(ddf: dd) -> pd.DataFrame:
    '''
    Group the values of the dataframe per hour, brand and category. Sum the price to get the total revenue
    per hour per brand/category.
    :param ddf: The dataframe as processed by prepare_dataframe, must contain column timestamp_hour
    :return: Pandas dataframe containing the aggregations
    '''
    try:
        logging.info('Aggregating price')
        ddf = ddf.groupby(['timestamp_hour', 'brand', 'category_code', 'category_id']).price.sum().compute()
        ddf = ddf.reset_index()
        return ddf
    except Exception as e:
        logging.exception(f'Could not aggregate price: {e}')
        return None