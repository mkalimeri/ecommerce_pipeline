import logging

from environs import Env
import time

from dask.distributed import Client, LocalCluster
from elasticsearch import Elasticsearch
from influxdb_client import InfluxDBClient

from helpers import get_environment_variables, parse_arguments
from aggregations import import_data, prepare_dataframe, get_hourly_sales_per_brand_and_category
from batch_views import write_to_database, query_database
from visualization import from_query_to_document, insert_data_to_elastic


def main():
    # Get environment variables
    env = Env()
    aws_storage_options, influxdb_creds, elastic_creds = get_environment_variables(env)

    # Parse input arguments
    input_args = parse_arguments()
    filepath = input_args.s3_path
    run_full_pipeline = input_args.run_full_pipeline
    index_name = input_args.index_name

    if run_full_pipeline:
        # Batch processing tier
        logging.info('Reading data')

        start = time.time()

        # Create local dask cluster
        dask_cluster = LocalCluster()
        dask_client = Client(dask_cluster)
        logging.info(f'Started local dask cluster: {dask_cluster.scheduler}')

        # Import the data
        ddf = import_data(filepath, storage_options=aws_storage_options)
        ddf = prepare_dataframe(ddf)

        # Perform aggregations
        df = get_hourly_sales_per_brand_and_category(ddf)
        df = df.set_index('timestamp_hour')

        end = time.time()
        print(end-start)
        # with InfluxDBClient(url="http://localhost:8086", token=influxdb_creds['token'], org=influxdb_creds['org']) as client:
        #     print('creating bucket')
        #     create_db_bucket(client, influxdb_creds)

        start = time.time()

        # Batch views storage tier
        # Write to database
        with InfluxDBClient(
                url="http://localhost:8086",
                token=influxdb_creds['token'],
                org=influxdb_creds['org']
        ) as db_client:
            logging.info('writing to database')
            write_to_database(
                influxdb_client=db_client,
                db_creds=influxdb_creds,
                record=df,
                measurement='price',
                tags=['brand', 'category_code']
            )

        end = time.time()
        print(end-start)

    # Query database

    start = time.time()

    with InfluxDBClient(
            url="http://localhost:8086",
            token=influxdb_creds['token'],
            org=influxdb_creds['org']
    ) as client:
        logging.info('querying the database')
        query = f'from(bucket: "ecommerce_data") |> range(start: -3y)'
        query_res = query_database(client, query, influxdb_creds)

    end = time.time()
    print(end-start)
    print(f'First entry of queried data: \n {query_res[0]}')

    # Visualization Tier
    start = time.time()

    logging.info('Importing data to Elasticsearch')
    docs = from_query_to_document(query_res, index_name)
    print(f'First Elasticsearch document: {docs[0]}')
    end = time.time()
    print(end-start)

    start = time.time()
    es_client = Elasticsearch(
        cloud_id=elastic_creds['cloud_id'],
        basic_auth=(elastic_creds['user'], elastic_creds['password'])
    )

    insert_data_to_elastic(es_client, docs, elastic_creds["index_name"])
    end = time.time()
    print(end-start)
    logging.info('Application ended')

if __name__ == '__main__':
    main()
