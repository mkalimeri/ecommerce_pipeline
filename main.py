import argparse
from environs import Env
import time

from dask.distributed import Client, LocalCluster
from elasticsearch import Elasticsearch
from influxdb_client import InfluxDBClient

from helpers import get_environment_variables
from aggregations import import_data, prepare_dataframe, get_hourly_sales_per_brand_and_category
from batch_views import write_to_database, query_database
from visualization import from_query_to_document, insert_data_to_elastic


def main():
    env = Env()
    aws_storage_options, influxdb_creds, elastic_creds = get_environment_variables(env)

    # input_args = parse_arguments()
    filepath = "s3://csci88-sales-data/monthly/" #input_args.s3_path
    run_full_pipeline = True #input_args.run_full_pipeline

    if run_full_pipeline:
        print('Reading data')
        start = time.time()

        # Create local dask cluster
        dask_cluster = LocalCluster()
        dask_client = Client(dask_cluster)
        print(dask_cluster.scheduler)

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

        # Write to database
        with InfluxDBClient(url="http://localhost:8086", token=influxdb_creds['token'], org=influxdb_creds['org']) as db_client:
            print('writing to database')
            write_to_database(
                influxdb_client=db_client,
                db_creds=influxdb_creds,
                record=df,
                measurement='price',
                tags=['brand', 'category_id', 'category_code']
            )

        end = time.time()
        print(end-start)
    # Query database

    start = time.time()

    with InfluxDBClient(url="http://localhost:8086", token=influxdb_creds['token'], org=influxdb_creds['org']) as client:
        print('quering the database')
        # b = f"{influxdb_creds['bucket']}"
        query = f'from(bucket: "ecommerce_data") |> range(start: -3y)'
        query_res = query_database(client, query, influxdb_creds)

    end = time.time()
    print(end-start)

    start = time.time()

    docs = from_query_to_document(query_res, elastic_creds["index_name"])

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


if __name__ == '__main__':
    main()
