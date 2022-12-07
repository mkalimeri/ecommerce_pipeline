import logging
from typing import Dict, List

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

from helpers import get_environment_variables
from environs import Env


def from_query_to_document(query_res: List, index_name: str) -> List:
    """
    Make the query results in the appropriate format to be imported in Elasticsearch index.
    We assume that all documents are insterted to the same index
    :param query_res: List: The results of the query, as outputed from function batch_views.query_database
    :param index_name: str: The name of the index the documents will be added
    :return: List of dictionaries, containing document information in
    format appropriate to be inserted into Elasticsearch
    """
    try:
        logging.info(f'Converting query results to Elasticsearch document format')
        docs = []  #TODO: use multiprocessing
        for i in range(len(query_res)):
            docs.append({
                '_index': index_name,
                'price': query_res[i]["_value"],
                'brand': query_res[i]["brand"],
                'category_id': query_res[i]["category_id"],
                'category_code': query_res[i]["category_code"],
                '@timestamp': query_res[i]["_time"]
            })
        return docs
    except Exception as e:
        logging.exception(f"Could not convert query results to Elasticsearch document format: {e}")
        return None


def insert_data_to_elastic(es_client: Elasticsearch, documents: List, index_name: str):
    """
    Insert documents to elasticsearch index. We assume that all documents are insterted to the same index
    :param es_client: The elasticsearch client
    :param documents: List: List of dictionaries, with document information as it should be inserted in the index.
        This is the output of function from_query_to_document
    :param index_name: The name of the index the documents should be interted
    """
    logging.info(f'Insert documents into elasticsearch index {index_name}')
    for success, info in parallel_bulk(client=es_client, actions=documents, index=index_name):
        if not success:
            logging.info(info)
