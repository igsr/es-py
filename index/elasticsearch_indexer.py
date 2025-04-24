import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticSearchIndexer:
    """Configuration for the ElasticSearch Index build"""

    def __init__(self, es_host: str, index_name: str):
        """Initialization of the ElasticSearchIndexer Class

        Args:
            es_host (str): Host of the ElasticSearch
            index_name (str): name of the index
        """
        self.client = Elasticsearch(es_host)
        self.index_name = index_name

    def index_data(self, data: dict, doc_id: str, action_type: str):
        """Creation of the index (required for the action for bulk action)

        Args:
            population_data (dict): Population information
            doc_id (str): doc_id information
            action_type (str): action type : create or update

        Returns:
            _type_: _description_
        """
        data_index = {
            "_op_type": action_type,
            "_index": self.index_name,
            "_id": doc_id,
            "doc": data,
        }
        return data_index

    def bulk_index(self, actions: list):
        """Bulk indexing

        Args:
            actions (list): list of dictionaries

        Raises:
            Exception: BadRequestError

        Returns:
            _type_: bulk action
        """
        try:
            bulk(self.client, actions)
        except elasticsearch.BadRequestError as e:
            raise Exception(f"Error during bulk indexing: {str(e)}")
