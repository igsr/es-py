import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticSearchIndexer:
    def __init__(self, es_host: str, index_name: str):
        """_summary_

        Args:
            es_host (str): _description_
            index_name (str): _description_
        """
        self.client = Elasticsearch(es_host)
        self.index_name = index_name

    def index_population(self, population_data: dict, doc_id: str, action_type: str):
        """
        _summary_

        Args:
            population_data (dict): _description_
            doc_id (str): _description_
            action_type (str): _description_

        Returns:
            _type_: _description_
        """
        data_index = {
            "_op_type": action_type,
            "_index": self.index_name,
            "_id": doc_id,
            "doc": population_data,
        }
        return data_index

    def bulk_index(self, actions: list):
        """
        _summary_

        Args:
            actions (list): _description_

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        try:
            return bulk(self.client, actions)
        except elasticsearch.BadRequestError as e:
            raise Exception(f"Error during bulk indexing: {str(e)}")
