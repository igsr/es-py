import elasticsearch
import click
from elasticsearch import Elasticsearch, exceptions
from elasticsearch.helpers import bulk
from typing import Any, Dict, List


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

    def create_index(self, settings: dict[str, Any], mappings: dict[str, Any]) -> bool:
        """Creates an ElasticSearch Index with the settings and mappings

        Args:
            settings (dict[str, Any]): settings for the index
            mappings (dict[str, Any]): mappings for the index

        Returns:
            bool: True or False
        """
        if self.client.indices.exists(index=self.index_name):
            click.echo(f"Index '{self.index_name}' already exists.")
            return False
        try:
            self.client.indices.create(
                index=self.index_name, body={"settings": settings, "mappings": mappings}
            )
            click.echo(f"Index '{self.index_name}' created successfully.")
            return True
        except exceptions as e:
            click.echo(f"Failed to create index: {e}")
            return False

    def index_data(
        self, data: dict[str, Any], doc_id: str, action_type: str
    ) -> dict[str, Any]:
        """Creation of the index (required for the action for bulk action)

        Args:
            population_data (dict): Population information
            doc_id (str): doc_id information
            action_type (str): action type : create or update

        Returns:
            dict[str, Any]:  Bulk indexing action dict
        """
        return {
            "_op_type": action_type,
            "_index": self.index_name,
            "_id": doc_id,
            "doc": data,
        }

    def bulk_index(self, actions: List[Dict[str, Any]]):
        """
        Perform a bulk indexing operation.

        Args:
            actions (List[Dict[str, Any]]): List of actions to be indexed

        Raises:
            Exception: Raised on any bulk indexing failure
        """
        try:
            bulk(self.client, actions)
        except elasticsearch.BadRequestError as e:
            raise Exception(f"Error during bulk indexing: {str(e)}")
