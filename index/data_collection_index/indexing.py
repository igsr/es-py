import click 
from elasticsearch_indexer import ElasticSearchIndexer
from .fetch_information_from_db import DCDetailsFetcher
from config_read import read_from_config_file
from elasticsearch.helpers import BulkIndexError

class DataCollectionsIndexer:
    """DataCollectionsIndexer class
    """

    def __init__(self, config_file: str, es_host: str, type_of: str):
        """Initialization of the DataCollectionsIndexer

        Args:
            config_file (str): Configuration file
            es_host (str): ElasticSearch Host
            type_of (str): Type of: create or update
        """

        self.type_of = type_of
        self.data = read_from_config_file(config_file)
        self.fetcher = DCDetailsFetcher(self.data)
        self.indexer = ElasticSearchIndexer(es_host, "data_collections")


    def build_and_index_datacollections(self):
        """Build and index dataCollections
        """
        actions = []
        data_collection = self.fetcher.fetch_datacollections()
        for row in data_collection:
            code = row[1]
            dc_data = self.fetcher.populate_the_dictionary_structure(row)
            action = self.indexer.index_data(dc_data, code, self.type_of)
            actions.append(action)

        try:
          response = self.indexer.bulk_index(actions)
          print(f"Bulk indexing successful {response}")
        except BulkIndexError as e:
            print("Bulk indexing failed")
            for error in e.errors:
                print(error)
   

@click.command()
@click.option(
    "--config_file",
    "-c",
    type=click.Path(exists=True),
    help="Configuration file",
    required=True,
)
@click.option("--es_host", "-es", type=str, help="ElasticSearch host", required=True)
@click.option(
    "--type_of", "-t", type=str, help="Update or create an index", required=True
)
def create_data(config_file:str, es_host: str, type_of: str):
    dc_indexer = DataCollectionsIndexer(config_file, es_host, type_of)
    dc_indexer.build_and_index_datacollections()


if __name__ == "__main__":
    create_data()


# Enables programmatic use (from main.py)
def run(config_file, es_host, type_of):
    indexer = DataCollectionsIndexer(config_file, es_host, type_of)
    result = indexer.build_and_index_datacollections()