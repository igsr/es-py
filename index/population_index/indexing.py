import click
from index.elasticsearch_indexer import ElasticSearchIndexer
from index.population_index.fetch_information_from_db import PopulationDetailsFetcher
from index.config_read import read_from_config_file
from index.population_index.utils import create_the_dictionary_structure


class PopulationIndexer:
    def __init__(self, config_file: str, es_host: str, type_of: str):
        """Initialization of the class

        Args:
            config_file (str): Configuration file 
            es_host (str): ElasticSearch Host
            type_of (str): Type of 
        """        
        self.config_file = config_file
        self.es_host = es_host
        self.type_of = type_of
        self.data = read_from_config_file(config_file)
        self.fetcher = PopulationDetailsFetcher(self.data)
        self.indexer = ElasticSearchIndexer(es_host, "population")

    def build_and_index_population_info(self):
        """Build and index population info
        """        
        populations = create_the_dictionary_structure()
        pop_info = self.fetcher.fetch_population()
        actions = []

        for row in pop_info:
            code = row[0]
            if not code:
                continue
            population_data = self.fetcher.build_population_info(populations, row)
            action = self.indexer.index_data(population_data, code, self.type_of)
            actions.append(action)

        self.indexer.bulk_index(actions)


@click.command()
@click.option("--config_file", "-c", type=click.Path(exists=True), required=True)
@click.option("--es_host", "-es", type=str, required=True)
@click.option("--type_of", "-t", type=str, required=True)
def create_data(config_file: str, es_host: str, type_of: str):
    indexer = PopulationIndexer(config_file, es_host, type_of)
    result = indexer.build_and_index_population_info()


# Enables CLI use
if __name__ == "__main__":
    create_data()


# Enables programmatic use (from main.py)
def run(config_file, es_host, type_of):
    indexer = PopulationIndexer(config_file, es_host, type_of)
    result = indexer.build_and_index_population_info()
    print(result)
