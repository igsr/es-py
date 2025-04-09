import click
from population_index.elasticsearch_indexer import ElasticSearchIndexer
from population_index.fetch_information_from_db import PopulationDetailsFetcher
from config_read import  read_from_config_file
from population_index.utils import create_the_dictionary_structure

class PopulationIndexer:
    def __init__(self, config_file: str, es_host: str, type_of: str):
        self.config_file = config_file
        self.es_host = es_host
        self.type_of = type_of
        self.data = read_from_config_file(config_file)
        self.fetcher = PopulationDetailsFetcher(self.data)
        self.indexer = ElasticSearchIndexer(es_host, "population")

    def build_and_index_population_info(self):
        """Fetches population data and indexes it in Elasticsearch."""
        populations = create_the_dictionary_structure()
        pop_info = self.fetcher.fetch_population()
        actions = []

        for row in pop_info:
            code = row[0]
            if not code:
                continue
            # Add logic to build population info from row and populate the population structure
            population_data = self.fetcher.build_population_info(populations, row)
            action = self.indexer.index_population(population_data, code, self.type_of)
            actions.append(action)

        return self.indexer.bulk_index(actions)


@click.command()
@click.option("--config_file", "-c", type=click.Path(exists=True), help="Configuration file", required=True)
@click.option("--es_host", "-es", type=str, help="ElasticSearch host", required=True)
@click.option("--type_of", "-t", type=str, help="Update or create an index", required=True)
def create_data(config_file: str, es_host: str, type_of: str):
    """Command-line function to create data in Elasticsearch."""
    population_indexer = PopulationIndexer(config_file, es_host, type_of)
    result = population_indexer.build_and_index_population_info()
    print(result)


if __name__ == "__main__":
    create_data()

