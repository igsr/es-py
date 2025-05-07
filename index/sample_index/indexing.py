import click
import sys
from elasticsearch_indexer import ElasticSearchIndexer
from sample_index.fetch_samples_from_db import SampleDetailsFetcher
from config_read import read_from_config_file


class SampleIndexer:
    """SampleIndexer Class """

    def __init__(self, config_file: str, es_host: str, type_of: str):
        """Initializing of the Sample Indexer class

        Args:
            config_file (str): Configuration file
            es_host (str): Elasticsearch host
            type_of (str): Type of whether create or update
        """
        self.config_file = config_file
        self.es_host = es_host
        self.type_of = type_of
        self._data = None
        self._fetcher = None
        self._indexer = None

    @property
    def data(self):
        """Property function for data

        Returns:
            _type_: self.data
        """
        if self._data is None:
            self._data = read_from_config_file(self.config_file)

        return self._data

    @property
    def fetcher(self):
        """Property function of fetcher

        Returns:
            _type_: self.fetcher
        """
        if self._fetcher is None:
            self._fetcher = SampleDetailsFetcher(self.data)
        return self._fetcher

    @property
    def indexer(self):
        """Property function of indexer

        Returns:
            _type_: self.indexer
        """
        if self._indexer is None:
            self._indexer = ElasticSearchIndexer(self.es_host, "sample")
        return self._indexer

    def generate_actions(self):
        """Generate actions that will be used for bulk index

        Yields:
            _type_: A generator
        """
        samples_info = self.fetcher.fetch_samples()

        for row in samples_info:
            code = row[1]
            samples_data = self.fetcher.build_the_dictionary_structure(row)
            yield self.indexer.index_data(samples_data, code, self.type_of)

    def build_and_index_sample_info(self):
        """Build and index sample information

        Returns:
            _type_: Bulk actions
        """
        actions = self.generate_actions()
        return self.indexer.bulk_index(actions)


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
def create_data(config_file: str, es_host: str, type_of: str):
    """_summary_

    Args:
        config_file (str): _description_
        es_host (str): _description_
        type_of (str): _description_
    """
    sample_indexer = SampleIndexer(config_file, es_host, type_of)
    sample_indexer.build_and_index_sample_info()


if __name__ == "__main__":
    create_data()
