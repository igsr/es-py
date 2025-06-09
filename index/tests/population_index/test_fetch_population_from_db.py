import pytest
from unittest.mock import MagicMock
from index.population_index.fetch_information_from_db import PopulationDetailsFetcher
from typing import Any
from pytest_mock import MockerFixture


@pytest.fixture
def db_config() -> dict[str, Any]:
    """DB configuration

    Returns:
        dict: Dictionary containing the test db configuration
    """
    return {
        "host": "localhost",
        "port": 3306,
        "user": "user",
        "password": "pass",
        "database": "test_db",
    }


@pytest.fixture
def fetcher(db_config: dict[str, Any]) -> PopulationDetailsFetcher:
    """The fetcher fixture

    Args:
        db_config (dict): DB configuration

    Returns:
        PopulationDetailsFetcher: A class
    """
    return PopulationDetailsFetcher(db_config)


def test_fetch_population(mocker: MockerFixture, fetcher: PopulationDetailsFetcher):
    """Test the fetch population from the PopulationDetailsFetcher class

    Args:
        mocker (MockerFixture): Mocker from the Pytest_mock:MockerFixture
        fetcher (PopulationDetailsFetcher): PopulationDetailsFetcher class
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (
            "code1",
            "name1",
            "desc",
            1.0,
            2.0,
            "eid",
            1,
            5,
            "spcode",
            "spname",
            "#fff",
            2,
            1,
        )
    ]
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch("mysql.connector.connect", return_value=mock_db)

    result = fetcher.fetch_population()
    assert len(result) == 1
    assert result[0][0] == "code1"


def test_data_collection_details_population(
    mocker: MockerFixture, fetcher: PopulationDetailsFetcher
):
    """Test the fetch data collection population from the PopulationDetailsFetcher class

    Args:
        mocker (MockerFixture): Mocker from the Pytest_mock:MockerFixture
        fetcher (PopulationDetailsFetcher): PopulationDetailsFetcher class
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("type1", "group1", "title1", 123, "open")]
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch("mysql.connector.connect", return_value=mock_db)

    result = fetcher.data_collection_details_population(1)
    assert result[0][0] == "type1"


def test_add_data_collection_details(
    mocker: MockerFixture, fetcher: PopulationDetailsFetcher
):
    """Test to check add_data_collection_details from PopulationDetailsFetcher

    Args:
        mocker (MockerFixture): Mocker from the Pytest_mock:MockerFixture
        fetcher (PopulationDetailsFetcher): PopulationDetailsFetcher class
    """
    mocker.patch.object(
        fetcher,
        "data_collection_details_population",
        return_value=[("type1", "group1", "title1", 123, "open")],
    )
    pop_info = {"dataCollections": {"type1": [], "dataTypes": []}}
    result = fetcher.add_data_collection_details(pop_info, 1)
    assert result["dataCollections"]["title"] == "title1"
    assert "group1" in result["dataCollections"]["type1"]


def test_build_population_info(
    mocker: MockerFixture, fetcher: PopulationDetailsFetcher
):
    """Test for the PopulationDetailsFetcher build_population_info

    Args:
        mocker (MockerFixture): Mocker from the Pytest_mock:MockerFixture
        fetcher (PopulationDetailsFetcher): PopulationDetailsFetcher class
    """
    row = (
        "code",
        "name",
        "desc",
        1.1,
        2.2,
        "eid",
        0,
        10,
        "spcode",
        "spname",
        "#000",
        3,
        123,
    )
    mocker.patch.object(fetcher, "add_data_collection_details", return_value={})
    mocker.patch.object(fetcher, "add_overlap_population_info", return_value={})
    population_info = {
        "dataCollections": {"dataTypes": [], "type1": []},
        "overlappingPopulations": {"sharedSamples": []},
    }

    result = fetcher.build_population_info(population_info, row)
    assert result["name"] == "name"


def test_add_overlap_population_info(
    mocker: MockerFixture, fetcher: PopulationDetailsFetcher
):
    """Test for the PopulationDetailsFetcher add_overlap_population_info

    Args:
        mocker (MockerFixture): Mocker from the Pytest_mock:MockerFixture
        fetcher (PopulationDetailsFetcher): PopulationDetailsFetcher class
    """
    mocker.patch.object(
        fetcher,
        "select_overlap_population_details",
        return_value=[
            {
                "populationElasticId": "popE1",
                "populationDescription": "desc",
                "sharedSampleName": "sample1",
            }
        ],
    )
    pop_info = {"overlappingPopulations": {"sharedSamples": []}}

    result = fetcher.add_overlap_population_info(pop_info, 1)
    assert result["overlappingPopulations"]["sharedSampleCount"] == 1


def test_select_overlap_population_details(
    mocker: MockerFixture, fetcher: PopulationDetailsFetcher
):
    """Test for the PopulationDetailsFetcher select_overlap_population

    Args:
        mocker (MockerFixture): Mocker from the Pytest_mock:MockerFixture
        fetcher (PopulationDetailsFetcher): PopulationDetailsFetcher class
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        {
            "populationElasticId": "pop1",
            "populationDescription": "desc",
            "sharedSampleName": "sample",
        }
    ]
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch("mysql.connector.connect", return_value=mock_db)

    result = fetcher.select_overlap_population_details(1)
    assert isinstance(result, list)
    assert result[0]["populationElasticId"] == "pop1"
