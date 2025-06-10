import pytest
from unittest.mock import MagicMock, patch
from index.sample_index.fetch_samples_from_db import SampleDetailsFetcher


@pytest.fixture
def db_config():
    return {
        "host": "localhost",
        "port": 3306,
        "user": "user",
        "password": "pass",
        "database": "test_db",
    }


@pytest.fixture
def fetcher(db_config):
    return SampleDetailsFetcher(db_config)


def test_fetch_samples(mocker, fetcher):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(1, "Sample1", "TEST123", "M")]
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch(
        "index.sample_index.fetch_samples_from_db.connect", return_value=mock_db
    )

    result = fetcher.fetch_samples()
    assert len(result) == 1
    assert result[0][2] == "TEST123"


def test_fetch_source_samples(mocker, fetcher):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, "test_source", "test source description", "http:/test_source_url")
    ]
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch(
        "index.sample_index.fetch_samples_from_db.connect", return_value=mock_db
    )

    result = fetcher.fetch_source_samples(1)
    assert len(result) == 1
    assert result[0][1] == "test_source"
    assert result[0][2] == "test source description"


def test_fetch_population_samples(mocker, fetcher):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (
            1,
            "GBR",
            "Great Britian",
            "Britain",
            "Britain, Scotland and Ireland",
            None,
            None,
            "BR",
            2,
            "SUPERGBR",
            "Super GBR",
            None,
            1,
        )
    ]

    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch(
        "index.sample_index.fetch_samples_from_db.connect", return_value=mock_db
    )

    result = fetcher.fetch_population_samples(1)
    assert result[0][1] == "GBR"
    assert result[0][6] == None


def test_fetch_dataCollections_samples(mocker, fetcher):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("test_igsr", "test pacbio sequence", "test igsr", 1, None)
    ]

    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch(
        "index.sample_index.fetch_samples_from_db.connect", return_value=mock_db
    )
    result = fetcher.fetch_dataCollections_samples(1)

    assert result[0][0] == "test_igsr"
    assert result[0][4] == None

