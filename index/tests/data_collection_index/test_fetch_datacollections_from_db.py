import pytest
from unittest.mock import MagicMock, patch
from index.data_collection_index.fetch_information_from_db import DCDetailsFetcher
from typing import Any


@pytest.fixture
def db_config()-> dict[str, Any]:
    return {
        "host": "localhost",
        "port": 3306,
        "user": "user",
        "password": "pass",
        "database": "test_db",
    }


@pytest.fixture
def fetcher(db_config: dict[str, Any]) -> DCDetailsFetcher:
    return DCDetailsFetcher(db_config)

def test_fetch_datacollections(mocker, fetcher: DCDetailsFetcher):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(
        1,
        "test-dc",
        "Test datacollections",
        "test-c",
        1,
        None,
        None,
        None
    )
    ]

    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch("index.data_collection_index.fetch_information_from_db.connect", return_value=mock_db)


    result = fetcher.fetch_datacollections()
    assert result[0][1] == "test-dc"
    assert result[0][2] == "Test datacollections"


def test_fetch_publication_info(mocker, fetcher:DCDetailsFetcher):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (1, "http://testdc", 1, "test stuff and everything")
    ]

    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch("index.data_collection_index.fetch_information_from_db.connect", return_value=mock_db)

    result = fetcher.fetch_publication_info(1)
    assert result[0][1] == "http://testdc"
    assert result[0][3] == "test stuff and everything"


def test_fetch_analysis_information(mocker, fetcher:DCDetailsFetcher):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("sequence", "Exome"),
        ("alignment", "PCR-free high coverage"),
        ("variants", "test variant cells")
    ]

    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch("index.data_collection_index.fetch_information_from_db.connect", return_value=mock_db)

    result = fetcher.fetch_analysis_information(1)
    assert isinstance(result, list) 
    assert len(result) == 3
    assert result[0][0] == "sequence"
    assert result[1][0] == "alignment"
    assert result[2][1] == "test variant cells"