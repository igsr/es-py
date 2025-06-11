import pytest
from unittest.mock import MagicMock, patch
from index.super_population_index.fetch_information_from_db import FetchSPFromDB
from pytest_mock import MockerFixture

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
    return FetchSPFromDB(db_config)


def test_fetch_information_from_db(mocker: MockerFixture, fetcher:FetchSPFromDB):
    """_summary_

    Args:
        mocker (MockerFixture): _description_
        fetcher (FetchSPFromDB): _description_
    """    
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("TEST", "Test ancestry", None, None)
    ]

    mock_db = mock_cursor
    mock_db.cursor.return_value = mock_cursor
    mocker.patch(
        "index.super_population_index.fetch_information_from_db.connect",
        return_value=mock_db
    )
    result = fetcher.fetch_information_from_db()
    assert result[0][0] == "TEST"
    assert isinstance(result, list)


def test_build_superpopulation_info(mocker: MockerFixture, fetcher: FetchSPFromDB):
    row = ("TEST", "Test ancestry", None, None)

    result = fetcher.build_superpopulation_info(row)

    assert result["elasticId"] == "TEST"
    assert result["name"] == "Test ancestry"
