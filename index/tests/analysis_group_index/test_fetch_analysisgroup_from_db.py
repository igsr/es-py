import pytest
from unittest.mock import MagicMock, patch
from index.analysis_group_index.fetch_ag_from_db import FetchAGFromDB


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
    return FetchAGFromDB(db_config)


def test_fetch_information_from_db(mocker, fetcher):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(1, "test_exome", "Test exome", "test-exome", 2, None)]
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mocker.patch(
        "index.analysis_group_index.fetch_ag_from_db.connect", return_value=mock_db
    )

    result = fetcher.fetch_information_from_DB()
    assert result[0][1] == "test_exome"
    assert result[0][5] == None

def test_build_ag_info(fetcher):
    return_value = (1, "test_exome", "Test exome", "test-exome", 2, None)

    result = fetcher.build_ag_info(return_value)

    assert result["code"] == "test_exome"
    assert result["description"] == "Test exome"