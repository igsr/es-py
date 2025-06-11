import pytest
from unittest.mock import MagicMock, patch
from index.data_collection_index.fetch_information_from_db import DCDetailsFetcher
from typing import Any
from pytest_mock import MockerFixture


@pytest.fixture
def db_config()-> dict[str, Any]:
    """_summary_

    Returns:
        dict[str, Any]: _description_
    """    
    return {
        "host": "localhost",
        "port": 3306,
        "user": "user",
        "password": "pass",
        "database": "test_db",
    }


@pytest.fixture
def fetcher(db_config: dict[str, Any]) -> DCDetailsFetcher:
    """_summary_

    Args:
        db_config (dict[str, Any]): _description_

    Returns:
        DCDetailsFetcher: _description_
    """    
    return DCDetailsFetcher(db_config)

def test_fetch_datacollections(mocker: MockerFixture, fetcher: DCDetailsFetcher):
    """_summary_

    Args:
        mocker (MockerFixture): _description_
        fetcher (DCDetailsFetcher): _description_
    """    
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


def test_fetch_publication_info(mocker: MockerFixture, fetcher:DCDetailsFetcher):
    """_summary_

    Args:
        mocker (MockerFixture): _description_
        fetcher (DCDetailsFetcher): _description_
    """    
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


def test_fetch_analysis_information(mocker: MockerFixture, fetcher:DCDetailsFetcher):
    """_summary_

    Args:
        mocker (MockerFixture): _description_
        fetcher (DCDetailsFetcher): _description_
    """    
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


def test_populate_the_dictionary_structure(mocker: MockerFixture, fetcher: DCDetailsFetcher):
    """_summary_

    Args:
        mocker (MockerFixture): _description_
        fetcher (DCDetailsFetcher): _description_
    """    
    row = (
        1,
        "test-dc",
        "Test datacollections",
        "test-c",
        1,
        None,
        None,
        None
    )
    mocker.patch.object(fetcher, "fetch_samples_count", return_value=2)
    mocker.patch.object(fetcher, "fetch_population_count", return_value=1)
    mocker.patch.object(fetcher, "fetch_publication_info", return_value=[
        (1, "http://pub1", 1, "Publication One"),
    ])
    mocker.patch.object(fetcher, "fetch_analysis_information", return_value=[
        ("WGS", "Variant Calling"),
    ])

    result = fetcher.populate_the_dictionary_structure(row)
    assert result["code"] == "test-dc"
    assert result["samples"]["count"] == 2
    assert result["populations"]["count"] == 1
    assert result["publications"][0]["url"] == "http://pub1"
