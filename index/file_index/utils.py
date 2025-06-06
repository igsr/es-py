from typing import Any


def create_the_dictionary_structure() -> dict[str, Any]:
    """Create the dictionary structure

    Returns:
        dict[str, Any]: Returns a dictionary
    """
    file = {
        "dataReusePolicy": None,
        "analysisGroup": None,
        "dataCollections": [],
        "url": None,
        "md5": None,
        "populations": [],
        "dataType": None,
        "samples": [],
    }

    return file
