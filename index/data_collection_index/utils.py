from collections import defaultdict
from typing import Any


def create_the_dictionary_structure() -> dict[str, Any]:
    """Creating the dataCollections dictionary structure

    Returns:
        dict[str, Any]: The initialized dictionary
    """    
    datacollections_info = defaultdict(
        lambda: {
            "code": None,
            "title": None,
            "shortTitle": None,
            "dataReusePolicy": None,
            "website": None,
            "displayOrder": None,
            "samples": {"count": 0},
            "populations": {"count" : 0},
        }
    )

    datacollections_info["publications"] = []

    return datacollections_info
