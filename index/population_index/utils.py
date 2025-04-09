from collections import defaultdict


def create_the_dictionary_structure() -> dict[str, any]:
    """
    Creation of the dictionary structure for the population

    Returns:
        _type_: The created dictionary
    """
    population_info = defaultdict(
        lambda: {
            "code": None,
            "name": None,
            "description": None,
            "display_order": None,
            "elasticId": None,
            "latitude": None,
            "longitude": None,
            "superpopulation": {
                "code": None,
                "name": None,
                "display_color": None,
                "display_order": None,
            },
            "samples": None,
        }
    )
    # because we are not sure of what could be the keys in the dataCollections,
    # sometimes it is variant or/and sequence or/and alignment
    population_info["dataCollections"] = defaultdict(list)
    population_info["overlappingPopulations"] = {
        "populationElasticId": None,
        "populationDescription": None,
        "sharedSamples": [],
        "sharedSampleCount": 0,
    }

    return population_info
