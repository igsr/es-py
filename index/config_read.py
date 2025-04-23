from configparser import ConfigParser


def read_from_config_file(config_file: str) -> dict[str, any]:
    """Reads from config file using ConfigParser

    Args:
        config_file (str): the configuration file

    Returns:
        dict[str, any]: Data of configuration
    """
    data = {}
    config = ConfigParser()
    config.read(config_file)
    data["host"] = config["database"]["host"]
    data["port"] = config["database"]["port"]
    data["user"] = config["database"]["user"]
    data["database"] = config["database"]["name"]
    data["password"] = config["database"]["password"]

    return data
