from mysql.connector import connect
from typing import Any


class FetchAGFromDB:
    """Fetch Analysis Group from DB class"""

    def __init__(self, data: dict[str, Any]):
        """Initialization of the class

        Args:
            data (dict[str, Any]): Configuration Data
        """
        self.data = data

    def fetch_information_from_DB(self) -> list[tuple]:
        """Fetch analysis group information from database

        Returns:
            list[tuple]: Rows of information from the database
        """

        fetch_ag_sql = """SELECT ag.* from file f INNER JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id INNER JOIN sample_file sf on sf.file_id = f.file_id GROUP BY ag.analysis_group_id"""

        host = self.data["host"]
        port = self.data["port"]
        database = self.data["database"]
        password = self.data["password"]
        user = self.data["user"]

        db = connect(
            host=host, port=port, database=database, password=password, user=user
        )
        cursor = db.cursor()
        cursor.execute(fetch_ag_sql)
        ag = cursor.fetchall()
        cursor.close()
        db.close()

        return ag

    def build_ag_info(self, row: tuple) -> dict[str, Any]:
        """Build analysis group information

        Args:
            row (tuple): Row, containing the information from the database

        Returns:
            dict[str, Any]: Updated dictionary containing the analysis group information
        """

        analysis_group = create_the_dictionary_structure()

        analysis_group.update(
            {
                "code": row[1],
                "description": row[2],
                "shortTitle": row[3],
                "displayOrder": row[4],
                "longDescription": row[5],
            }
        )

        return analysis_group


def create_the_dictionary_structure() -> dict[str, Any]:
    """Creates the dictionary structure for the analysis group

    Returns:
        dict[str, Any]: An initialized dictionary
    """

    analysis_group = {
        "code": None,
        "description": None,
        "shortTitle": None,
        "displayOrder": None,
        "longDescription": None,
    }

    return analysis_group
