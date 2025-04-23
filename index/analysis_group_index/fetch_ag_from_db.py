from mysql.connector import connect
from typing import Any


class FetchAGFromDB:
    """_summary_"""

    def __init__(self, data: dict[str, Any]):
        """_summary_

        Args:
            data (dict[str, Any]): _description_
        """
        self.data = data

    def fetch_information_from_DB(self) -> tuple:
        """_summary_

        Returns:
            tuple: _description_
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
        """_summary_

        Args:
            row (tuple): _description_

        Returns:
            dict[str, Any]: _description_
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
    """_summary_

    Returns:
        dict[str, Any]: _description_
    """

    analysis_group = {
        "code": None,
        "description": None,
        "shortTitle": None,
        "displayOrder": None,
        "longDescription": None,
    }

    return analysis_group
