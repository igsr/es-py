from mysql.connector import connect

class FetchSPFromDB:
    def __init__(self, data: dict[str, any]):
        self.data = data

    def fetch_information_from_db(self):

        # not using code because code is sometimes null
        select_superpop_sql = """SELECT sp.elastic_id, sp.name, sp.display_colour, sp.display_order from superpopulation sp GROUP BY sp.superpopulation_id"""

        host = self.data["host"]
        port = self.data["port"]
        database = self.data["database"]
        password = self.data["password"]
        user = self.data["user"]

        db = connect(host=host, port=port, user=user, password=password, database=database)
        cursor = db.cursor()
        cursor.execute(select_superpop_sql)
        superpopulation = cursor.fetchall()
        db.close()
        cursor.close

        return superpopulation

    def build_superpopulation_info(self, row: tuple):

        superpopulation = create_the_dictionary_structure()
        superpopulation.update(
            {"elasticId" : row[0],
             "name": row[1],
             "display_colour": row[2],
             "display_order": row[3]
            }
        )

        return superpopulation


def create_the_dictionary_structure() -> dict[str, any]:
    superpopulation = {
        "elasticId" : None, 
        "name": None,
        "display_colour": None,
        "display_order": None
    }

    return superpopulation



