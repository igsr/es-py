from mysql.connector import connect
from data_collection_index.utils import create_the_dictionary_structure
from config_read import read_from_config_file


class DCDetailsFetcher:
    """_summary_
    """    

    
    def __init__(self, db_config: dict):
        """_summary_

        Args:
            db_config (dict): _description_
        """
        self.host = db_config["host"]
        self.port = db_config["port"]
        self.user = db_config["user"]
        self.password = db_config["password"]
        self.database = db_config["database"]


    def fetch_datacollections(self) -> list[tuple]:
        """_summary_

        Returns:
            list[tuple]: _description_
        """

        select_all_dc_sql = (
            "SELECT * from data_collection"
        )

        db = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            database=self.database,
            password=self.password,
        )

        cursor = db.cursor()
        cursor.execute(select_all_dc_sql)
        data_collection = cursor.fetchall()
        cursor.close()

        return data_collection


    def fetch_samples_count(self, dc_id: int) -> int:
        """_summary_

        Args:
            dc_id (int): _description_

        Returns:
            int: _description_
        """

        select_samples_count = """ SELECT count(samples.sample_id) AS num_samples
                        FROM (
                        SELECT DISTINCT sf.sample_id
                        FROM sample_file sf, file_data_collection fdc
                        WHERE sf.file_id = fdc.file_id AND fdc.data_collection_id = %s
                    ) AS samples """

        db = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            database=self.database,
            password=self.password,
        )
        cursor = db.cursor()
        cursor.execute(select_samples_count, (dc_id,))
        samples_count = cursor.fetchone()[0]
        cursor.close()
        db.close()

        return samples_count


    def fetch_population_count(self, dc_id: int) -> int:
        """_summary_

        Args:
            dc_id (int): _description_

        Returns:
            int: _description_
        """

        select_population_count = """ SELECT count(*) AS num_populations
                                FROM (
                            SELECT DISTINCT dcsp.population_id
                            FROM sample_file sf,
                                file_data_collection fdc,
                                sample s,
                                dc_sample_pop_assign dcsp
                            WHERE dcsp.data_collection_id = fdc.data_collection_id
                            AND dcsp.sample_id = s.sample_id
                            AND s.sample_id = sf.sample_id
                            AND sf.file_id = fdc.file_id
                            AND fdc.data_collection_id = %s
                        ) AS populations """

        db = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            database=self.database,
            password=self.password,
        )
        cursor = db.cursor()
        cursor.execute(select_population_count, (dc_id,))
        population_count = cursor.fetchone()[0]
        cursor.close()
        db.close()

        return population_count


    def fetch_publication_info(self, dc_id: int) -> list[tuple]:
        """_summary_

        Args:
            dc_id (int): _description_

        Returns:
            list[tuple]: _description_
        """

        publication_info_sql = """Select * from publications where data_collection_id=%s and publication is NOT NULL"""

        db = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            database=self.database,
            password=self.password,
        )

        cursor = db.cursor()
        cursor.execute(publication_info_sql, (dc_id,))
        publication_info = cursor.fetchall()
        cursor.close()
        db.close()

        return publication_info


    def fetch_analysis_information(self, dc_id) -> list[tuple]:
        analysis_info_sql = """SELECT dt.code data_type, ag.description analysis_group
            FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
            LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
            INNER JOIN file_data_collection fdc ON f.file_id=fdc.file_id
            WHERE fdc.data_collection_id= %s
            GROUP BY dt.data_type_id, ag.analysis_group_id """
        
        db = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            database=self.database,
            password=self.password,
        )

        cursor = db.cursor()
        cursor.execute(analysis_info_sql, (dc_id,))
        analysis_info = cursor.fetchall()
        cursor.close()
        db.close()

        return analysis_info

    
    def populate_the_dictionary_structure(self, row):
        dc_data = create_the_dictionary_structure()
        dc_data.update(
            {
                "code": row[1],
                "title": row[2],
                "shortTitle": row[3],
                "dataReusePolicy": row[5],
                "website": row[7],
                "samples": {
                    "count": self.fetch_samples_count(row[0])
                },
                "populations" : {
                    "count": self.fetch_population_count(row[0])
                }
            }
        )

        publications = self.fetch_publication_info(row[0])
        for pub in publications:
            dc_data["publications"].append(
                {"displayOrder": pub[2], "name": pub[3], "url": pub[1]}
            )

        analysis_info = self.fetch_analysis_information(row[0])
        for category, data in analysis_info:
            if category not in dc_data:
                dc_data[category] = []
            if data not in dc_data[category]:
                dc_data[category].append(data)

            if "dataTypes" not in dc_data:
                dc_data["dataTypes"] = []

            if category not in dc_data["dataTypes"]:
                dc_data["dataTypes"].append(category)


        return dc_data

