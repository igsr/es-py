import mysql.connector


class PopulationDetailsFetcher:
    def __init__(self, db_config: dict):
        self.db_config = db_config

    
    def fetch_population(self) -> list[tuple]:
        """
            _summary_

            Args:
                self.db_config (dict): _description_

            Returns:
                list[tuple]: _description_
        """
        population_sql = """SELECT p.code, p.name, p.description, p.latitude, p.longitude, p.elastic_id, p.display_order,
                            COUNT(DISTINCT sample_id) AS num_samples, sp.code, sp.name, sp.display_colour, 
                            sp.display_order, p.population_id from population p, superpopulation sp, dc_sample_pop_assign dcsp 
                            where p.superpopulation_id = sp.superpopulation_id 
                            and p.population_id=dcsp.population_id group by p.population_id"""

        db = mysql.connector.connect(
            host=self.db_config["host"],
            port=self.db_config["port"],
            user=self.db_config["user"],
            database=self.db_config["database"],
            password=self.db_config["password"],
        )
        cursor = db.cursor()
        cursor.execute(population_sql)
        population = cursor.fetchall()
        cursor.close()
        db.close()

        return population

    def data_collection_details_population(self,
        pop_id: int,
    ) -> list[tuple]:
        """
        Fetches all the data collection information of a population based on the population_id
        Basically any collection where the population can be found

        Args:
            id (int): Population id from the database
            data(dict) : Data for the configuration info

        Returns:
            list[tuple]: List of tuples from the cursor.fetchall()
        """

        files_sql = """ SELECT dt.code, ag.description, dc.title, dc.data_collection_id, dc.reuse_policy FROM population p, dc_sample_pop_assign dspa, 
                        sample_file sf, file f, analysis_group ag, data_type dt, file_data_collection fdc, data_collection dc WHERE p.population_id=%s 
                        and p.population_id=dspa.population_id and dspa.sample_id=sf.sample_id and sf.file_id=f.file_id and f.analysis_group_id=ag.analysis_group_id 
                        and f.data_type_id=dt.data_type_id and f.file_id=fdc.file_id and fdc.data_collection_id=dc.data_collection_id and dspa.data_collection_id=dc.data_collection_id 
                        GROUP BY dt.data_type_id, ag.analysis_group_id, dc.data_collection_id"""

        db = mysql.connector.connect(
            host=self.db_config["host"],
            port=self.db_config["port"],
            user=self.db_config["user"],
            database=self.db_config["database"],
            password=self.db_config["password"],
        )

        cursor = db.cursor()
        cursor.execute(files_sql, (pop_id,))
        file_info = cursor.fetchall()
        cursor.close()
        db.close()

        return file_info

    def add_data_collection_details(self,
        pop_info: dict[str, any], pop_id: int
    ) -> dict[str, any]:
        """
        Adds the data collection details to the build pop_info dictionary

        Args:
            pop_info (dict[str, any]): The population info dictionary we are building
            pop_id (int): pop id which is used to fetch the data collection information for that population
            data (dict[str, any]): Configuration dictionary which is used in data_collection_details_population

        Returns:
            dict: The added information of the data collection details to the population info dictionary we are building
        """
        for dc in self.data_collection_details_population(pop_id):
            if dc[1] not in pop_info["dataCollections"][dc[0]]:
                pop_info["dataCollections"][dc[0]].append(dc[1])
            if dc[0] not in pop_info["dataCollections"]["dataTypes"]:
                pop_info["dataCollections"]["dataTypes"].append(dc[0])
            pop_info["dataCollections"]["dataReusePolicy"] = dc[4]
            pop_info["dataCollections"]["title"] = dc[2]

        return pop_info

    def build_population_info(self,
        population_info: dict[str, any], row: tuple
    ) -> dict[str, any]:
        """
        Builds the population information dictionary

        Args:
            population_info (dict[str, any]): population info dictionary
            row (tuple): the tuple containing the information we will be using to build
            data (dict[str, any]): Configuration dictionary

        Returns:
            dict[str, any]: Returns the populated information dictionary
        """
        population_info.update(
            {
                "code": row[0],
                "name": row[1],
                "description": row[2],
                "latitude": float(row[3]),
                "longitude": float(row[4]),
                "elasticId": row[5],
                "display_order": row[6],
                "samples": row[7],
                "superpopulation": {
                    "code": row[8],
                    "name": row[9],
                    "display_color": row[10],
                    "display_order": row[11],
                },
            }
        )

        # Add data collections details
        self.add_data_collection_details(population_info, row[12])

        # Add overlapping population details
        self.add_overlap_population_info(population_info, row[12])

        return population_info

    def add_overlap_population_info(self,
        population_info: dict[str, any], pop_id: int
    ) -> dict[str, any]:
        """
        Adds the overlapping population details

        Args:
            population_info (dict[str, any]): The population information dictionary
            pop_id (int): population id
            data (dict[str, any]): configuration dictionary required by

        Returns:
            dict[str, any]: Population info dictionary with added overlapping population info
        """

        for pop in self.select_overlap_population_details(pop_id):
            population_info["overlappingPopulations"]["populationElasticId"] = pop[
                "populationElasticId"
            ]
            if (
                pop["sharedSampleName"]
                not in population_info["overlappingPopulations"]["sharedSamples"]
            ):
                population_info["overlappingPopulations"]["sharedSamples"].append(
                    pop["sharedSampleName"]
                )
            population_info["overlappingPopulations"]["populationDescription"] = pop[
                "populationDescription"
            ]
            # to append the population sample
            population_info["overlappingPopulations"]["sharedSampleCount"] = len(
                population_info["overlappingPopulations"]["sharedSamples"]
            )

        return population_info

    def select_overlap_population_details(self,
        pop_id: int
    ) -> list[tuple]:
        """
        Select the overlap population details from the sql

        Args:
            pop_id (int): Population id
            data (dict[str, any]): Configuration dictionary

        Returns:
            list[tuple]: Returns from the fetchrow
        """

        query = """
        SELECT 
            p.elastic_id AS populationElasticId,
            p.description AS populationDescription,
            s.name AS sharedSampleName
        FROM
            dc_sample_pop_assign dspa1
            JOIN dc_sample_pop_assign dspa2 ON dspa1.sample_id = dspa2.sample_id
            JOIN population p ON dspa2.population_id = p.population_id
            JOIN sample s ON dspa1.sample_id = s.sample_id
        WHERE
            dspa1.population_id = %s
            AND dspa2.population_id != %s
        ORDER BY
            p.description, s.name;
        """

        db = mysql.connector.connect(
            host=self.db_config["host"],
            port=self.db_config["port"],
            user=self.db_config["user"],
            database=self.db_config["database"],
            password=self.db_config["password"],
        )
        cursor = db.cursor(dictionary=True)
        cursor.execute(query, (pop_id, pop_id))
        results = cursor.fetchall()
        cursor.close()
        db.close()

        return results
