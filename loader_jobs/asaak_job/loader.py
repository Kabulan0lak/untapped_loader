import os
import json
import logging
from datetime import datetime

from sqlalchemy import MetaData, Table

from loader_jobs.utils.postgres import PgConnector


def read_and_load_asaak_raw_data(only_last_day: bool = False):

    logging.info("Asaak job starting")

    # Read the configuration file
    with open("loader_jobs/conf.json") as f:
        config = json.load(f)

    # Assign parameters
    db_conf = config["db_conf"]
    data_path = config["asaak_path"]

    # Connect to Postgres using the conf file
    with PgConnector(db_conf) as pg:

        # List all files to process
        file_paths = []
        for root, _, files in os.walk(data_path):
            for file in files:
                file_path = os.path.join(root, file)
                file_paths.append(file_path)

        # Read mapping file
        with open("loader_jobs/asaak_job/mapping.json") as f:
            mapping = json.load(f)

        # Initialize metadata
        metadata = MetaData(schema="asaak")

        # Create an SQLAlchemy Table Object for each table
        tables = {}
        for table_name in mapping.keys():
            tables[table_name] = Table(
                table_name, metadata, autoload=True, autoload_with=pg
            )

        # For each file
        for fp in file_paths:

            # Calculate the acquisition date using the file path
            month, day = fp[fp.index(data_path) + len(data_path) + 1 :].split("/")[0:2]
            acquisition_timestamp = datetime.strptime(f"2022-{month}-{day}", "%Y-%m-%d")

            # Read the raw data
            with open(fp) as f:
                raw = json.load(f)

            # For each table, if they are in the file
            for table_name in [k for k in mapping.keys() if k in raw.keys()]:

                # Initialize a list of Dict to insert
                record_list = []

                # For each record
                for raw_line in raw[table_name]:

                    # Generate a Dict using the field mapping
                    record = {
                        k1: v2
                        for k1, v1 in mapping[table_name].items()
                        for k2, v2 in raw_line.items()
                        if v1 == k2
                    }

                    # Add the acquisition date to the Dict
                    record["acquisition_timestamp"] = acquisition_timestamp

                    # Add the record to the list
                    record_list.append(record)

                # Prepare the statement
                stmt = tables[table_name].insert().values(record_list)

                # Execute the statement
                with pg.connect() as conn:
                    conn.execute(stmt)
                    logging.info(f"Inserted {len(record_list)} rows into {table_name}")

    logging.info("Asaak job done")


def json_to_db(jsonFile):
    return
