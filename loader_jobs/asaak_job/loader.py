import os
import json
import logging
from datetime import datetime

from sqlalchemy import MetaData, Table, text as sa_text, delete

from loader_jobs.utils.postgres import PgConnector
from loader_jobs.utils.walk_files import *


def read_and_load_asaak_raw_data(full_refresh: bool = True, n_days: int = 3):

    logging.warn("Asaak job starting")

    # Read the configuration file
    with open("loader_jobs/conf.json") as f:
        config = json.load(f)

    # Assign parameters
    db_conf = config["db_conf"]
    data_path = config["asaak_path"]

    # Connect to Postgres using the conf file
    with PgConnector(db_conf) as pg:

        # List all files to process
        if full_refresh:
            file_paths = get_full_list_of_filepaths(data_path)
        else:
            file_paths = get_top_n_files_of_last_month(data_path, n_days)

        # Read mapping file
        with open("loader_jobs/asaak_job/mapping.json") as f:
            mapping = json.load(f)

        # Initialize metadata
        metadata = MetaData(schema="asaak")

        # Connect the engine
        with pg.connect() as conn:

            # Create an SQLAlchemy Table Object for each table
            tables = {}
            for table_name in mapping.keys():
                tables[table_name] = Table(
                    table_name, metadata, autoload=True, autoload_with=pg
                )

                # If we full refresh, truncate the tables
                if full_refresh:
                    pg.execute(
                        sa_text(f"truncate table asaak.{table_name}").execution_options(
                            autocommit=True
                        )
                    )

                # Else, remove the last n_days of data
                else:
                    max_m, n_days = get_max_month_and_n_last_days(data_path)
                    stmt = delete(tables[table_name]).where(
                        tables[table_name].c.acquisition_timestamp.in_(
                            (datetime.strptime(f"2022-{max_m}-{day}", "%Y-%m-%d"))
                            for day in n_days
                        )
                    )
                    conn.execute(stmt)

            # File ounter
            cpt = 0

            # For each file
            for fp in file_paths:

                # Log every 50 files processed
                cpt += 1
                if cpt % 50 == 0:
                    logging.warn(f"File {cpt}/{len(file_paths)}")

                # Calculate the acquisition date using the file path
                acquisition_timestamp = get_acquisition_date_from_file_path(
                    data_path, fp
                )

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
                    conn.execute(stmt)

    logging.warn("Asaak job done")


def json_to_db(jsonFile):
    return
