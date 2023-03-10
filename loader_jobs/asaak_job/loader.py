import os
import json
import logging
from datetime import datetime

from sqlalchemy import MetaData, Table, text as sa_text, delete

from loader_jobs.utils.postgres import PgConnector
from loader_jobs.utils.walk_files import *


def read_and_load_asaak_raw_data(full_refresh: bool = False, n_days: int = 3):
    """Main function to read and load data from asaak directory
    Uses  a conf.json file and a mapping.json file.

    Args:
        full_refresh (bool, optional): Whether or not to fully refresh the table. Defaults to False.
        n_days (int, optional): Number of days to fetch if full_refresh is False. Defaults to 3.
    """

    # Log info
    mode = "full_refresh" if full_refresh else f"incremental ({n_days} days)"
    logging.warn(f"Running asaak_job with mode {mode}")

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

            # File counter
            cpt = 0
            total = len(file_paths)

            # For each file
            for fp in file_paths:

                # Log every 50 files processed
                cpt += 1
                if cpt % 50 == 0:
                    logging.warn(f"File {cpt}/{total}")

                # Calculate the acquisition date using the file path
                acquisition_timestamp = get_acquisition_date_from_file_path(
                    data_path, fp
                )

                # Read the raw data
                with open(fp) as f:
                    raw = json.load(f)

                # For each table except payments_installments relation table,
                # if they are in the file
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

                        # Transform splits into single value (installments/payments)
                        if table_name == "installments":
                            record["payment_id"] = (
                                record["payment_id"][0]
                                if record["payment_id"]
                                else None
                            )

                        # Add the acquisition date to the Dict
                        record["acquisition_timestamp"] = acquisition_timestamp

                        # Add the insert timestamp
                        record["insert_timestamp"] = datetime.now()

                        # Add the record to the list
                        record_list.append(record)

                    # Insert the record list
                    stmt = tables[table_name].insert().values(record_list)
                    conn.execute(stmt)

    logging.warn("Asaak job done")
