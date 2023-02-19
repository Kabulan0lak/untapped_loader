import os
import logging
from typing import Dict
from sqlalchemy import create_engine


class PgConnector:
    """Generates an SQLAlchemy engine based on environment variables"""

    def __init__(self, conf: Dict):
        try:
            self.host = conf["host"]
            self.port = conf["port"]
            self.database = conf["database"]
            self.user = conf["user"]
            self.password = conf["password"]
            self.uri = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        except:
            raise Exception("Invalid configuration")

    def __enter__(self):

        # Create the engine
        self.engine = create_engine(self.uri)

        logging.info(f"Connected to database: {self.uri}")

        # Return the connection
        return self.engine

    def __exit__(self, exception_type, exception_value, exception_traceback):

        # Dispose the engine
        self.engine.dispose()

        logging.info(f"Closed connection to database")
