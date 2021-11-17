import logging

import json
from beartype import beartype

from .mongo_config_builder import MongoConfig
from ..nosql_connection_builder import NoSQLConnection

from package_utils import spark
from pyspark.sql.dataframe import DataFrame


class MongoConnection(NoSQLConnection):
    """
    Microsoft SQL connection class for establishing connection
    """

    connection_type = "mongodb"

    @beartype
    def __init__(self, config: MongoConfig, connection_name, **options):
        super().__init__(config, connection_name, **options)
        logging.info(f"Connecting to the connection name : {connection_name}")
        self.config.db_name = config.db_name or "sajal_db"
        self.current_df: DataFrame

    def execute_nosql_query(self, query = None, **options) -> DataFrame:
        """
        query: Will be in form for json
        """
        logging.info(f"Fetching the data from mongodb")
        self.current_df = spark.read.format("mongo").load()
        logging.info(f"Fetch the data : {self.current_df.schema}")
        return self.current_df

    def persist_nosql(self, data: DataFrame, to_container = None, **options):
        """
        An abstract method that will be overwritten by the derived class
        """
        logging.info(f"Sending data to mongodb")
        data.write\
            .format("mongo")\
            .mode(options.get('mode' or "append"))\
            .options(**options.get('spark_write_options'))\
            .save()
        logging.info("Successfully sent it :D")

    def create_jdbc_url(self) -> str:
        pass