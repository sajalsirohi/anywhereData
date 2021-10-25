import logging

import json
import pandas as pd
from beartype import beartype
from pandas.io.json._normalize import nested_to_record
from pymongo import MongoClient

from .mongo_config_builder import MongoConfig
from ..nosql_connection_builder import NoSQLConnection


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
        self.database_obj    = None
        self.collection_obj  = None
        self.conn            = None
        self.set_connection()

    def set_connection(self):
        """
        Set the connection to the microsoft server based on the inputs
        :return:
        """
        logging.info("Setting the MongoDB connection using set_connection() method")
        # check if direct connection URL is given, if not, create one sqlalchemy engine link
        if self.config.connection_str:
            # connect to the database if connection string is given, then directly use that
            self.conn = MongoClient(self.config.connection_str, connect=True)
        else:
            # establish the connection object.
            self.conn = MongoClient(
                host=self.config.host,
                username=self.config.username,
                password=self.config.password,
                port=self.config.port,
                connect=True
            )
        logging.info(f"Conn object is successfully created : {self.conn} now creating the "
                     f"database object of {self.config.db_name} and collection object {self.config.collection_name}")
        # creating the database object
        self.database_obj = self.conn[self.config.db_name]
        # extract the collection object
        self.collection_obj = self.database_obj[self.config.collection_name]

    def execute_nosql_query(self, query, **options) -> pd.DataFrame:
        """
        query: Will be in form for json
        """
        result = pd.DataFrame()
        logging.info(f"Inside the MongoDB query execution method")
        if options.get('source_collection_name'):
            self.collection_obj = self.database_obj[options['source_collection_name']]
        # now extract the data from the database
        logging.info(f"Fetching the data using the 'find' method")
        data = self.collection_obj.find(query, options.get('fields'))
        logging.info(f'Iterating over the data set now to convert it into a pandas dataframe from {self.collection_obj}')
        for datum in data:
            # flatten the dict. It will always be flattened before converting it into a pandas df
            datum   = nested_to_record(datum, sep='_')
            # convert it into a pandas df
            temp_df = pd.DataFrame([datum])
            # concat the pandas df
            result  = pd.concat([result, temp_df], axis=0, ignore_index=True)
        logging.info(f"Prepared the dataset with columns : {result.columns}")
        return result

    def persist_nosql(self, data, to_container, **options):
        """
        An abstract method that will be overwritten by the derived class
        """
        logging.info('Converting data into json format')
        json_data = json.loads(data.to_json(orient='records'))
        logging.info(f'Creating the container object for container : {to_container}')
        collection = self.database_obj[to_container]
        logging.info("Now inserting the data")
        collection.insert_many(json_data)