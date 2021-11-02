import logging
import os

import json
import pandas as pd
from beartype import beartype

from .local_config_builder import LocalFileConfig
from GlobalBaseClasses import Connection


class LocalConnection(Connection):
    """
    Microsoft SQL connection class for establishing connection
    """
    connection_type = "mongodb"

    @beartype
    def __init__(self, config: LocalFileConfig, connection_name, **options):
        self.config  = config
        self.options = options
        logging.info(f"Acknowledging of the File connection name : {connection_name}")

    def set_connection(self):
        """
        Set the connection to the microsoft server based on the inputs
        :return:
        """
        pass

    def get_data(self, query=None, **options) -> pd.DataFrame:
        """
        query: Will be in form for json
        """
        files_to_read = []
        logging.info(f"Inside the Local query execution method")
        assert os.path.isdir(self.config.file_dir_path),\
            f"File directory is not present : {self.config.file_dir_path}"
        # if it is a file, then just read that file and proceed with it
        if os.path.isfile(os.path.join(self.config.file_dir_path, self.config.file_name)):
            files_to_read.append(os.path.join(self.config.file_dir_path, self.config.file_name))
        # read all the files from the given directory. as of now let's do csv only
        else:
            for file in os.listdir(self.config.file_dir_path):
                files_to_read.append(os.path.join(self.config.file_dir_path, file))
        logging.info(f"Going to read the files : {files_to_read}")
        result = pd.concat(map(pd.read_csv, files_to_read), ignore_index=True)
        logging.info(f"Successfully read the files, this is the header : {result.columns}")
        return result

    def send_data(self, data, to_container, **options):
        """
        An abstract method that will be overwritten by the derived class
        """
        logging.info('Converting data into json format')
        json_data = json.loads(data.to_json(orient='records'))
        logging.info(f'Creating the container object for container : {to_container}')
        collection = self.database_obj[to_container]
        logging.info("Now inserting the data")
        collection.insert_many(json_data)