import logging
import os

import pandas as pd
import time
from beartype import beartype

from .local_config_builder import LocalFileConfig
from GlobalBaseClasses import Connection


class LocalConnection(Connection):
    """
    Microsoft SQL connection class for establishing connection
    """
    connection_type = "local"

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
        # take the file_dir_path given in the task, else take the default value
        self.config.file_dir_path = options.get('file_dir_path', self.config.file_dir_path)

        logging.info(f"Inside the Local query execution method")
        assert os.path.isdir(self.config.file_dir_path),\
            f"File directory is not present : {self.config.file_dir_path}"
        # if it is a file, then just read that file and proceed with it
        if os.path.isfile(os.path.join(self.config.file_dir_path, self.config.file_name)):
            files_to_read.append(os.path.join(self.config.file_dir_path, self.config.file_name))
        # read all the files from the given directory. as of now let's do csv only
        else:
            logging.info(f"Reading multiple files as the given file path does not exist")
            for file in os.listdir(self.config.file_dir_path):
                if file.endswith(".csv"):
                    files_to_read.append(os.path.join(self.config.file_dir_path, file))
        logging.info(f"Going to read the files : {files_to_read}")
        result = pd.concat(map(pd.read_csv, files_to_read), ignore_index=True)
        logging.info(f"Successfully read the files, count : {len(result)} and this is the header : {result.columns}")
        return result

    @beartype
    def send_data(self, data: pd.DataFrame, to_container=None, **options):
        """
        An abstract method that will be overwritten by the derived class
        """
        to_container = self.config.file_dir_path or to_container
        assert os.path.isdir(to_container), \
            f"File directory is not present : {to_container}"
        # this is the name of the file using which it will be saved
        file_path = os.path.join(to_container, self.config.file_name or 'data.csv')
        # check if file with the same name is already not present
        if os.path.isfile(file_path):
            file_path = file_path.replace(".csv", '') + "_" + str(time.time_ns()) + ".csv"
        logging.info(f"Saving the file with file path : {file_path}")

        data.to_csv(file_path, sep=self.config.sep or ",", index=False)