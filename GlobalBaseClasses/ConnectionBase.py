import logging
from abc import ABC, abstractmethod

import pandas as pd
from beartype import beartype

from package_utils import read_file


class Config:
    __doc__ = """
    Base class for config, which is passed onto to connection factory, to establish a 
    connection.
    """
    # connection type info which is passed into every class that implements this class
    connection_type = None

    def __init__(self, config, **options):
        logging.info(f"Parsing the config : {config}")
        self.config           = config  # connections.yaml
        self.options          = options
        self.host             = self.config.get('host')
        self.password         = self.config.get('password')
        self.username         = self.config.get('username')
        self.db_name          = self.config.get('db_name')
        self.port             = self.config.get('port')
        self.connection_str   = self.config.get('connection_str')

        if self.password == '':
            if 'password_path' in self.config:
                self.password = read_file(self.config.get('password_path'))
            else:
                self.password = ''
            logging.info(f"Password successfully fetched, will be connecting using the credentials")

    def __str__(self):
        return f"""
        HOST NAME   : {self.host}
        PASSWORD    : {len(self.password or '') * "*"}
        USERNAME    : {self.username}
        DB NAME     : {self.db_name}
        PORT        : {self.port}
        """


class FileStorageConfig(Config):
    __doc__ = """
    Base class for File config, as it will have different configs requirement,
    which is passed onto to connection factory, to establish a 
    connection.
    """
    # connection type info which is passed into every class that implements this class
    connection_type = None

    def __init__(self, config, **options):
        super().__init__(config, **options)
        self.file_name      = config.get('file_name', '')
        self.file_dir_path  = config.get('file_dir_path', '')
        self.sep            = config.get('sep', ',')

        # name of the azure blob container or the s3 bucket name
        self.s3_bucket = self.container = config.get('container', config.get('s3_bucket'))

    def __str__(self):
        return f"File Name      : {self.file_name}" \
               f"File Dir Path  : {self.file_dir_path}" \
               f"Container Name : {self.s3_bucket}"


class Connection(ABC):
    __doc__ = """
    A connection type abstract class. 
    """
    connection_name = None

    @abstractmethod
    def get_data(self, query, **options):
        pass

    @abstractmethod
    @beartype
    def send_data(self, data: pd.DataFrame, to_container, **options):
        pass

    @abstractmethod
    def set_connection(self):
        pass