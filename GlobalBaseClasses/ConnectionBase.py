import logging
from abc import ABC, abstractmethod

from beartype import beartype
from pyspark.sql.dataframe import DataFrame

from package_utils import read_file


class Config:
    __doc__ = """
    Base class for config, which is passed onto to connection factory, to establish a 
    connection.
    """
    # connection type info which is passed into every class that implements this class
    connection_type: str = None

    def __init__(self, config, **options) -> None:
        logging.info(f"Parsing the config : {config}")
        self.config           = config  # connections.yaml
        self.options          = options
        self.host             = self.config.get('host')
        self.password         = self.config.get('password')
        self.username         = self.config.get('username')
        self.db_name          = self.config.get('db_name', '')
        self.port             = self.config.get('port')
        self.connection_str   = self.config.get('connection_str')
        self.driver           = self.config.get('driver')
        self.properties       = self.config.get('properties', '')
        self.jdbc_url         = self.config.get('jdbc_url', '')

        if self.password == '':
            if 'password_path' in self.config:
                self.password = read_file(self.config.get('password_path'))
            else:
                self.password = ''
            logging.info(f"Password successfully fetched, will be connecting using the credentials")

    def __str__(self) -> str:
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

    def __init__(self, config, **options) -> None:
        super().__init__(config, **options)
        self.file_name      = config.get('file_name', '')
        self.file_dir_path  = config.get('file_dir_path', '')
        self.sep            = config.get('sep', ',')

        # name of the azure blob container or the s3 bucket name
        self.s3_bucket = self.container = config.get('container', config.get('s3_bucket'))

    def __str__(self) -> str:
        return f"File Name      : {self.file_name}" \
               f"File Dir Path  : {self.file_dir_path}" \
               f"Container Name : {self.s3_bucket}"


class Connection(ABC):
    __doc__ = """
    A connection type abstract class. 
    """
    connection_name = None

    @abstractmethod
    def get_data(self, query, **options) -> (DataFrame, list[dict]): ...

    @abstractmethod
    @beartype
    def send_data(self, data: DataFrame, to_container, **options) -> None: ...

    @abstractmethod
    def create_jdbc_url(self) -> str: ...

    def __repr__(self):
        return f"{self.__class__.__name__}(" \
               f"Host = {self.config.host}, " \
               f"User = {self.config.username}, " \
               f"Password = {self.config.password}, " \
               f"Port = {self.config.port}, " \
               f"JDBC = {self.jdbc_url}) "