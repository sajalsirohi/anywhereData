import logging

from package_utils import read_file
from BaseClasses import Config


class SQLConfig(Config):
    """
    A class to contain all the details of a SQL connection detail
    Reads and parses the config file location passed to the class.
    """
    connection_type = "sql"

    def __init__(self, config):
        self.config          = config    # connections.yaml
        self.host_name       = ""
        self.password        = ""
        self.username        = ""
        self.db_name         = ""
        self.port            = 1433
        self.odbc_driver     = ""
        self.driver_location = ""
        self.connect_through = ""   # Can be jdbc also
        self.parse_config()

    def parse_config(self):
        """
        Parse the config file and get the details
        :return:
        """
        self.host_name       = self.config.get('host_name')
        self.password        = self.config.get('password', 'password_key_not_present')
        self.username        = self.config.get('username')
        self.port            = self.config.get('port', 1433)
        self.db_name         = self.config.get('db_name')
        self.driver_location = self.config.get('driver_location')
        self.connect_through = self.config.get('connect_through', 'odbc')
        self.odbc_driver     = self.config.get('odbc_driver', 'SQL Server')

        if self.password == 'password_key_not_present':
            if 'password_path' in self.config:
                self.password = read_file(self.config.get('password_path'))
            else:
                self.password = None
            logging.info(f"Password successfully fetched, will be connecting using the credentials")

    def __str__(self):
        return f"""
        HOST NAME   : {self.host_name}
        PASSWORD    : {len(self.password) * "*"}
        USERNAME    : {self.username}
        DB NAME     : {self.db_name}
        PORT        : {self.port}
        DRIVER_LOC  : {self.driver_location}
        ODBC_DRIVER : {self.odbc_driver}
        """