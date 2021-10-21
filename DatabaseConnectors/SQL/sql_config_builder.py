import logging

from package_utils.package_utils import read_file
from GlobalBaseClasses import Config


class SQLConfig(Config):
    """
    A class to contain all the details of a SQL connection detail
    Reads and parses the config file location passed to the class.
    """
    connection_type = "sql"

    def __init__(self, config, **options):
        self.config          = config    # connections.yaml
        self.port            = ""
        self.host            = ""
        self.password        = ""
        self.username        = ""
        self.db_name         = ""
        self.connection_str  = ""
        self.connect_through = ""
        logging.info("Now calling the config parsing function")
        self.parse_config()

    def parse_config(self):
        """
        Parse the config file and get the details
        :return:
        """
        logging.info(f"Parsing the config : {self.config}")
        self.host            = self.config.get('host')
        self.password        = self.config.get('password')
        self.username        = self.config.get('username')
        self.db_name         = self.config.get('db_name')
        self.port            = self.config.get('port')
        self.connection_str  = self.config.get('connection_str', '')

        if self.password == '':
            if 'password_path' in self.config:
                self.password = read_file(self.config.get('password_path'))
            else:
                self.password = ''
            logging.info(f"Password successfully fetched, will be connecting using the credentials")

    def __str__(self):
        return f"""
        HOST NAME   : {self.host}
        PASSWORD    : {len(self.password) * "*"}
        USERNAME    : {self.username}
        DB NAME     : {self.db_name}
        PORT        : {self.port}
        """