import logging

from package_utils import read_file
from GlobalBaseClasses import Config


class SQLConfig(Config):
    """
    A class to contain all the details of a SQL connection detail
    Reads and parses the config file location passed to the class.
    """
    connection_type = "sql"

    def __init__(self, config, **options):
        self.config          = config    # connections.yaml
        self.host            = ""
        self.password        = ""
        self.username        = ""
        self.db_name         = ""
        self.parse_config()

    def parse_config(self):
        """
        Parse the config file and get the details
        :return:
        """
        self.host            = self.config.get('host')
        self.password        = self.config.get('password', 'password_key_not_present')
        self.username        = self.config.get('username')
        self.db_name         = self.config.get('db_name')

        if self.password == 'password_key_not_present':
            if 'password_path' in self.config:
                self.password = read_file(self.config.get('password_path'))
            else:
                self.password = None
            logging.info(f"Password successfully fetched, will be connecting using the credentials")

    def __str__(self):
        return f"""
        HOST NAME   : {self.host}
        PASSWORD    : {len(self.password) * "*"}
        USERNAME    : {self.username}
        DB NAME     : {self.db_name}
        """