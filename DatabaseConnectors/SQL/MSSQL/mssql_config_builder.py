from ..sql_config_builder import SQLConfig
import logging

__all__ = ["MSSQLConfig"]


class MSSQLConfig(SQLConfig):
    __doc__ = """
    Config Builder for microsoft SQL server.
    :param: config: JSON like document containing the details to establish a connection  
    """
    connection_type = "mssql"

    def __init__(self, config, **options):
        # calling the super class to initialize the attributes
        super_ = super()
        super_.__init__(config, **options)
        super_.parse_config()

        self.connection_str  = ""
        self.port            = 1433
        # jdbc driver name or odbc driver name
        self.driver          = ""
        self.driver_location = ""
        self.connect_through = "odbc"   # Can be jdbc also

    def parse_config(self):
        """
        Parse the config file and get the details
        :return:
        """
        self.port            = self.config.get('port', 1433)
        self.driver_location = self.config.get('driver_location', '')
        self.connect_through = self.config.get('connect_through', 'odbc')
        self.driver          = self.config.get('driver', 'SQL Server')
        self.connection_str  = self.config.get('connection_str', '')