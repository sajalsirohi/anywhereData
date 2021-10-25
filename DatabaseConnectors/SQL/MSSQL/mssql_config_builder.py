from GlobalBaseClasses import Config

__all__ = ["MSSQLConfig"]


class MSSQLConfig(Config):
    __doc__ = """
    Config Builder for microsoft SQL server.
    :param: config: JSON like document containing the details to establish a connection  
    """
    connection_type = "mssql"

    def __init__(self, config, **options):
        # calling the super class to initialize the attributes
        super().__init__(config, **options)
        self.port            = self.port or 1433
        # jdbc driver name or odbc driver name
        self.driver_location = self.config.get('driver_location', '')
        self.connect_through = self.config.get('connect_through', 'odbc')
        self.driver          = self.config.get('driver', 'ODBC Driver 17 for SQL Server')