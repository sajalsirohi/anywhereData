import logging
import urllib

from beartype import beartype

from ..utils import connection_generator, create_uri
from .mssql_config_builder import MSSQLConfig
from ..sql_connection_builder import SQLConnection


class MSSQLConnection(SQLConnection):
    """
    Microsoft SQL connection class for establishing connection
    """
    connection_type = ""

    @beartype
    def __init__(self, config: MSSQLConfig, connection_name, **options):
        super().__init__(config, connection_name, **options)
        logging.info(f"Connecting to the connection name : {connection_name}")
        self.config.db_name = config.db_name or "master"
        self.set_connection()
        MSSQLConnection.connection_type = MSSQLConfig.connection_type

    def set_connection(self):
        """
        Set the connection to the microsoft server based on the inputs
        :return:
        """
        config = self.config
        # check if direct connection URL is given, if not, create one sqlalchemy engine link
        if config.connect_through == 'jdbc':

            logging.info(f"As 'connect_through' is jdbc, using jaydebeapi module to connect to server")
            import jaydebeapi

            self.conn = jaydebeapi.connect(
                config.driver,
                f"jdbc:sqlserver://{config.host_name}:{config.port};database={config.db_name}",
                {'user': config.username, 'password': config.password},
                config.driver_location
            )
            self.curs = self.conn.cursor()

        elif config.connect_through == 'odbc':

            logging.info(f"As 'connect_through' is odbc, using sqlalchemy module to connect to server")

            if config.connection_str:
                config.connection_str = f"mssql+pyodbc:///?odbc_connect="\
                          f"{urllib.parse.quote_plus(config.connection_str)}"
            else:
                # if username and password is given, then use that.
                logging.info("Connecting using mssql+pyodbc, let's see if it works")
                config.connection_str = create_uri(
                    drivername="mssql+pyodbc",
                    config=config,
                    query={"driver": config.driver or "ODBC Driver 17 for SQL Server"},
                    **self.options
                )

            try:
                # create an engine object using the connection string
                self.engine, self.conn \
                    = connection_generator(config.connection_str, **self.options)
            except Exception as e:
                logging.info(f"Error occurred using pyodbc, now using pymssql, this is the error -> {e}")
                config.connection_str = create_uri(
                    drivername="mssql+pymssql",
                    config=config,
                    **self.options
                )

                # create the same object, but using different config now.
                self.engine, self.conn \
                    = connection_generator(config.connection_str, **self.options)