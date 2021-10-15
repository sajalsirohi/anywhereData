import logging
import urllib

from beartype import beartype

from ..utils import connection_generator
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
                if config.username and config.password:
                    config.connection_str = f"mssql+pyodbc://" \
                                            f"f{config.username}:{config.password}" \
                                            f"@{config.host}"
                # else it will be considered as 'trusted_connection=yes' and username and
                # password are not required in that case.
                else:
                    config.connection_str = f"mssql+pyodbc://" \
                                            f"@{config.host}"

            # create an engine object using the connection string
            self.engine, self.conn, self.curs \
                = connection_generator(config.connection_str, **self.options)