import logging
import sqlalchemy

from ..utils import connection_generator, create_uri
from beartype import beartype

from .mysql_config_builder import MySQLConfig
from ..sql_connection_builder import SQLConnection


class MySQLConnection(SQLConnection):
    """
    Microsoft SQL connection class for establishing connection
    """
    connection_type = ""

    @beartype
    def __init__(self, config: MySQLConfig, connection_name, **options):
        super().__init__(config, connection_name, **options)
        MySQLConnection.connection_type = MySQLConfig.connection_type
        self.set_connection()

    def set_connection(self):
        """
        Set the connection to the microsoft server based on the inputs
        :return:
        """
        config = self.config

        logging.info(f"Connecting to MySQL Server")

        if not config.connection_str:
            config.connection_str = create_uri(
                drivername="mysql+pymysql",
                config=config,
                **self.options
            )

        self.engine, self.conn \
            = connection_generator(config.connection_str, **self.options)
