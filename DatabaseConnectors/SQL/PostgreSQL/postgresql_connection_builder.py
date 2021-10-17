import logging

from ..utils import connection_generator, create_uri
from beartype import beartype

from .postgresql_config_builder import PostgreSQLConfig
from ..sql_connection_builder import SQLConnection


class PostgreSQLConnection(SQLConnection):
    """
    Microsoft SQL connection class for establishing connection
    """
    connection_type = ""

    @beartype
    def __init__(self, config: PostgreSQLConfig, connection_name, **options):
        super().__init__(config, connection_name, **options)
        # copy the connection_type from the config object
        PostgreSQLConnection.connection_type = PostgreSQLConfig.connection_type
        self.set_connection()

    def set_connection(self):
        """
        Set the connection to the microsoft server based on the inputs
        :return:
        """
        config = self.config

        logging.info(f"Connecting to Postgre Server")

        # optional_params contains something like ?host=/var/lib/postgresql"
        if not config.connection_str:
            config.connection_str = create_uri(
                drivername="postgresql+psycopg2",
                config=config,
                **self.options
            )

        self.engine, self.conn \
            = connection_generator(config.connection_str, **self.options)