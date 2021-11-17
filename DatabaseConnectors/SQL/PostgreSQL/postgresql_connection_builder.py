import logging

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
        self.jdbc_url = self.create_jdbc_url()

    def create_jdbc_url(self) -> str:
        """
        Create the jdbc URL
        """
        return f"jdbc:postgresql://{self.config.host}:{self.config.port}" \
               f"/{self.config.db_name}"