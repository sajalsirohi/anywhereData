import logging
import sqlalchemy

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
        self.jdbc_url = self.create_jdbc_url()

    def create_jdbc_url(self) -> str:
        """
        Create the jdbc URL
        """
        return f"jdbc:mysql://{self.config.host}:{self.config.port}/{self.config.db_name}" \
               f"{self.config.properties}"
