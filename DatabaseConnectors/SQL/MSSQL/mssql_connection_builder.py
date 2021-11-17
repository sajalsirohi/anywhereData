import logging
import urllib

from beartype import beartype

from .mssql_config_builder import MSSQLConfig
from ..sql_connection_builder import SQLConnection


class MSSQLConnection(SQLConnection):
    """
    Microsoft SQL connection class for establishing connection
    """
    connection_type = "mssql"

    @beartype
    def __init__(self, config: MSSQLConfig, connection_name, **options):
        super().__init__(config, connection_name, **options)
        logging.info(f"Connecting to the connection name : {connection_name}")
        self.config.db_name = config.db_name or "master"
        self.jdbc_url = self.create_jdbc_url()

    def create_jdbc_url(self) -> str:
        """
        Create the jdbc URL
        """
        return f"jdbc:sqlserver://{self.config.host}:{self.config.port}" \
               f";database={self.config.db_name}{self.config.properties}"