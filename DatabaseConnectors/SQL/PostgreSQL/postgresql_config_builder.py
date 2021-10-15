from ..sql_config_builder import SQLConfig

__all__ = ["PostgreSQLConfig"]


class PostgreSQLConfig(SQLConfig):
    __doc__ = """
    Config Builder for MySQL server.
    :param: config: JSON like document containing the details to establish a connection  
    """
    connection_type = "postgresql"

    def __init__(self, config, **options):
        # calling the super class to initialize the attributes
        super().__init__(config, **options)
        self.port = config.get('port', 5432)