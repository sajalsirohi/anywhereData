from ..sql_config_builder import SQLConfig

__all__ = ["MySQLConfig"]


class MySQLConfig(SQLConfig):
    __doc__ = """
    Config Builder for MySQL server.
    :param: config: JSON like document containing the details to establish a connection  
    """
    connection_type = "mysql"

    def __init__(self, config, **options):
        # calling the super class to initialize the attributes
        super().__init__(config, **options)