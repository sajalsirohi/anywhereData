from GlobalBaseClasses import Config

__all__ = ["PostgreSQLConfig"]


class PostgreSQLConfig(Config):
    __doc__ = """
    Config Builder for MySQL server.
    :param: config: JSON like document containing the details to establish a connection  
    """
    connection_type = "postgresql"

    def __init__(self, config, **options):
        # calling the super class to initialize the attributes
        super().__init__(config, **options)
        self.port = self.port or 5432
        self.connect_through = 'easter_egg_random_time'