import logging
from beartype import beartype

from package_utils import Singleton
from Base_classes import Config, Connection


class ConnectionFactory(metaclass=Singleton):

    __doc__ = """
    A connection factory class, that takes in config instance, and creates a 
    connection object using that config instance.
    """

    def __init__(self):
        """
        Store connection builders in this dict. Add as new connection builders are added
        """
        self._connection_builders = {}

    def register_connection_builder(self, connection_type, connection_builder) -> None:
        """
        Register new config builders using this method
        :param connection_type: Type of the connection. SQL | Hive | MySQL etc
        :param connection_builder:
        """
        if connection_type not in self._connection_builders:
            self._connection_builders[connection_type] = connection_builder

    @beartype
    def create(self, config: Config, **options) -> Connection:
        """
        Create the config object
        :param config: A Config instance that contains the information to create or initialize
        a connection with the Database.
        :return: Connection instance
        """

        logging.info(f"Creating a config obj, using the raw_config : {config}")
        connection_type = config.connection_type

        assert connection_type, f"'connection_type' key is missing from : {config}" \
                                f"Need this key to determine the type of connection"

        # need to check if we have this connection_type config builder registered or not
        connection_builder = self._connection_builders.get(connection_type)
        if connection_builder:
            return connection_builder(config, **options)
        else:
            raise ValueError(f'Config builder not present for connection_type : "{connection_type}"')