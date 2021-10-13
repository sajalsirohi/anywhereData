import logging
from beartype import beartype

from package_utils import Singleton


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
    def create(self, config: list, **options):
        """
        Create the config object
        :param config: JSON input config, which contains connection related
                       attributes
        :return:
        """

        logging.info(f"Creating a config obj, using the raw_config : {config}")
        config_builder_type = config.get('connection_type')

        assert config_builder_type, f"'connection_type' key is missing from : {config}" \
                                    f"Need this key to determine the type of connection"

        # need to check if we have this connection_type config builder registered or not
        config_builder = self._connection_builders.get(config_builder_type)
        if config_builder:
            return config_builder(config)
        else:
            raise ValueError(f'Config builder not present for connection_type : "{config_builder_type}"')