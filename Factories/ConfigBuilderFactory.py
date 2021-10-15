import logging

from package_utils import Singleton

from DatabaseConnectors import\
    MySQLConfig,\
    MSSQLConfig,\
    PostgreSQLConfig


class ConfigBuilderFactory(metaclass=Singleton):

    __doc__ = """
    A config builder factory which takes a connection config in json / yaml 
    format, and returns an config object, which is acceptable by the 
    ConnectionFactory, to return an instance of that connection info
    """

    def __init__(self):
        """
        Store config builders in this dict. Add as new config builders are added
        """
        self._config_builders = {}

    def register_config_builder(self, config_builder_type, config_builder) -> None:
        """
        Register new config builders using this method
        """
        if config_builder_type not in self._config_builders:
            self._config_builders[config_builder_type] = config_builder

    def create(self, raw_config, **options):
        """
        Create the config object
        :param raw_config: JSON input config, which contains connection related
                           attributes
        :return:
        """
        # combine the raw_config and options together, and assign it to raw config
        raw_config = {**raw_config, **options}

        logging.info(f"Creating a config obj, using the raw_config : {raw_config}")
        config_builder_type = raw_config.get('connection_type')

        assert config_builder_type, f"'connection_type' key is missing from : {raw_config}" \
                                    f"Need this key to determine the type of connection"

        # need to check if we have this connection_type config builder registered or not
        config_builder = self._config_builders.get(config_builder_type)
        if config_builder:
            return config_builder(raw_config)
        else:
            raise ValueError(f'Config builder not present for connection_type : "{config_builder_type}"')


# creating an instance on Config builder factory
config_factory = ConfigBuilderFactory()

# registering the SQL config
config_factory.register_config_builder('mssql', MSSQLConfig)
config_factory.register_config_builder('mysql', MySQLConfig)
config_factory.register_config_builder('postgresql', PostgreSQLConfig)