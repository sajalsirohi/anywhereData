import logging

from GlobalBaseClasses import Connection
from package_utils import Singleton
from beartype import beartype

from Factories import connection_factory, config_factory

__all__ = ["ConnectionPool"]


class ConnectionPool(metaclass=Singleton):
    __doc__ = """ 
    Create a connection pool. This pool will contain all connections mentioned in 
    connections config file and maintain them. All connections will be pulled 
    from here.
    """

    @beartype
    def __init__(self, connections_config: (list, dict, None), **options):
        # if connection_pool is to be created using the connections.yaml file
        if options.get('connections_yaml'):
            from package_utils import connections_config as cc
            connections_config = cc

        if isinstance(connections_config, dict):
            connections_config = [connections_config]

        self.connections_config = connections_config
        self.options = options
        self.conns = {}
        # create the connection pool
        self.create_connection_pool()

    def __getitem__(self, connection_name) -> Connection:
        """
        Return the connection type associated from .conns
        """
        return self.conns[connection_name]

    def set_connections_config(self, connections_config: list):
        """
        setter for connections_config
        """
        self.connections_config = connections_config

    def create_connection_pool(self):
        """
        Create the connection pool using the config
        :return:
        """
        logging.info(f"Starting to create the connection pool")
        for config in self.connections_config:
            logging.info(f"Working on connection config : {config}")

            try:
                connection_name = config['connection_name']
            except KeyError:
                raise KeyError(f"'connection_name' needs to be specified in the conn_config")

            logging.info(f'Fetching the config builder')

            # get the config object.
            database_config = config_factory.create(
                raw_config=config, connection_name=connection_name, **self.options
            )

            # create the connection object.
            conn = connection_factory.create(
                config=database_config, connection_name=connection_name, **self.options
            )

            # have the connection saved in conns field
            self.conns[connection_name] = conn
        print(f"Successfully created the connection pool : {self.conns}")
        logging.info(f"Successfully created the connection pool : {self.conns}")
        # stage db placeholder
        self.conns['stagedb'] = "just a place holder"