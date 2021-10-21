from .ConnectionPool import ConnectionPool

connection_pool = ConnectionPool(connections_config=None, connections_yaml=True)

__all__ = ['connection_pool', 'ConnectionPool']