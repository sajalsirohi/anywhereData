from abc import ABC, abstractmethod


class Config(ABC):
    __doc__ = """
    Base class for config, which is passed onto to connection factory, to establish a 
    connection.
    """
    # connection type info which is passed into every class that implements this class
    connection_type = None

    @abstractmethod
    def parse_config(self):
        pass


class Connection(ABC):
    __doc__ = """
    A connection type abstract class. 
    """
    connection_name = None

    @abstractmethod
    def execute_raw_query(self):
        pass

    @abstractmethod
    def persist(self, data, to_table, **options):
        pass