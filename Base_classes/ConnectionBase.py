from abc import ABC, abstractmethod


class Config(ABC):
    __doc__ = """
    Base class for config, which is passed onto to connection factory, to establish a 
    connection.
    """
    # contains raw_config
    raw_config = {}

    @abstractmethod
    def parse_config(self):
        pass