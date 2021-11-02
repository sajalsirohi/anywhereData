from GlobalBaseClasses import FileStorageConfig

__all__ = ["LocalFileConfig"]


class LocalFileConfig(FileStorageConfig):
    __doc__ = """
    Config Builder for microsoft SQL server.
    :param: config: JSON like document containing the details to establish a connection  
    """
    connection_type = "local"

    def __init__(self, config, **options):
        # calling the super class to initialize the attributes
        super().__init__(config, **options)