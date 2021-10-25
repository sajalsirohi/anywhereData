from GlobalBaseClasses import Config

__all__ = ["MongoConfig"]


class MongoConfig(Config):
    __doc__ = """
    Config Builder for microsoft SQL server.
    :param: config: JSON like document containing the details to establish a connection  
    """
    connection_type = "mongodb"

    def __init__(self, config, **options):
        # calling the super class to initialize the attributes
        super().__init__(config, **options)
        self.port = self.port or 27017
        self.collection_name = self.options.get('collection_name') or 'default_collection'

    def __str__(self):
        return f"{super().__str__()} " \
               f"COLLECTION NAME : {self.collection_name}"