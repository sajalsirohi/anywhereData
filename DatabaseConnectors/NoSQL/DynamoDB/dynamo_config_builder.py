from GlobalBaseClasses import Config

__all__ = ["DynamoConfig"]


class DynamoConfig(Config):
    __doc__ = """
    Config Builder for microsoft SQL server.
    :param: config: JSON like document containing the details to establish a connection  
    """
    connection_type = "dynamodb"

    def __init__(self, config, **options):
        # calling the super class to initialize the attributes
        super().__init__(config, **options)
        self.table = self.options.get('table') or config.get('table')

    def __str__(self):
        return f"{super().__str__()} " \
               f"TABLE NAME : {self.table}"