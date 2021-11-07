from GlobalBaseClasses import FileStorageConfig

__all__ = ["S3Config"]


class S3Config(FileStorageConfig):
    __doc__ = """
    Config Builder for S3 File Storage
    :param: config: JSON like document containing the details to establish a connection  
    """
    connection_type = "s3"

    def __init__(self, config, **options):
        # calling the super class to initialize the attributes
        super().__init__(config, **options)
        self.prefix = options.get("prefix")

    def __str__(self):
        return f"{super().__str__()} " \
               f"BUCKET : {self.s3_bucket}"
