import logging
import os
import tempfile
import time

import boto3
import pandas as pd
from beartype import beartype

from GlobalBaseClasses import Connection
from .s3_config_builder import S3Config

__all__ = ['S3Connection']


class S3Connection(Connection):
    """
    Microsoft SQL connection class for establishing connection
    """
    connection_type = "local"

    @beartype
    def __init__(self, config: S3Config, connection_name, **options):
        self.config  = config
        self.options = options
        logging.info(f"Acknowledging of the S3 File connection name : {connection_name}")
        self.client   = boto3.client('s3')
        self.resource = boto3.resource('s3')

    def set_connection(self):
        """
        Set the connection to the microsoft server based on the inputs
        :return:
        """
        pass

    def get_data(self, query=None, **options) -> pd.DataFrame:
        """
        query: Will be in form for json
        """
        self.config.s3_bucket = options.get('s3_bucket', self.config.s3_bucket)
        self.config.prefix    = options.get('prefix', self.config.prefix) or ''
        logging.info(f"Getting data from the S3 File connection : {self.config.s3_bucket} "
                     f"and prefix : {self.config.prefix}")

        with tempfile.TemporaryDirectory() as tempdir:
            # create a temporary directory and then download all the files there
            response = self.client.list_objects(Bucket=self.config.s3_bucket, Prefix=self.config.prefix)
            s3_objects = [obj["Key"] for obj in response["Contents"]]
            print(s3_objects)
            for s3_file in s3_objects:
                if not s3_file.endswith("/"):
                    print(tempdir)
                    temp_str = s3_file.replace(self.config.prefix or '', "")
                    local_file_path = os.path.join(tempdir, temp_str)
                    logging.info(f"Local file path : {local_file_path} \ns3_file : {s3_file}")
                    self.client.download_file(self.config.s3_bucket, s3_file, local_file_path)
            logging.info(f"Downloaded files: {', '.join(os.listdir(tempdir))}")
            files_to_read = []
            # take the file_dir_path given in the task, else take the default value
            self.config.file_dir_path = options.get('file_dir_path', self.config.file_dir_path)
            logging.info(f"Reading multiple files as the given file path does not exist")

            for file in os.listdir(tempdir):
                # only going to read csv files for now
                if file.endswith(".csv"):
                    files_to_read.append(os.path.join(tempdir, file))

            logging.info(f"Going to read the files : {files_to_read}")
            result = pd.concat(map(pd.read_csv, files_to_read), ignore_index=True)
            logging.info(
                f"Successfully read the files, count : {len(result)} and this is the header : {result.columns}")

        return result

    @beartype
    def send_data(self, data: pd.DataFrame, to_container=None, **options):
        """
        An abstract method that will be overwritten by the derived class
        to_container : Name of the bucket where we need to send this
        """
        to_container = self.config.file_dir_path or to_container or options.get('s3_bucket')
        # this is the name of the file using which it will be saved
        file_path = f"s3://{to_container}" \
                    f"/{self.config.file_dir_path or options.get('prefix') or ''}" \
                    f"{self.config.file_name or options.get('file_name') or f'data_{str(time.time_ns())}.csv'}"
        logging.info(f"File path where we storing the data : {file_path}")

        data.to_csv(file_path, sep=self.config.sep or ",", index=False)
        logging.info(f"Data sent successfully to bucket with count : {len(data)}")