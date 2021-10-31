import json
import logging
import os
from multiprocessing.pool import ThreadPool

import boto3
import pandas as pd

from beartype import beartype
from boto3.dynamodb.types import TypeSerializer
from boto3.dynamodb.conditions import Key

from .dynamo_config_builder import DynamoConfig
from ..nosql_connection_builder import NoSQLConnection

serializer = TypeSerializer()


class DynamoConnection(NoSQLConnection):
    """
    Microsoft SQL connection class for establishing connection
    """
    connection_type = "dynamodb"

    @beartype
    def __init__(self, config: DynamoConfig, connection_name, **options):
        super().__init__(config, connection_name, **options)
        logging.info(f"Connecting to the connection name : {connection_name}")
        self.client = boto3.client('dynamodb')
        self.resource = boto3.resource('dynamodb')
        self.set_connection()

    def set_connection(self):
        """
        Set the connection to the microsoft server based on the inputs
        :return:
        """
        logging.info("Nothing is needed to setup the connection as of now for dynamo db, just have "
                     "the config file ready, or set the environment variables")

    def execute_nosql_query(self, query=None, **options) -> pd.DataFrame:
        """
        query: Will be in form for json
        """
        print(options)
        if query is None:
            query = {}
        table = self.config.table or options['table']
        # create the table object
        logging.info(f"Inside the DynamoDB query execution method, executing query on : {table}")
        dynamo_table = self.resource.Table(table)
        # get the total cpu processors
        total_cpu_count = os.cpu_count()
        logging.info(f"Total CPU count : {total_cpu_count}")

        def fetch_data(thread_no):
            # parallel execution of threads and fetching the data in parallel manner
            scan_args = {**{'Segment': thread_no, 'TotalSegments': total_cpu_count}, **query}
            return dynamo_table.scan(**scan_args)

        logging.info('Fetching the results now')
        results = ThreadPool(20).imap_unordered(fetch_data, list(range(total_cpu_count)))
        total_results = []
        # Appending all the parallel results into the final result list
        logging.info('Creating the dataframe object now')
        for result in results:
            total_results += result['Items']
        logging.info(f"Total record count fetched : {len(total_results)}")
        return pd.DataFrame(total_results)

    @beartype
    def persist_nosql(self, data: pd.DataFrame, to_container, **options):
        """
        An abstract method that will be overwritten by the derived class
        """
        import uuid
        logging.info('Converting data into json format')
        json_data = json.loads(data.to_json(orient='records'))
        logging.info(f'Creating the container object for container : {to_container}')
        # creating the dynamo table object
        dynamo_table = self.resource.Table(to_container)
        with dynamo_table.batch_writer() as batch:
            for data in json_data:
                if str(options.get('automatic_id', 'true')).lower() == 'true':
                    data['_id'] = str(uuid.uuid4())
                batch.put_item(Item=data)
        logging.info(f"Successfully sent out the data for length : {len(data)}")