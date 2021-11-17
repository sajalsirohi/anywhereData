import logging
from abc import abstractmethod, ABC

from pyspark.sql.dataframe import DataFrame

from beartype import beartype

from GlobalBaseClasses import Connection
from .MSSQL.mssql_config_builder import MSSQLConfig
from .MySQL.mysql_config_builder import MySQLConfig
from .PostgreSQL.postgresql_config_builder import PostgreSQLConfig

from package_utils import spark


class SQLConnection(Connection, ABC):
    """
    SQL connection object
    """

    @beartype
    def __init__(self,
                 sql_config: (MSSQLConfig, MySQLConfig, PostgreSQLConfig),
                 connection_name,
                 **options):
        logging.info(f"Connecting to server with config : \n {sql_config}")
        self.jdbc_url: str = ""
        self.config = sql_config
        self.table_name: str = ""
        self.current_df: DataFrame = None
        self.connection_name: str = connection_name
        self.options: dict = options

    @beartype
    def get_data(self, query: str, **options):
        """
        Execute the query
        :param query:
        :return:
        """
        logging.info(
            f"Executing on connection_name -> :'{self.connection_name}': in SQL, connecting to : {self.jdbc_url}"
        )
        logging.info(f"Executing the query : {query}, "
                     f"spark_read_additional_parameters : {options.get('spark_read_additional_parameters')}")
        try:
            # reading the dataframe from the database using the query passed on
            self.current_df = spark.read \
                .format("jdbc") \
                .options(**{
                    **{
                        "url": self.jdbc_url,
                        "user": self.config.username,
                        "password": self.config.password,
                        "dbtable": f"({query}) some_alias" if "select" in query.lower() else query,  # if only table name is given
                        "driver": self.config.driver
                    },
                    **{
                        **options.get('spark_read_additional_parameters', {})
                    }
                })
        except Exception as err:
            logging.error(f"Error occurred while executing query : {query}, Error stack - {err}")
            if options.get('ignore_errors'):
                return ""
            else:
                raise err

    def set_current_table(self, table_name, **options):
        """
        Set the current table
        :param table_name:
        :param options:
        :return:
        """
        self.current_df = self.get_data(f"select {options.get('top', '')} * from {table_name} "
                                        f" {options.get('where', '')} ")

    @beartype
    def send_data(self, data: DataFrame, to_container: str, **options) -> None:
        """
        Put the values of df to `to_table`
        :param data:
        :param to_container:
        :return:
        """
        logging.info(f"Sending the data to the connection name: {self.connection_name},"
                     f" with schema : {data.schema}, \nto table : {to_container}, jdbc_url : {self.jdbc_url}")
        data.write \
            .format("jdbc") \
            .options(**{
             **{
                "url": self.jdbc_url,
                "user": self.config.username,
                "password": self.config.password,
                # if only table name is given
                "dbtable": to_container,
                "driver": self.config.driver
             },
             **{
                **options.get('spark_write_additional_parameters', {})
             }
         })

    def __str__(self):
        return self.config
