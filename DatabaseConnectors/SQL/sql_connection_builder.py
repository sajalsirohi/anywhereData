import logging
from abc import abstractmethod, ABC

import pandas as pd
from beartype import beartype

from GlobalBaseClasses import Connection
from .MSSQL.mssql_config_builder import MSSQLConfig
from .MySQL.mysql_config_builder import MySQLConfig
from .PostgreSQL.postgresql_config_builder import PostgreSQLConfig


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
        self.engine          = None
        self.config          = sql_config
        self.conn            = None
        self.curs            = None
        self.table_name      = ""
        self.current_df      = None
        self.connection_name = connection_name
        self.options         = options

    def get_data(self, query, **options):
        """
        Execute the query
        :param query:
        :return:
        """
        logging.info(f"Executing on connection_name -> :'{self.connection_name}': in SQL")
        logging.info(f"Executing the query : {query}")
        try:
            if query.strip().lower().startswith("select"):
                return pd.read_sql_query(query, self.conn)
            else:
                # execute the query
                self.conn.execute(query)
                return None
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

    def send_data(self, data, to_container, **options):
        """
        Put the values of df to `to_table`
        :param data:
        :param to_container:
        :return:
        """
        # legacy code for jdbc. Only supports appending as it manually runs insert commands
        if self.config.connect_through == 'jdbc':
            sym = "'"
            query = f"INSERT INTO {to_container} VALUES "
            if isinstance(data, pd.DataFrame):
                for datum in data.values.tolist():
                    query += f"({', '.join([f'{sym}{d}{sym}' for d in datum])}), "
            # if the data is sent in list format
            elif isinstance(data, (list, tuple)):
                # if multiple entries are sent in form of [[], [], []]
                if isinstance(data[0], (list, tuple)):
                    for datum in data:
                        query += f"({', '.join([f'{sym}{d}{sym}' for d in datum])}), "
                # if only one entry is given. []
                else:
                    query += f"({', '.join([f'{sym}{d}{sym}' for d in data])}), "
            query = query[0:-2]
            self.get_data(query)
        else:
            assert isinstance(data, pd.DataFrame), f"'data' should be of type pandas df, received type is " \
                                                   f"{type(data)}"
            logging.info(f'Processing the data to the table {to_container}')
            data.to_sql(to_container, self.conn, index=False, if_exists=options['if_exists'])

    def get_columns(self, entity_name):
        """
        Get the columns for the entity_name
        :param entity_name:
        :return:
        """
        df = pd.read_sql_query(f"select top 0 * from {entity_name}", self.conn)
        return list(df.columns)

    def __str__(self):
        return self.config