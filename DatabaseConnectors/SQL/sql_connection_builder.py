from MSSQL import MSSQLConfig
from MySQL import MySQLConfig
from PostgreSQL import PostgreSQLConfig
from GlobalBaseClasses import Connection

import pandas as pd
import logging

from beartype import beartype
from abc import abstractmethod, ABC


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

    @abstractmethod
    def set_connection(self) -> None:
        """
        Get SQL curs and conn object for the appropriate environment.
        :return:
        """
        pass

    def execute_raw_query(self, query, **options):
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
                self.curs.execute(query)
                # commit the transaction
                self.conn.commit()
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
        self.current_df = self.execute_raw_query(f"select {options.get('top', '')} * from {table_name} "
                                                 f" {options.get('where', '')} ")

    def persist(self, data, to_table, **options):
        """
        Put the values of df to `to_table`
        :param data:
        :param to_table:
        :return:
        """
        # legacy code for jdbc. Only supports appending as it manually runs insert commands
        if self.config.connect_through == 'jdbc':
            sym = "'"
            query = f"INSERT INTO {to_table} VALUES "
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
            self.execute_raw_query(query)
        else:
            assert isinstance(data, pd.DataFrame), f"'data' should be of type pandas df, received type is " \
                                                   f"{type(data)}"
            logging.info(f'Processing the data to the table {to_table}')
            data.to_sql(to_table, self.conn, **options)

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