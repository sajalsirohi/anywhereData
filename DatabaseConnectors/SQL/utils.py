import logging

import sqlalchemy
from beartype import beartype
from sqlalchemy import create_engine

from GlobalBaseClasses import Config


def connection_generator(connection_str, **options) -> tuple:
    """
    Creates connection object using the connection_str
    :param connection_str:
    :return:
    """
    try:
        try:
            engine = create_engine(connection_str, fast_executemany=options.get('fast_executemany', True))
        except Exception as err_:
            engine = create_engine(connection_str)
        # create the connection object
        conn = engine.connect()

        logging.info(f"Successfully connected to connection string : {connection_str}")
        return engine, conn

    except Exception as e:
        logging.error(f"Error while connecting using the connection string : {connection_str}")
        raise e


@beartype
def create_uri(drivername, config: Config, **options):
    """
    Create the connection URI which is used to create connection object.
    """

    return sqlalchemy.engine.url.URL.create(
        drivername=drivername,
        username=config.username,
        password=config.password,
        host=config.host,
        database=config.db_name,
        port=config.port,
        query=options.get('query')
    )

