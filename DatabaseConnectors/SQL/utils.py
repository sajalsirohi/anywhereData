from sqlalchemy import create_engine


def connection_generator(connection_str, **options) -> tuple:
    """
    Creates connection object using the connection_str
    :param connection_str:
    :param options:
    :return:
    """
    engine = create_engine(connection_str, **options)

    # create the connection object
    conn = engine.connect()

    # create a cursor object
    curs = conn.cursor()
    return engine, conn, curs