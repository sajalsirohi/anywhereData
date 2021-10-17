from package_utils import *
from ConnectionPool import ConnectionPool


def main():
    """
    Main driver of the program
    """
    conn_config = {
        'connection_name': 'sajal_connection',
        'connection_type': 'mssql',
        'username': 'sa',
        'password': 'Str0ngPa$$w0rd',
        'host': 'localhost',
    }
    cp = ConnectionPool(connections_config=conn_config)


if __name__ == "__main__":
    main()
    print("sajal")