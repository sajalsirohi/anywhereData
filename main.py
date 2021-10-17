from package_utils import *
from ConnectionPool import ConnectionPool


def main():
    """
    Main driver of the program
    """
    conn_config = [
        {
            'connection_name': 'sajal_connection',
            'connection_type': 'mssql',
            'username': 'sa',
            'password': 'Str0ngPa$$w0rd',
            'host': 'localhost',
        },
        {
            'connection_name': 'sajal_postgres',
            'connection_type': 'postgresql',
            'username': 'postgres',
            'password': 'docker',
            'host': 'localhost',
        },
        {
            'connection_name': 'sajal_mysql',
            'connection_type': 'mysql',
            'username': 'root',
            'password': 'Password',
            'host': '127.0.0.1',
            'port': 13306
        }
    ]
    cp = ConnectionPool(connections_config=conn_config)
    print(cp.conns['sajal_connection'].execute_raw_query("select * from cars"))


if __name__ == "__main__":
    main()
    print("sajal")
