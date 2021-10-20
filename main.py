from package_utils import connections_config
from ConnectionPool import ConnectionPool


def main():
    """
    Main driver of the program
    """
    cp = ConnectionPool(connections_config=connections_config)
    print(cp.conns['mssql_conn'].execute_raw_query("select * from cars"))


if __name__ == "__main__":
    main()
    print("sajal")
