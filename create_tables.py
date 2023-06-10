import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Summary:
        This function drops tables in the database should they exist to start fresh on iteration/run as established in the drop_table_queries list in sql_queries.py
    Args: 
        conn: for connection to database
        cur: for cursor object to execute queries
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Summary:
        This function creates the staging tables, fact and dimension tables in the database
    Args: 
        conn: for connection to database
        cur: for cursor object to execute queries
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Summary:
        This function connects to the database and calls the drop_tables and create_tables functions
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    print("******Drop Tables Complete******")
    create_tables(cur, conn)
    print("******Create Tables Complete******")
    conn.close()


if __name__ == "__main__":
    main()