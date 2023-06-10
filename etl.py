import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Summary:
        This function loads the staging files from S3 to staging tables in Redshift
    Args: 
        conn: for connection to database
        cur: for cursor object to execute queries
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Summary:
        This function loads data from staging tables in Redshift to final tables in Redshift
    Args: 
        conn: for connection to database
        cur: for cursor object to execute queries
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Summary:
        This function connects to the database and calls the load_staging_tables and insert_tables functions
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("******Staging Tables Load Complete******")
    insert_tables(cur, conn)
    print("******Final Tables Load Complete******")
    conn.close()


if __name__ == "__main__":
    main()