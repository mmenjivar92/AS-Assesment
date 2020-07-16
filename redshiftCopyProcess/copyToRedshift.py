import configparser
import psycopg2
from commonFunctions.copy_queries import copy_table_queries

def load_tables(cur, conn):
    """
    The goal of this function is load into the presentation tables in redshift.
    Reads the querys from the copy_queries.py file and execute them trough a connection here.
    Parameters:
    cur (psycopg2.extensions.cursor)      : Connection cursor pointing to the redshift cluster
    conn (psycopg2.extensions.connection) : Connection pointin to the redshift cluster
    Returns:
    No returns
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Entry point of the script, creates the connection and cursor pointing to the redshift cluster using the psycopg2 library.
    Reads the configuration parameters from the dl.cfg file.
    Parameters:
    No parameters
    Returns:
    No returns
    """
    config = configparser.ConfigParser()
    config.read('../dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()