# Import all the necessary packages
import configparser
import psycopg2

from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
        Load data from staging tables to analytics tables on Redshift.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
        Load data from S3 to staging tables on Redshift.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Load the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Database on the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Load data from S3 to staging tables on Redshift
    load_staging_tables(cur, conn)
    # Load data from staging tables to analytics tables on Redshift
    insert_tables(cur, conn)

    # Close the connection
    conn.close()



if __name__ == "__main__":
    main()