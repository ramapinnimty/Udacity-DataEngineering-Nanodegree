# Import all the necessary packages
import configparser
import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
        Drop tables if they already exist.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
        Create tables if they do not exist.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Load the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Redshift cluster that has access to S3 through IAM
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Delete tables if they already exist
    drop_tables(cur, conn)
    # Create tables if they do not exist
    create_tables(cur, conn)

    # Close the connection
    conn.close()



if __name__ == "__main__":
    main()