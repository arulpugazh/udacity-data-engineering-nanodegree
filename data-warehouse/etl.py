import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, drop_table_queries, create_table_queries

def load_staging_tables(cur, conn):
    """Copy data from S3 to Redshift staging tables
    
    Args:
        cur (cursor): Redshift Cursor Object
        conn (connection): Redshift Connection Object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Copy records from staging tables to final tables
    
    Args:
        cur (cursor): Redshift Cursor Object
        conn (connection): Redshift Connection Object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ETL pipeline which drops existing tables, creates tables, 
    copy data from S3 to Redshift staging tables and then insert records into final tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()