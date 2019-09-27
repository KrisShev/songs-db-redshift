import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Executes all copy commands in the file copy_table_queies given a Postgres connection and cursor
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Executes all insertions from the file insert_table_queries given a Postgres connection and cursor
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()