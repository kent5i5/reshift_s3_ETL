import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''This method run queries that drop the existing tables'''
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''This method run queriest that create staging tables and analysis tables'''
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect the db with redshit endpoint, port, user, password
    conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    
    conn = psycopg2.connect(conn_string)
    
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()