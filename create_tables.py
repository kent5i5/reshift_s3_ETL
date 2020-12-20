import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST = config.get('CLUSTER','HOST')
    DB_NAME= config.get('CLUSTER','DB_NAME')
    DB_USER= config.get('CLUSTER','DB_USER')
    DB_PASSWORD= config.get('CLUSTER','DB_PASSWORD')
    DB_PORT= config.get('CLUSTER','DB_PORT')
    
    print(HOST)
    print(DB_NAME)
    print(DB_USER)
    print(DB_PASSWORD)
    print(DB_PORT)
    
    conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    print(conn_string)
    conn = psycopg2.connect(conn_string)
    #conn ="postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, HOST, DB_PORT,DB_NAME)
    
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()