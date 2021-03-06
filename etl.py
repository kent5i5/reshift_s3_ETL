import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, create_table_queries


def load_staging_tables(cur, conn):
    '''This method run postgre queries that copy data from s3 to staging tables'''
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''This method tranform the data in staging tables into Fact and Dimension tables'''
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

    

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Connect the db with redshit endpoint, port, user, password
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
   
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()