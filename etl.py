import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time
import sys

"""
    Description: This function is responsible for load JSON song/artist data from S3 bucket and creates temporary view
   
    Arguments:
        cur: the cursor object.
        conn: RedShift DB connection
    
    Returns:
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        start_time = time.time()
        print(('Starting load staging data into {} table').format(query))
        print('-' * 40)
        cur.execute(copy_table_queries[query])
        conn.commit()
        print(('load staging data done for table {} ').format(query))
        print("\nThis took %s seconds." % (time.time() - start_time))
        print('-' * 40)

"""
    Description: This function is responsible for Process and load log data from S3 bucket and creates temporary view 
    
    Arguments:
        cur: the cursor object.
        conn: RedShift DB connection
    
    Returns:
        None
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        start_time = time.time()
        print(('Starting load data into {} table').format(query))
        print('-' * 40)
        cur.execute(insert_table_queries[query])
        conn.commit()
        print(('load data done for table {} ').format(query))
        print("\nThis took %s seconds." % (time.time() - start_time))
        print('-' * 40)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
