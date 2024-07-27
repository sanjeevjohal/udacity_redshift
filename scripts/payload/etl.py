import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, select_number_rows_queries

import logging

# set up the logger
logging.basicConfig(filename='../../logs/etl.log', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.getLogger().setLevel(logging.INFO)  # setup log level


def load_staging_tables(cur, conn):
    '''
     load staging tables of sparkify database 
     
     parameter:
       1- cursor to database to execute queries
       2- connection to dataabase to perform commit 
       
    return :no
    '''
    for query in copy_table_queries:
        logging.info(f"query = {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
     insert into sparkify star schema from staging tables
    
    parameter:
       1- cursor to database to execute queries
       2- connection to dataabase to perform commit 
       
    return :no
     '''
    for query in insert_table_queries:
        logging.info(f"query = {query}")
        cur.execute(query)
        conn.commit()

def count_rows(cur, conn):
    '''
    count number of rows in a table

    parameter:
       1- cursor to database to execute queries
       2- table name to count rows

    return :no
    '''
    for query in select_number_rows_queries:
        logging.info(f"query = {query}")
        cur.execute(query)
        conn.commit()
        result = cur.fetchone()
        logging.info(f"table {query} has {result[0]} rows")

def main():
    """
    reads config file
    start and setup logger
    create connection to database
    loads staging tables of sparkify database
    insert data into sparkify star schema from staging tables
    close connection to database
    """

    import time, csv
    import os

    # add timer to measure time of execution
    start_time = time.monotonic()

    config = configparser.ConfigParser()
    config.read('../../dwh.cfg')

    conn_string = f"postgresql://" \
                  f"{config['CLUSTER']['DWH_DB_USER']}:{config['CLUSTER']['DWH_DB_PASSWORD']}" \
                  f"@{config['CLUSTER']['HOST']}:{config['CLUSTER']['DWH_PORT']}/{config['CLUSTER']['DWH_DB']}"
    logging.info(f"connection string {conn_string}")
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()


    logging.info('STARTING: loading staging tables')
    load_staging_tables(cur, conn)
    logging.info('FINISHED: loading staging tables')
    insert_tables(cur, conn)
    logging.info('FINISHED: inserting tables')

    end_time = time.monotonic()
    elapsed_time = end_time - start_time
    logging.info(f"Elapsed time for loading: {elapsed_time:.2f} seconds")

    count_rows(cur, conn)
    logging.info('FINISHED: counting records')

    # write performance to a csv file and include name of package, date and time of execution, and elapsed time
    package_name = os.path.basename(__file__)
    current_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with open('../../logs/performance_report.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([package_name, current_datetime, f"{elapsed_time:.2f} seconds"])

    conn.close()


if __name__ == "__main__":
    main()