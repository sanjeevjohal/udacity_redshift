import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

import logging

# set up the logger
logging.basicConfig(filename='../../logs/create_tables.log', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.getLogger().setLevel(logging.INFO)  # setup log level


def drop_tables(cur, conn):
    '''Drop tables of sparkify database
    
       parameters :
       1- cursor to database to execute queries
       2- connection to dataabase to perform commit 
       
       return : no return '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''Create tables of sparkify database
    
       parameters :
       1- cursor to database to execute queries
       2- connection to dataabase to perform commit 
       
       return : no return '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    main fuction of create tables script.
    load AWS parameter from config file
    Connects to database and execute creation script.
    
    parameter :no
    
    return : no    
    '''
    config = configparser.ConfigParser()
    config.read('../../dwh.cfg')

    conn_string = f"postgresql://" \
                  f"{config['CLUSTER']['DWH_DB_USER']}:{config['CLUSTER']['DWH_DB_PASSWORD']}" \
                  f"@{config['CLUSTER']['HOST']}:{config['CLUSTER']['DWH_PORT']}/{config['CLUSTER']['DWH_DB']}"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    import time, csv
    import os

    logging.info(f"connection string {conn_string}")

    # add timer to measure time of execution
    start_time = time.monotonic()

    logging.info(f"STARTED at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    drop_tables(cur, conn)
    logging.info(f"tables dropped at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    create_tables(cur, conn)
    logging.info(f"tables created at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

    end_time = time.monotonic()
    elapsed_time = end_time - start_time
    logging.info(f"ENDED at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    logging.info(f"Elapsed time: {elapsed_time:.2f} seconds")

    # write performance to a csv file and include name of package, date and time of execution, and elapsed time
    package_name = os.path.basename(__file__)
    current_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with open('../../logs/performance_report.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Package', 'Date/Time', 'Elapsed Time'])
        writer.writerow([package_name, current_datetime, f"{elapsed_time:.2f} seconds"])


    conn.close()


if __name__ == "__main__":
    main()
