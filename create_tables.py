import psycopg2
from sql_queries import CREATE_TABLE_QUERIES, DROP_TABLE_QUERIES
from app_config_reader import get_database_configuration

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    cred = get_database_configuration()
    # connect to default database
    conn = psycopg2.connect(f"host={cred['host']} dbname={cred['defaultdbname']} user={cred['username']} password={cred['password']}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute(f"DROP DATABASE IF EXISTS {cred['dbname']}",)
    cur.execute(f"CREATE DATABASE {cred['dbname']} WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(f"host={cred['host']} dbname={cred['dbname']} user={cred['username']} password={cred['password']}")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in DROP_TABLE_QUERIES:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in CREATE_TABLE_QUERIES:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets cursor to it.  
    
    - Drops all the tables.
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()