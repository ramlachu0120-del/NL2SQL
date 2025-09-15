import pandas as pd
import sqlite3
import os

def create_database(db_name):
    conn = sqlite3.connect(db_name)
    return conn

def create_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS daily_revenue (
        date TEXT PRIMARY KEY,
        revenue REAL
    );
    """
    conn.execute(create_table_query)
    conn.commit()

def insert_data_from_csv(conn, csv_file):
    df = pd.read_csv(csv_file)
    df.to_sql('daily_revenue', conn, if_exists='replace', index=False)

def main():
    db_name = 'demo.db'
    csv_file = 'daily_revenue.csv'

    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} does not exist.")
        return

    conn = create_database(db_name)
    create_table(conn)
    insert_data_from_csv(conn, csv_file)
    conn.close()
    print(f"Data from {csv_file} has been successfully imported into {db_name}.")

if __name__ == "__main__":
    main()