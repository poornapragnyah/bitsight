#!/usr/bin/env python3

import psycopg2
import sys
from tabulate import tabulate
import os
from datetime import datetime

# Database connection parameters
DB_PARAMS = {
    'dbname': 'bitsight',
    'user': 'poorna',
    'password': 'poorna',
    'host': 'localhost',
    'port': '5432'
}

def connect_to_db():
    """Establish connection to the database"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def list_tables(conn):
    """List all available tables"""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [row[0] for row in cur.fetchall()]
        print("\nAvailable Tables:")
        for i, table in enumerate(tables, 1):
            print(f"{i}. {table}")
        cur.close()
        return tables
    except Exception as e:
        print(f"Error listing tables: {e}")
        return []

def check_table_contents(conn, table_name):
    """Display contents of a table"""
    try:
        cur = conn.cursor()
        # Determine the datetime column based on table name
        if table_name == 'stock_data':
            datetime_col = 'price_datetime'
        else:
            datetime_col = 'start_datetime'
            
        cur.execute(f"SELECT * FROM {table_name} ORDER BY {datetime_col} DESC LIMIT 10")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        print(f"\nContents of {table_name} table (latest 10 rows):")
        print(tabulate(rows, headers=colnames, tablefmt='grid'))
        cur.close()
    except Exception as e:
        print(f"Error reading table contents: {e}")

def check_table_schema(conn, table_name):
    """Display schema of a table"""
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """)
        rows = cur.fetchall()
        print(f"\nSchema of {table_name} table:")
        print(tabulate(rows, headers=['Column Name', 'Data Type', 'Max Length'], tablefmt='grid'))
        cur.close()
    except Exception as e:
        print(f"Error reading table schema: {e}")

def run_custom_query(conn):
    """Run a custom SQL query"""
    try:
        query = input("\nEnter your SQL query: ")
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        if rows:
            colnames = [desc[0] for desc in cur.description]
            print("\nQuery Results:")
            print(tabulate(rows, headers=colnames, tablefmt='grid'))
        else:
            print("\nNo results found.")
        cur.close()
    except Exception as e:
        print(f"Error executing query: {e}")

def check_db_size(conn):
    """Check database size"""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT pg_size_pretty(pg_database_size(current_database())) as db_size;
        """)
        size = cur.fetchone()[0]
        print(f"\nDatabase Size: {size}")
        cur.close()
    except Exception as e:
        print(f"Error checking database size: {e}")

def main():
    """Main menu function"""
    conn = connect_to_db()
    
    while True:
        print("\nDatabase Inspector Menu")
        print("1. List available tables")
        print("2. Check table contents")
        print("3. Check table schema")
        print("4. Run custom SQL query")
        print("5. Check database size")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ")
        
        if choice == '1':
            list_tables(conn)
        elif choice == '2':
            tables = list_tables(conn)
            if tables:
                table_choice = input("\nEnter table number: ")
                try:
                    table_index = int(table_choice) - 1
                    if 0 <= table_index < len(tables):
                        check_table_contents(conn, tables[table_index])
                    else:
                        print("Invalid table number.")
                except ValueError:
                    print("Please enter a valid number.")
        elif choice == '3':
            tables = list_tables(conn)
            if tables:
                table_choice = input("\nEnter table number: ")
                try:
                    table_index = int(table_choice) - 1
                    if 0 <= table_index < len(tables):
                        check_table_schema(conn, tables[table_index])
                    else:
                        print("Invalid table number.")
                except ValueError:
                    print("Please enter a valid number.")
        elif choice == '4':
            run_custom_query(conn)
        elif choice == '5':
            check_db_size(conn)
        elif choice == '6':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")
    
    conn.close()

if __name__ == "__main__":
    main() 