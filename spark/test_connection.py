import psycopg2

try:
    conn = psycopg2.connect(
        host="172.18.0.2",
        database="bitsight",
        user="poorna",
        password="poorna",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stock_data")
    count = cur.fetchone()[0]
    print(f"Found {count} records in stock_data table")
    
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}") 