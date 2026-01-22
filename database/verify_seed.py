import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

try:
    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT')
    )

    cur = conn.cursor()

    print("--- Exchanges ---")
    cur.execute("SELECT id, name FROM exchange")
    for row in cur.fetchall():
        print(row)

    print("\n--- Products ---")
    cur.execute("SELECT id, exchange_id, base_asset, quote_asset FROM product")
    for row in cur.fetchall():
        print(row)

    print("\n--- Strategies ---")
    cur.execute("SELECT id, name FROM strategy")
    for row in cur.fetchall():
        print(row)

    conn.close()
except Exception as e:
    print(f"Error: {e}")
