import os
import psycopg2
from psycopg2 import pool
import urllib.parse as urlparse
import settings_manager # Added import

# Create a connection pool
db_pool = None

def init_pool():
    global db_pool
    if db_pool is None:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is not set")

        url = urlparse.urlparse(database_url)
        db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port,
            database=url.path[1:]
        )

def get_db_connection():
    if db_pool is None:
        init_pool()
    return db_pool.getconn()

def release_db_connection(conn):
    if db_pool is not None:
        db_pool.putconn(conn)

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # User Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    totp_secret TEXT
                );
            """)

            # Savings Goals Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS savings_goals (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    target_amount NUMERIC NOT NULL,
                    saved_amount NUMERIC DEFAULT 0.0
                );
            """)

            # Transactions Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id SERIAL PRIMARY KEY,
                    type TEXT NOT NULL,
                    category TEXT NOT NULL,
                    item TEXT NOT NULL,
                    amount NUMERIC NOT NULL,
                    date DATE NOT NULL,
                    description TEXT,
                    savings_goal_id INTEGER REFERENCES savings_goals(id) ON DELETE SET NULL
                );
            """)

            # Expense Categories Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS expense_categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    icon TEXT
                );
            """)

            # Income Categories Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS income_categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    icon TEXT
                );
            """)
            
            # Settings Table (Key-Value Store)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)

            conn.commit()
            # Initialize default settings after tables are created
            settings_manager.initialize_default_settings() # Added call
    finally:
        release_db_connection(conn)

if __name__ == '__main__':
    # This allows you to run `python db.py` to initialize the database manually.
    print("Initializing database...")
    init_db()
    print("Database initialization complete.")
