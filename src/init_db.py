# =============================================
# PulseCommerce Engine - Database Setup
# Run: python src/init_db.py
# =============================================

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5433"))
DB_NAME = os.getenv("DB_NAME", "pulsecommerce_db")
DB_USER = os.getenv("DB_USER", "pulse_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pulse_secure_password_123")


def connect_to_database():
    for port in [DB_PORT, 5432]:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=port,
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            print("Connected to PostgreSQL on port", port)
            return conn
        except Exception:
            continue
    print("ERROR: Cannot connect to PostgreSQL!")
    return None


def create_tables():
    conn = connect_to_database()
    if conn is None:
        return

    cursor = conn.cursor()
    conn.autocommit = True

    cursor.execute("DROP TABLE IF EXISTS ecommerce_orders CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS pipeline_metrics CASCADE;")
    print("Old tables dropped.")

    cursor.execute("""
        CREATE TABLE ecommerce_orders (
            order_id            VARCHAR(50) PRIMARY KEY,
            customer_name       VARCHAR(100),
            product_name        VARCHAR(200),
            product_category    VARCHAR(100),
            brand               VARCHAR(100),
            unit_price          FLOAT,
            shipping_cost       FLOAT,
            total_amount        FLOAT,
            currency            VARCHAR(10) DEFAULT 'USD',
            items_count         INT,
            product_weight_g    FLOAT,
            payment_method      VARCHAR(50),
            rating              FLOAT,
            stock               INT,
            status              VARCHAR(50),
            processed_by_worker VARCHAR(50),
            source              VARCHAR(50),
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    print("Table 'ecommerce_orders' created (18 columns).")

    cursor.execute("""
        CREATE TABLE pipeline_metrics (
            metric_id           SERIAL PRIMARY KEY,
            worker_id           VARCHAR(50),
            rows_processed      INT,
            processing_time_ms  FLOAT,
            recorded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    print("Table 'pipeline_metrics' created.")

    cursor.execute("CREATE INDEX idx_orders_category ON ecommerce_orders(product_category);")
    cursor.execute("CREATE INDEX idx_orders_source ON ecommerce_orders(source);")
    cursor.execute("CREATE INDEX idx_orders_date ON ecommerce_orders(created_at);")
    cursor.execute("CREATE INDEX idx_orders_worker ON ecommerce_orders(processed_by_worker);")

    print("")
    print("========================================")
    print("DATABASE SETUP COMPLETE!")
    print("========================================")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    create_tables()
