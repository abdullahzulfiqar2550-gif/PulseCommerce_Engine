# =============================================
# PulseCommerce Engine - Worker (Order Processor)
# Run:   python src/worker.py        (Worker-1)
# Run:   python src/worker.py 2      (Worker-2)
# Run:   python src/worker.py 3      (Worker-3)
# Run:   python src/worker.py 4      (Worker-4)
# =============================================

import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
import json
import time
import redis
import psycopg2
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
QUEUE_NAME = "orders_queue"
DLQ_NAME = "dead_letter_queue"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5433"))
DB_NAME = os.getenv("DB_NAME", "pulsecommerce_db")
DB_USER = os.getenv("DB_USER", "pulse_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pulse_secure_password_123")

if len(sys.argv) > 1:
    WORKER_NUMBER = sys.argv[1]
else:
    WORKER_NUMBER = "1"
WORKER_NAME = "Worker-" + WORKER_NUMBER
INFLIGHT_KEY = "inflight:" + WORKER_NAME

os.makedirs("dead_letter", exist_ok=True)


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


def recover_inflight_order():
    old_order = redis_client.get(INFLIGHT_KEY)
    if old_order is not None:
        redis_client.lpush(QUEUE_NAME, old_order)
        redis_client.delete(INFLIGHT_KEY)
        print("[" + WORKER_NAME + "] RECOVERED in-flight order from last crash!")
    else:
        print("[" + WORKER_NAME + "] Clean start.")


def run_worker():
    print("==================================================")
    print("PULSECOMMERCE", WORKER_NAME.upper(), "STARTED")
    print("Listening to Redis queue:", QUEUE_NAME)
    print("Press Ctrl+C to stop.")
    print("==================================================")
    print("")

    recover_inflight_order()

    conn = connect_to_database()
    if conn is None:
        return

    cursor = conn.cursor()

    while True:
        redis_client.setex("worker_heartbeat:" + WORKER_NAME, 10, "active")

        result = redis_client.blpop(QUEUE_NAME, timeout=2)
        if result is None:
            continue

        queue_name, order_json = result
        order = json.loads(order_json)

        # In-flight lease
        redis_client.set(INFLIGHT_KEY, order_json)
        start_time = time.time()

        # Read fields
        order_id = order.get("order_id", "UNKNOWN")
        customer_name = order.get("customer_name", "Anonymous")
        product_name = order.get("product_name", "Unknown")
        product_category = order.get("product_category", "general")
        brand = order.get("brand", "N/A")
        unit_price = float(order.get("unit_price", 0))
        shipping_cost = float(order.get("shipping_cost", 0))
        total_amount = float(order.get("total_amount", 0))
        currency = order.get("currency", "USD")
        items_count = int(order.get("items_count", 1))
        product_weight = float(order.get("product_weight_g", 0))
        payment_method = order.get("payment_method", "Cash")
        rating = float(order.get("rating", 0))
        stock = int(order.get("stock", 0))
        source = order.get("source", "Unknown")

        # DLQ check
        if total_amount <= 0:
            redis_client.rpush(DLQ_NAME, json.dumps(order))
            redis_client.incr("dlq_count")
            with open("dead_letter/dlq_" + order_id + ".json", "w") as f:
                json.dump(order, f, indent=2)
            redis_client.delete(INFLIGHT_KEY)
            print("[" + WORKER_NAME + "] DLQ:", order_id, "|", product_name[:25], "| $" + str(total_amount))
            continue

        # Save to database
        try:
            sql = """
                INSERT INTO ecommerce_orders
                (order_id, customer_name, product_name, product_category, brand,
                 unit_price, shipping_cost, total_amount, currency, items_count,
                 product_weight_g, payment_method, rating, stock,
                 status, processed_by_worker, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (order_id) DO NOTHING;
            """
            cursor.execute(sql, (
                order_id, customer_name, product_name, product_category, brand,
                unit_price, shipping_cost, total_amount, currency, items_count,
                product_weight, payment_method, rating, stock,
                "PROCESSED", WORKER_NAME, source
            ))
            conn.commit()

            end_time = time.time()
            processing_ms = round((end_time - start_time) * 1000, 2)

            cursor.execute(
                "INSERT INTO pipeline_metrics (worker_id, rows_processed, processing_time_ms) VALUES (%s,%s,%s);",
                (WORKER_NAME, 1, processing_ms)
            )
            conn.commit()

            redis_client.delete(INFLIGHT_KEY)

            cursor.execute("SELECT COUNT(*) FROM ecommerce_orders;")
            total_rows = cursor.fetchone()[0]

            print("[" + WORKER_NAME + "] SAVED:", order_id, "|", product_name[:25], "| $" + str(total_amount),
                  "|", processing_ms, "ms | Rows:", total_rows)

        except Exception as error:
            conn.rollback()
            print("[" + WORKER_NAME + "] ERROR:", error)


if __name__ == "__main__":
    run_worker()
