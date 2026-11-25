# =============================================
# PulseCommerce Engine - Live Dashboard
# Run: streamlit run dashboard/app.py
# Opens at: http://localhost:8501
# =============================================

import os
import sys
import time
import subprocess
import json
import uuid
import random
import psycopg2
import redis
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# --- Page Setup ---
st.set_page_config(page_title="PulseCommerce Dashboard", layout="wide")

# --- Connect to Redis ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# --- Connect to PostgreSQL ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5433"))
DB_NAME = os.getenv("DB_NAME", "pulsecommerce_db")
DB_USER = os.getenv("DB_USER", "pulse_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pulse_secure_password_123")


def get_db_connection():
    for port in [DB_PORT, 5432]:
        try:
            return psycopg2.connect(
                host=DB_HOST, port=port,
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
        except Exception:
            continue
    return None


# =============================================
# TITLE
# =============================================
st.title("PulseCommerce Engine - Live Dashboard")
st.caption("Real-Time Distributed Order Processing Pipeline | Muhammad Abdullah (2025-ai-348)")

# =============================================
# PIPELINE STAGES
# =============================================
st.markdown("### Pipeline Stages")
s1, s2, s3, s4, s5, s6 = st.columns(6)
s1.success("1. INGEST")
s2.success("2. STAGING")
s3.info("3. QUEUE")
s4.warning("4. WORKERS")
s5.error("5. DLQ")
s6.success("6. DATABASE")
st.divider()

# =============================================
# WORKER CONTROLS
# =============================================
st.markdown("### Worker Controls")

if "worker_processes" not in st.session_state:
    st.session_state.worker_processes = {}
if "producer_process" not in st.session_state:
    st.session_state.producer_process = None

# Check heartbeats
worker_keys = redis_client.keys("worker_heartbeat:*")
active_workers = len(worker_keys)
worker_names = [k.replace("worker_heartbeat:", "") for k in worker_keys]

# Clean dead processes from session
for wn in list(st.session_state.worker_processes.keys()):
    if st.session_state.worker_processes[wn].poll() is not None:
        del st.session_state.worker_processes[wn]
if st.session_state.producer_process is not None:
    if st.session_state.producer_process.poll() is not None:
        st.session_state.producer_process = None

# Worker buttons
w1, w2, w3, w4 = st.columns(4)
for col, num in [(w1, "1"), (w2, "2"), (w3, "3"), (w4, "4")]:
    wname = "Worker-" + num
    is_on = wname in worker_names or wname in st.session_state.worker_processes
    if is_on:
        col.markdown("**" + wname + "**: Running")
        if col.button("Stop " + wname, key="stop_" + num):
            if wname in st.session_state.worker_processes:
                st.session_state.worker_processes[wname].terminate()
                del st.session_state.worker_processes[wname]
            redis_client.delete("worker_heartbeat:" + wname)
            st.rerun()
    else:
        col.markdown("**" + wname + "**: Stopped")
        if col.button("Start " + wname, key="start_" + num):
            flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            proc = subprocess.Popen(
                [sys.executable, "src/worker.py", num],
                cwd=os.getcwd(), stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, creationflags=flags
            )
            st.session_state.worker_processes[wname] = proc
            st.rerun()

# Producer button
st.markdown("---")
pc1, pc2, pc3 = st.columns(3)
if st.session_state.producer_process is not None:
    pc1.markdown("**Producer**: Running")
    if pc1.button("Stop Producer"):
        st.session_state.producer_process.terminate()
        st.session_state.producer_process = None
        st.rerun()
else:
    pc1.markdown("**Producer**: Stopped")
    if pc1.button("Start Producer"):
        flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        proc = subprocess.Popen(
            [sys.executable, "src/producer.py"],
            cwd=os.getcwd(), stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, creationflags=flags
        )
        st.session_state.producer_process = proc
        st.rerun()

if worker_names:
    pc2.info("Active: " + ", ".join(sorted(worker_names)))
else:
    pc2.warning("No workers running")
pc3.metric("Active Workers", str(active_workers) + " / 4")

# =============================================
# FLASH SALE SIMULATION
# =============================================
st.markdown("### Flash Sale Simulation")
st.caption("Simulate a flash sale burst or inject a bad order for DLQ testing")

BURST_PRODUCTS = [
    {"name": "iPhone 15 Pro Max", "category": "smartphones", "brand": "Apple", "price": 1199.99, "weight": 221, "rating": 4.9, "stock": 12},
    {"name": "Samsung Galaxy S24 Ultra", "category": "smartphones", "brand": "Samsung", "price": 1299.99, "weight": 233, "rating": 4.7, "stock": 8},
    {"name": "Sony WH-1000XM5", "category": "headphones", "brand": "Sony", "price": 349.99, "weight": 250, "rating": 4.8, "stock": 25},
    {"name": "MacBook Air M3", "category": "laptops", "brand": "Apple", "price": 1099.99, "weight": 1240, "rating": 4.9, "stock": 15},
    {"name": "Nike Air Max 270", "category": "shoes", "brand": "Nike", "price": 150.00, "weight": 300, "rating": 4.5, "stock": 50},
    {"name": "PlayStation 5 Slim", "category": "gaming", "brand": "Sony", "price": 449.99, "weight": 3200, "rating": 4.6, "stock": 5},
    {"name": "Dyson V15 Detect", "category": "home-appliances", "brand": "Dyson", "price": 749.99, "weight": 2850, "rating": 4.4, "stock": 20},
    {"name": "Canon EOS R6 Mark II", "category": "cameras", "brand": "Canon", "price": 2499.99, "weight": 670, "rating": 4.8, "stock": 7},
]
BURST_NAMES = ["Bilal Ahmed", "Hamza Sheikh", "Ayesha Malik", "Fatima Zahra", "Usman Tariq", "Sara Khan", "Ali Raza", "Zainab Hussain"]

fb1, fb2, fb3 = st.columns(3)

if fb1.button("Burst: 8 Orders (Flash Sale)", type="primary"):
    for product in BURST_PRODUCTS:
        order = {
            "order_id": "BURST-" + uuid.uuid4().hex[:6].upper(),
            "customer_name": random.choice(BURST_NAMES),
            "product_name": product["name"],
            "product_category": product["category"],
            "brand": product["brand"],
            "unit_price": product["price"],
            "shipping_cost": round(random.uniform(3.0, 12.0), 2),
            "total_amount": product["price"],
            "currency": "USD",
            "items_count": random.randint(1, 3),
            "product_weight_g": float(product["weight"]),
            "payment_method": random.choice(["Credit Card", "JazzCash", "EasyPaisa", "COD"]),
            "rating": product["rating"],
            "stock": product["stock"],
            "source": "Flash Sale Burst"
        }
        redis_client.rpush("orders_queue", json.dumps(order))
    st.success("FLASH SALE! 8 orders injected into Redis queue!")
    st.rerun()

if fb2.button("Send Bad Order (DLQ Test)", type="secondary"):
    bad_order = {
        "order_id": "BAD-" + uuid.uuid4().hex[:6].upper(),
        "customer_name": "Test Customer",
        "product_name": "CORRUPT ITEM",
        "product_category": "test",
        "brand": "N/A",
        "unit_price": -99.99,
        "shipping_cost": 0.0,
        "total_amount": -99.99,
        "currency": "USD",
        "items_count": 1,
        "product_weight_g": 0.0,
        "payment_method": "None",
        "rating": 0,
        "stock": 0,
        "source": "Manual DLQ Test"
    }
    redis_client.rpush("orders_queue", json.dumps(bad_order))
    st.warning("Bad order injected! Worker will send it to Dead-Letter Queue.")
    st.rerun()

fb3.metric("Queue Now", redis_client.llen("orders_queue"))

st.divider()

# =============================================
# LIVE METRICS
# =============================================
st.markdown("### Live Metrics")

queue_length = redis_client.llen("orders_queue")
dlq_count = redis_client.get("dlq_count")
dlq_count = int(dlq_count) if dlq_count else 0

conn = get_db_connection()
total_orders = 0
total_revenue = 0
source_data = pd.DataFrame()
category_data = pd.DataFrame()
recent_orders = pd.DataFrame()
worker_stats = pd.DataFrame()
avg_time = 0

if conn is not None:
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*), COALESCE(SUM(total_amount), 0) FROM ecommerce_orders;")
    row = cur.fetchone()
    total_orders = row[0]
    total_revenue = round(row[1], 2)

    # Orders by source (API vs CSV vs Postman)
    cur.execute("SELECT source, COUNT(*) FROM ecommerce_orders GROUP BY source ORDER BY COUNT(*) DESC;")
    rows = cur.fetchall()
    if rows:
        source_data = pd.DataFrame(rows, columns=["Source", "Orders"])

    # Orders by product category
    cur.execute("SELECT product_category, COUNT(*) FROM ecommerce_orders GROUP BY product_category ORDER BY COUNT(*) DESC LIMIT 10;")
    rows = cur.fetchall()
    if rows:
        category_data = pd.DataFrame(rows, columns=["Category", "Orders"])

    # Recent 10 orders (NEW 18-column schema)
    cur.execute("""
        SELECT order_id, customer_name, product_name, product_category,
               brand, unit_price, shipping_cost, total_amount,
               currency, items_count, payment_method, rating, stock,
               processed_by_worker, source, created_at
        FROM ecommerce_orders ORDER BY created_at DESC LIMIT 10;
    """)
    rows = cur.fetchall()
    if rows:
        recent_orders = pd.DataFrame(rows, columns=[
            "Order ID", "Customer", "Product", "Category",
            "Brand", "Unit Price", "Shipping", "Total ($)",
            "Currency", "Items", "Payment", "Rating", "Stock",
            "Worker", "Source", "Time"
        ])

    # Worker stats
    cur.execute("""
        SELECT processed_by_worker, COUNT(*), ROUND(SUM(total_amount)::numeric, 2)
        FROM ecommerce_orders GROUP BY processed_by_worker ORDER BY processed_by_worker;
    """)
    rows = cur.fetchall()
    if rows:
        worker_stats = pd.DataFrame(rows, columns=["Worker", "Orders", "Revenue ($)"])

    # Avg processing time
    cur.execute("SELECT AVG(processing_time_ms) FROM pipeline_metrics;")
    avg_result = cur.fetchone()[0]
    if avg_result:
        avg_time = round(avg_result, 2)

    cur.close()
    conn.close()

# 5 metric cards
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Orders", total_orders)
c2.metric("Total Revenue", "$ " + str(total_revenue))
c3.metric("Queue Length", queue_length)
c4.metric("DLQ (Bad)", dlq_count)
c5.metric("Avg Speed", str(avg_time) + " ms")

st.divider()

# =============================================
# CHARTS: Source + Category side by side
# =============================================
chart1, chart2 = st.columns(2)

with chart1:
    st.markdown("### Orders by Source")
    if not source_data.empty:
        st.bar_chart(source_data.set_index("Source"))
    else:
        st.info("No data yet")

with chart2:
    st.markdown("### Orders by Product Category")
    if not category_data.empty:
        st.bar_chart(category_data.set_index("Category"))
    else:
        st.info("No data yet")

st.divider()

# =============================================
# RECENT ORDERS TABLE (NEW SCHEMA - 16 columns)
# =============================================
st.markdown("### Recent Orders (Full Details)")

if not recent_orders.empty:
    st.dataframe(recent_orders, width="stretch", hide_index=True)
else:
    st.info("No orders yet. Start the producer and workers.")

# =============================================
# WORKER PERFORMANCE
# =============================================
if not worker_stats.empty:
    st.markdown("### Worker Performance")
    st.dataframe(worker_stats, width="stretch", hide_index=True)

st.divider()

# =============================================
# KAGGLE DATASET EXPLORER
# =============================================
st.markdown("### Kaggle Dataset Explorer")
st.caption("All CSV files from the Olist Brazilian E-Commerce dataset")

data_folder = "data"
csv_files = sorted([f for f in os.listdir(data_folder) if f.endswith(".csv")]) if os.path.exists(data_folder) else []

if csv_files:
    file_info = []
    for f in csv_files:
        path = os.path.join(data_folder, f)
        size_kb = round(os.path.getsize(path) / 1024, 1)
        file_info.append({"File": f, "Size (KB)": size_kb})
    st.dataframe(pd.DataFrame(file_info), width="stretch", hide_index=True)

    selected = st.selectbox("Select CSV to explore:", csv_files)
    if selected:
        df = pd.read_csv(os.path.join(data_folder, selected), nrows=50)
        st.write("**Columns:**", list(df.columns), "| **Showing first 50 rows**")
        st.dataframe(df, width="stretch", hide_index=True)

st.divider()

# =============================================
# PIPELINE PERFORMANCE
# =============================================
st.markdown("### Pipeline Performance")
p1, p2, p3 = st.columns(3)
p1.metric("Avg Processing Time", str(avg_time) + " ms")

staging_count = len([f for f in os.listdir("staging_inbox") if f.endswith(".json")]) if os.path.exists("staging_inbox") else 0
p2.metric("Staging Files", staging_count)

dlq_files = len([f for f in os.listdir("dead_letter") if f.endswith(".json")]) if os.path.exists("dead_letter") else 0
p3.metric("DLQ Files", dlq_files)

# =============================================
# AUTO-REFRESH
# =============================================
time.sleep(3)
st.rerun()
