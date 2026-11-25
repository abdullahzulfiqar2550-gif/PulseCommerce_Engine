# PulseCommerce Engine

**Distributed Real-Time Order Processing Pipeline for E-Commerce Flash Sales**

Student: Muhammad Abdullah (2025-ai-348)

---

## What This Project Does

During flash sales (like Daraz 11.11), thousands of orders come at once. If you save each order directly to the database, the system crashes. This project solves that problem using a **queue-based pipeline**:

```
PRODUCER  -->  REDIS QUEUE  -->  WORKERS (1-4)  -->  POSTGRESQL DATABASE
(generates      (holds orders    (pick up and        (saves orders
 orders)         in FIFO line)    compete for work)   permanently)
```

**6 Pipeline Stages:**
1. **INGEST** - Producer reads from DummyJSON Live API + Kaggle CSV (118K orders)
2. **STAGING** - Raw data saved to staging_inbox/ folder (audit trail)
3. **QUEUE** - Orders pushed into Redis FIFO queue (RPUSH)
4. **WORKERS** - 1-4 workers compete for orders (BLPOP) with in-flight lease
5. **DLQ** - Bad orders (negative price) quarantined to Dead-Letter Queue
6. **DATABASE** - Valid orders saved to PostgreSQL (ON CONFLICT DO NOTHING)

---

## Project Structure

```
PulseCommerce_Engine/
|-- docker-compose.yml          # Starts Redis + PostgreSQL in Docker
|-- requirements.txt            # Python packages needed
|-- .env                        # Environment variables (not in git)
|-- README.md                   # This file
|-- postman_collection.json     # Import into Postman for API testing
|-- data/                       # Kaggle CSV files (7 files)
|   |-- olist_orders_dataset.csv
|   |-- olist_order_payments_dataset.csv
|   |-- olist_customers_dataset.csv
|   |-- olist_order_items_dataset.csv
|   |-- olist_products_dataset.csv
|   |-- olist_sellers_dataset.csv
|   |-- product_category_name_translation.csv
|-- staging_inbox/              # Raw order files land here (audit trail)
|-- dead_letter/                # Corrupt orders quarantined here
|-- src/
|   |-- init_db.py              # Creates database tables (run once)
|   |-- producer.py             # Generates orders -> pushes to Redis
|   |-- worker.py               # Pulls from Redis -> saves to PostgreSQL
|   |-- api.py                  # Postman API -> pushes to Redis
|-- dashboard/
|   |-- app.py                  # Streamlit live dashboard
```

---

## Tech Stack

| Tool           | Purpose                             |
|----------------|-------------------------------------|
| Python         | Main programming language           |
| Redis 7        | In-memory FIFO message queue        |
| PostgreSQL 16  | Relational database (ACID)          |
| FastAPI        | REST API for Postman webhook        |
| Pandas         | Merge 7 Kaggle CSV files            |
| Docker Compose | Run Redis + PostgreSQL containers   |
| Streamlit      | Real-time interactive dashboard     |

---

## Data Sources

| Source | Type | Description |
|--------|------|-------------|
| **DummyJSON API** | Live REST API | Free API returning real products (194 items) with name, price, brand, category, rating, stock. No API key needed. |
| **Kaggle CSV** | Batch Data | Olist Brazilian E-Commerce dataset — 7 CSV files merged into 118,431 orders with payments, customers, items, products, sellers, categories. |
| **Postman API** | Manual Input | FastAPI endpoint accepting POST /order from Postman. |

All prices are in **USD**.

---

## Database Schema (18 Columns)

```sql
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
```

---

## How to Run (Step by Step)

### Prerequisites
- Docker Desktop installed and running
- Python 3.11+ with pip
- Postman (for API testing)

### Step 1: Start Docker Containers
```
docker compose up -d
```
This starts Redis (port 6379) and PostgreSQL (port 5433).

### Step 2: Install Python Packages
```
pip install -r requirements.txt
```

### Step 3: Create Database Tables (one time only)
```
python src/init_db.py
```

### Step 4: Start the Dashboard
```
streamlit run dashboard/app.py
```
Opens at http://localhost:8501 — Dashboard has Start/Stop buttons for all workers and producer.

### Step 5: Start Producer and Workers

**Option A: Use Dashboard Buttons (Recommended)**
- Click "Start Worker-1" through "Start Worker-4" in the dashboard
- Click "Start Producer" to begin generating orders

**Option B: Use Terminal (Manual)**
```
# Terminal 1 - Producer
python src/producer.py

# Terminal 2-5 - Workers (open 4 terminals)
python src/worker.py 1
python src/worker.py 2
python src/worker.py 3
python src/worker.py 4
```

### Step 6: Simulate Flash Sale
- In the dashboard, click **"Burst: 8 Orders (Flash Sale)"** to inject 8 orders at once
- Watch workers compete to process them in real-time

### Step 7: Test Dead-Letter Queue
- Click **"Send Bad Order (DLQ Test)"** in the dashboard
- Worker will quarantine it to the Dead-Letter Queue
- DLQ count will increase in the Live Metrics

### Step 8: Test with Postman
- Start the API server: `python src/api.py`
- Import `postman_collection.json` into Postman
- Send POST request to `http://localhost:8000/order`:
```json
{
    "customer_name": "Bilal Ahmed",
    "product_name": "Samsung Galaxy S24",
    "product_category": "smartphones",
    "brand": "Samsung",
    "unit_price": 899.99,
    "shipping_cost": 12.50,
    "total_amount": 912.49,
    "items_count": 1,
    "product_weight_g": 234.0,
    "payment_method": "Credit Card",
    "rating": 4.8,
    "stock": 50
}
```

---

## How Each File Works

### producer.py
1. Connects to Redis queue
2. Loads 7 Kaggle CSV files and merges them (118,431 rows)
3. Alternates between DummyJSON Live API (odd orders) and Kaggle CSV (even orders)
4. 5% chance: injects bad order (negative price) for DLQ testing
5. Saves raw order to staging_inbox/ as audit file
6. Pushes order JSON into Redis queue using RPUSH
7. Waits 2 seconds, then repeats

### worker.py (run 1-4 instances)
1. On startup, checks for crashed in-flight orders and recovers them
2. Sends heartbeat to Redis every 10 seconds
3. Uses BLPOP to wait for orders in Redis queue
4. Sets in-flight lease (crash recovery)
5. If price <= 0: sends to Dead-Letter Queue + saves JSON file
6. If price > 0: inserts into PostgreSQL with ON CONFLICT DO NOTHING
7. Logs processing time to pipeline_metrics table
8. Deletes in-flight lease after successful save

### api.py
1. FastAPI server running on port 8000
2. POST /order receives JSON from Postman
3. Generates unique order_id (POST-XXXXXXXX)
4. Pushes the order into the same Redis queue
5. Worker picks it up automatically

### dashboard/app.py
1. Streamlit dashboard on port 8501
2. Shows 6 pipeline stages
3. Start/Stop buttons for 4 workers + producer
4. Flash Sale Burst button (8 orders at once)
5. Send Bad Order button (DLQ test)
6. Live metrics: orders, revenue, queue, DLQ, speed
7. Charts: Orders by Source + Orders by Category
8. Recent Orders table (16 visible columns)
9. Worker Performance table
10. Kaggle Dataset Explorer (9 CSV files)
11. Auto-refreshes every 3 seconds

---

## Key Concepts for Viva

| Concept | Explanation |
|---------|-------------|
| FIFO Queue | First In, First Out — orders processed in order they arrive |
| RPUSH / BLPOP | Redis commands to add (right) and remove (left-blocking) from queue |
| In-Flight Lease | Redis key that tracks which order a worker is processing; if worker crashes, order is recovered |
| Dead-Letter Queue | Separate queue for bad/corrupt orders that failed validation |
| ON CONFLICT DO NOTHING | PostgreSQL prevents duplicate inserts (idempotent) |
| Competing Consumers | Multiple workers pulling from same queue — load balancing |
| Staging Inbox | Raw files saved before processing — audit trail for replay |
| Pipeline Metrics | Processing time logged per order for performance monitoring |
| Flash Sale Burst | Simulates sudden spike of orders to test pipeline under load |

---

## Checking Results

**Check database rows:**
```
docker exec -it pulse_postgres psql -U pulse_admin -d pulsecommerce_db -c "SELECT COUNT(*) FROM ecommerce_orders;"
```

**Check Dead-Letter Queue count:**
```
docker exec -it pulse_redis redis-cli GET dlq_count
```

**Check queue length:**
```
docker exec -it pulse_redis redis-cli LLEN orders_queue
```

**Open Dashboard:**
```
streamlit run dashboard/app.py
```
