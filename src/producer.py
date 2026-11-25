# =============================================
# PulseCommerce Engine - Producer (Order Generator)
# Run: python src/producer.py
# =============================================
# DUAL-SOURCE INGESTION:
#   Source 1: DummyJSON Live E-Commerce REST API
#             (real products, prices, categories)
#   Source 2: Kaggle CSV batch data
#             (historical orders from Olist dataset)
#
# Alternates between both sources every order.
# =============================================

import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
import json
import time
import random
import uuid
import pandas as pd
import redis
import urllib.request
from dotenv import load_dotenv

load_dotenv()

# --- Step 1: Connect to Redis ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
QUEUE_NAME = "orders_queue"

# --- Step 2: DummyJSON Live E-Commerce API (no key needed) ---
API_BASE = os.getenv("ECOMMERCE_API", "https://dummyjson.com")

os.makedirs("staging_inbox", exist_ok=True)


def fetch_live_api_order():
    """Fetch a REAL product from DummyJSON Live E-Commerce API.
    This is a free REST API that returns real product data:
    name, price, category, brand, etc."""
    try:
        # Pick a random product (DummyJSON has 194 products)
        product_id = random.randint(1, 194)
        url = API_BASE + "/products/" + str(product_id)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=5)
        product = json.loads(resp.read().decode("utf-8"))

        order = {
            "order_id": "API-" + uuid.uuid4().hex[:8].upper(),
            "customer_name": random.choice(NAMES),
            "product_name": product.get("title", "Unknown Product"),
            "product_category": product.get("category", "general"),
            "brand": product.get("brand", "N/A"),
            "unit_price": float(product.get("price", 0)),
            "shipping_cost": round(random.uniform(2.0, 15.0), 2),
            "total_amount": float(product.get("price", 0)),
            "currency": "USD",
            "items_count": random.randint(1, 3),
            "product_weight_g": float(product.get("weight", 0)) * 1000,
            "payment_method": random.choice(["Credit Card", "JazzCash", "EasyPaisa", "COD"]),
            "rating": float(product.get("rating", 0)),
            "stock": int(product.get("stock", 0)),
            "source": "DummyJSON Live API"
        }
        return order
    except Exception as e:
        print("[PRODUCER] API Error:", e, "-> Falling back to CSV")
        return None


def load_kaggle_data():
    """Merge 7 Kaggle CSV files for rich batch order data."""
    files = {
        "orders": "data/olist_orders_dataset.csv",
        "payments": "data/olist_order_payments_dataset.csv",
        "customers": "data/olist_customers_dataset.csv",
        "items": "data/olist_order_items_dataset.csv",
        "products": "data/olist_products_dataset.csv",
        "categories": "data/product_category_name_translation.csv",
        "sellers": "data/olist_sellers_dataset.csv"
    }

    for name, path in files.items():
        if not os.path.exists(path):
            print("WARNING: Missing", path)
            return None

    print("Loading 7 Kaggle CSV files...")
    orders = pd.read_csv(files["orders"])
    payments = pd.read_csv(files["payments"])
    customers = pd.read_csv(files["customers"])
    items = pd.read_csv(files["items"])
    products = pd.read_csv(files["products"])
    categories = pd.read_csv(files["categories"])
    sellers = pd.read_csv(files["sellers"])

    # Merge product categories (Portuguese -> English)
    products = pd.merge(products, categories, on="product_category_name", how="left")

    # Chain merge: orders -> payments -> customers -> items -> products -> sellers
    merged = pd.merge(orders, payments, on="order_id", how="inner")
    merged = pd.merge(merged, customers, on="customer_id", how="left")
    merged = pd.merge(merged, items, on="order_id", how="left")
    merged = pd.merge(merged, products, on="product_id", how="left")
    merged = pd.merge(merged, sellers, on="seller_id", how="left")

    # Fill missing values
    merged["product_category_name_english"] = merged["product_category_name_english"].fillna("general")
    merged["customer_city"] = merged["customer_city"].fillna("unknown")
    merged["customer_state"] = merged["customer_state"].fillna("XX")
    merged["seller_city"] = merged["seller_city"].fillna("unknown")
    merged["freight_value"] = merged["freight_value"].fillna(0)
    merged["price"] = merged["price"].fillna(0)
    merged["product_weight_g"] = merged["product_weight_g"].fillna(0)
    merged = merged.dropna(subset=["payment_value"])

    print("Merged", len(merged), "orders from 7 CSV files.")
    return merged


def make_csv_order(row):
    """Build an order dictionary from a Kaggle CSV row."""
    return {
        "order_id": str(row["order_id"])[:12],
        "customer_name": random.choice(NAMES),
        "product_name": str(row.get("product_category_name_english", "general")).title(),
        "product_category": str(row.get("product_category_name_english", "general")),
        "brand": "Olist Marketplace",
        "unit_price": round(float(row.get("price", 0)), 2),
        "shipping_cost": round(float(row.get("freight_value", 0)), 2),
        "total_amount": round(float(row.get("payment_value", 0)), 2),
        "currency": "USD",
        "items_count": random.randint(1, 3),
        "product_weight_g": float(row.get("product_weight_g", 0)),
        "payment_method": str(row.get("payment_type", "credit_card")).replace("_", " ").title(),
        "rating": 0,
        "stock": 0,
        "source": "Kaggle CSV Batch"
    }


def save_to_staging(order, batch_number):
    """Save raw order to staging_inbox/ as audit file."""
    filename = "staging_inbox/batch_" + str(batch_number) + ".json"
    with open(filename, "w") as file:
        json.dump(order, file, indent=2)
    return filename


# Customer names for simulation
NAMES = [
    "Bilal Ahmed", "Hamza Sheikh", "Ayesha Malik",
    "Fatima Zahra", "Usman Tariq", "Sara Khan",
    "Ali Raza", "Zainab Hussain", "Omar Farooq", "Hira Noor"
]


# --- Main Producer Loop ---
def run_producer():
    print("==================================================")
    print("PULSECOMMERCE PRODUCER STARTED")
    print("Source 1: DummyJSON Live E-Commerce API")
    print("Source 2: Kaggle CSV (7 files, 118K orders)")
    print("Press Ctrl+C to stop.")
    print("==================================================")

    kaggle_data = load_kaggle_data()
    print("")

    count = 1

    while True:
        # ALTERNATE between Live API and CSV
        if count % 2 == 1:
            # ODD orders: from DummyJSON Live API
            order = fetch_live_api_order()
            if order is None and kaggle_data is not None:
                row = kaggle_data.sample(1).iloc[0]
                order = make_csv_order(row)
        else:
            # EVEN orders: from Kaggle CSV
            if kaggle_data is not None and len(kaggle_data) > 0:
                row = kaggle_data.sample(1).iloc[0]
                order = make_csv_order(row)
            else:
                order = fetch_live_api_order()

        if order is None:
            print("[PRODUCER] No data source available!")
            time.sleep(2)
            continue

        # 5% chance: inject BAD order for DLQ testing
        if random.random() < 0.05:
            order["total_amount"] = -100.0

        # Save to staging inbox
        staging_file = save_to_staging(order, count)

        # Push to Redis Queue
        redis_client.rpush(QUEUE_NAME, json.dumps(order))

        # Print
        if order["total_amount"] < 0:
            print("[PRODUCER] BAD #" + str(count) + ":", order["order_id"], "|", order["source"], "-> DLQ Test")
        else:
            print("[PRODUCER] #" + str(count) + ":", order["order_id"], "|",
                  order["product_name"][:30], "| $" + str(order["total_amount"]),
                  "|", order["source"])

        count = count + 1
        time.sleep(2)


if __name__ == "__main__":
    run_producer()
