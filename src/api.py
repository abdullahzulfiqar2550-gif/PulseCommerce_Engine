# =============================================
# PulseCommerce Engine - API (Postman Webhook)
# Run: python src/api.py
# Open: http://localhost:8000/docs
# =============================================
# This script creates a web API so you can
# send orders from Postman and they go into
# the same Redis Queue as the Producer.
# =============================================

import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
import json
import uuid
import redis
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv

# Load .env file (reads all values from .env)
load_dotenv()

# --- Step 1: Connect to Redis (settings from .env) ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
QUEUE_NAME = "orders_queue"

# --- Step 2: Create FastAPI App ---
app = FastAPI(title="PulseCommerce Live API")


# --- Step 3: Define what an Order looks like (matches 18-column schema) ---
class Order(BaseModel):
    customer_name: str
    product_name: str = "Custom Product"
    product_category: str = "general"
    brand: str = "N/A"
    unit_price: float = 0.0
    shipping_cost: float = 5.0
    total_amount: float
    items_count: int = 1
    product_weight_g: float = 0.0
    payment_method: str = "Credit Card"
    rating: float = 0.0
    stock: int = 0


# --- Step 4: Home Page (GET /) ---
@app.get("/")
def home():
    return {
        "status": "Running",
        "message": "Send a POST request to /order using Postman",
        "docs": "http://localhost:8000/docs"
    }


# --- Step 5: Health Check (GET /health) ---
@app.get("/health")
def health_check():
    queue_length = redis_client.llen(QUEUE_NAME)
    return {
        "status": "Healthy",
        "redis": "Connected",
        "queue_length": queue_length
    }


# --- Step 6: Receive Order (POST /order) ---
@app.post("/order")
def receive_order(order: Order):
    """
    When you click 'Send' in Postman:
    1. Postman sends order data here
    2. This pushes the order into Redis queue
    3. Worker picks it up and saves to database
    """
    order_dict = order.model_dump()
    order_dict["order_id"] = "POST-" + uuid.uuid4().hex[:8].upper()
    order_dict["currency"] = "USD"
    order_dict["source"] = "Postman Live API"

    # Push to Redis Queue
    redis_client.rpush(QUEUE_NAME, json.dumps(order_dict))

    print("[API] Received order from", order.customer_name, "| $" + str(order.total_amount), "-> Pushed to Redis")

    return {
        "status": "ACCEPTED",
        "message": "Order received and sent to Redis queue!",
        "order": order_dict
    }


# --- Run the server ---
if __name__ == "__main__":
    import uvicorn
    print("")
    print("API Server started!")
    print("Open in browser: http://localhost:8000/docs")
    print("Or use Postman:  POST http://localhost:8000/order")
    print("")
    uvicorn.run(app, host="0.0.0.0", port=8000)
