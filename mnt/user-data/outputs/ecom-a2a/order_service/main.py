"""
Order Service — Ecommerce Backend
Shopify-compatible API structure.
Swap ORDERS dict with Shopify Admin API calls when real store is ready.
"""
import os, uuid
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="Ecom Order Service", version="1.0.0")

# In-memory store — replace with Shopify API or real DB
ORDERS = {
    "ORD-001": {
        "id": "ORD-001",
        "customer": {"name": "Rahul Sharma", "email": "rahul@example.com", "phone": "+91-9876543210"},
        "items": [
            {"sku": "TSHIRT-BLK-M", "name": "Black T-Shirt Medium", "qty": 2, "price": 599},
            {"sku": "JEANS-BLU-32",  "name": "Blue Jeans 32",        "qty": 1, "price": 1299}
        ],
        "shipping_address": {
            "name":    "Rahul Sharma",
            "line1":   "42 MG Road",
            "city":    "Bangalore",
            "state":   "Karnataka",
            "pincode": "560001",
            "country": "IN"
        },
        "total":          2497,
        "status":         "confirmed",
        "awb":            None,
        "carrier":        None,
        "tracking_url":   None,
        "created_at":     "2026-04-24T09:00:00Z",
        "updated_at":     "2026-04-24T09:00:00Z"
    },
    "ORD-002": {
        "id": "ORD-002",
        "customer": {"name": "Priya Nair", "email": "priya@example.com", "phone": "+91-8765432109"},
        "items": [
            {"sku": "SHOE-WHT-8", "name": "White Sneakers Size 8", "qty": 1, "price": 2499}
        ],
        "shipping_address": {
            "name":    "Priya Nair",
            "line1":   "15 Linking Road",
            "city":    "Mumbai",
            "state":   "Maharashtra",
            "pincode": "400050",
            "country": "IN"
        },
        "total":          2499,
        "status":         "confirmed",
        "awb":            None,
        "carrier":        None,
        "tracking_url":   None,
        "created_at":     "2026-04-24T10:30:00Z",
        "updated_at":     "2026-04-24T10:30:00Z"
    }
}


@app.get("/health")
def health():
    return {"status": "ok", "service": "order_service"}


@app.get("/orders")
def list_orders():
    return {"orders": list(ORDERS.values()), "count": len(ORDERS)}


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    if order_id not in ORDERS:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return ORDERS[order_id]


@app.post("/orders/{order_id}/fulfill")
def fulfill_order(order_id: str, payload: dict):
    """Update order with AWB number — called by client agent after logistics agent responds."""
    if order_id not in ORDERS:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    awb         = payload.get("awb")
    carrier     = payload.get("carrier", "Shiprocket")
    tracking_url = payload.get("tracking_url", f"https://shiprocket.co/tracking/{awb}")

    if not awb:
        raise HTTPException(status_code=400, detail="awb required")

    ORDERS[order_id]["status"]       = "shipped"
    ORDERS[order_id]["awb"]          = awb
    ORDERS[order_id]["carrier"]      = carrier
    ORDERS[order_id]["tracking_url"] = tracking_url
    ORDERS[order_id]["updated_at"]   = datetime.utcnow().isoformat() + "Z"

    return {
        "success":       True,
        "order_id":      order_id,
        "status":        "shipped",
        "awb":           awb,
        "carrier":       carrier,
        "tracking_url":  tracking_url,
        "message":       f"Order {order_id} fulfilled. AWB: {awb}"
    }


@app.post("/orders")
def create_order(payload: dict):
    """Create a new order — simulates order placement from ecom site."""
    order_id = f"ORD-{uuid.uuid4().hex[:6].upper()}"
    order = {
        "id":               order_id,
        "customer":         payload.get("customer", {}),
        "items":            payload.get("items", []),
        "shipping_address": payload.get("shipping_address", {}),
        "total":            payload.get("total", 0),
        "status":           "confirmed",
        "awb":              None,
        "carrier":          None,
        "tracking_url":     None,
        "created_at":       datetime.utcnow().isoformat() + "Z",
        "updated_at":       datetime.utcnow().isoformat() + "Z"
    }
    ORDERS[order_id] = order
    return order
