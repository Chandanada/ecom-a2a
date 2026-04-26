"""
Shopify MCP Server — fetches full order by ID after name lookup to get complete fields.
/orders.json returns partial address. /orders/{id}.json returns full address with name/city.
"""
import asyncio, os, json
from pathlib import Path
import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

SHOPIFY_STORE   = os.getenv("SHOPIFY_STORE", "agentic-ecom-demo")
SHOPIFY_TOKEN   = os.getenv("SHOPIFY_TOKEN", "")
SHOPIFY_API_VER = "2024-10"

LOG_FILE = Path(__file__).parent.parent / "shopify_debug.log"

def dbg(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def base_url():
    return f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{SHOPIFY_API_VER}"

def headers():
    return {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

async def rest_get(path: str, params: dict = None) -> dict:
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(f"{base_url()}{path}", headers=headers(), params=params or {})
        r.raise_for_status()
        return r.json()

async def rest_put(path: str, body: dict) -> dict:
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.put(f"{base_url()}{path}", headers=headers(), json=body)
        r.raise_for_status()
        return r.json()

async def rest_post(path: str, body: dict) -> dict:
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(f"{base_url()}{path}", headers=headers(), json=body)
        r.raise_for_status()
        return r.json()

async def get_order_by_name(order_num: str) -> dict | None:
    """
    Two-step fetch:
    1. /orders.json?name=#{num} → get the order ID
    2. /orders/{id}.json        → get FULL order with complete shipping_address, customer
    /orders.json returns partial address objects. /orders/{id}.json returns complete data.
    """
    num  = order_num.replace("#", "").strip()
    data = await rest_get("/orders.json", {"name": f"#{num}", "status": "any"})
    orders = data.get("orders", [])
    if not orders:
        data   = await rest_get("/orders.json", {"name": num, "status": "any"})
        orders = data.get("orders", [])
    if not orders:
        return None

    # Step 2: fetch full order by Shopify internal ID
    order_id   = orders[0]["id"]
    full_data  = await rest_get(f"/orders/{order_id}.json")
    full_order = full_data.get("order")
    return full_order if full_order else orders[0]

def _name_from_addr(addr) -> str:
    if not addr or not isinstance(addr, dict):
        return ""
    first = (addr.get("first_name") or "").strip()
    last  = (addr.get("last_name")  or "").strip()
    name  = (addr.get("name")       or "").strip()
    return f"{first} {last}".strip() or name

def _city_from_addr(addr) -> str:
    if not addr or not isinstance(addr, dict):
        return ""
    return (addr.get("city") or "").strip()

async def _extract_info(order: dict) -> dict:
    """
    Extract name/email/city.
    For imported orders Shopify stores name+city on customer profile, not on order.
    Fallback chain: order fields -> shipping_address -> customer profile API call.
    """
    cust = order.get("customer") or {}
    ship = order.get("shipping_address") or {}
    bill = order.get("billing_address")  or {}

    dbg(f"FULL customer: {json.dumps(cust)[:300]}")
    dbg(f"FULL shipping_address: {json.dumps(ship)[:300]}")
    dbg(f"order.email: {order.get("email")}")

    name = (
        _name_from_addr(cust) or
        _name_from_addr(ship) or
        _name_from_addr(bill)
    )
    email = (
        (cust.get("email") or "").strip() or
        (order.get("email") or "").strip()
    )
    phone = (
        (cust.get("phone") or "").strip() or
        (ship.get("phone") or "").strip() or
        (bill.get("phone") or "").strip()
    )
    city = (
        _city_from_addr(ship) or
        _city_from_addr(bill)
    )

    # If still missing name/city, fetch full customer profile
    cust_id = cust.get("id")
    if cust_id and (not name or not city or not email):
        try:
            cdata    = await rest_get(f"/customers/{cust_id}.json")
            cprofile = cdata.get("customer", {})
            dbg(f"CUSTOMER PROFILE: {json.dumps(cprofile)[:400]}")
            if not name:
                first = (cprofile.get("first_name") or "").strip()
                last  = (cprofile.get("last_name")  or "").strip()
                name  = f"{first} {last}".strip()
            if not email:
                email = (cprofile.get("email") or "").strip()
            if not phone:
                phone = (cprofile.get("phone") or "").strip()
            if not city:
                default_addr = cprofile.get("default_address") or {}
                city = (default_addr.get("city") or "").strip()
        except Exception as e:
            dbg(f"Customer profile fetch failed: {e}")

    name  = name  or "Unknown"
    city  = city  or ""
    dbg(f"EXTRACTED -> name={name} email={email} city={city}")
    return {"name": name, "email": email, "phone": phone, "city": city}


server = Server("shopify-mcp-server")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="get_order",
            description="Fetch complete order details from Shopify by order number (#1022). Returns customer, items, shipping address, fulfillment status.",
            inputSchema={"type":"object","properties":{"order_id":{"type":"string","description":"Order number e.g. 1022 or #1022"}},"required":["order_id"]}
        ),
        types.Tool(
            name="list_unfulfilled_orders",
            description="List all unfulfilled Shopify orders waiting for AWB generation.",
            inputSchema={"type":"object","properties":{"limit":{"type":"integer","default":10}},"required":[]}
        ),
        types.Tool(
            name="fulfill_order",
            description="Tag Shopify order with AWB tracking number and carrier. Shows AWB values in order Additional Details and Timeline.",
            inputSchema={"type":"object","properties":{
                "order_id":{"type":"string","description":"Order number e.g. 1022"},
                "awb":{"type":"string"},
                "carrier":{"type":"string"},
                "tracking_url":{"type":"string"}
            },"required":["order_id","awb","carrier"]}
        ),
        types.Tool(
            name="get_order_items",
            description="Get line items for a Shopify order.",
            inputSchema={"type":"object","properties":{"order_id":{"type":"string"}},"required":["order_id"]}
        ),
        types.Tool(
            name="create_test_order",
            description="Create a real fulfillable test order in Shopify for demo purposes.",
            inputSchema={"type":"object","properties":{
                "customer_name":{"type":"string","default":"Demo Customer"},
                "product_title":{"type":"string","default":"Demo T-Shirt"}
            },"required":[]}
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
  try:
    if name == "get_order":
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("=== get_order run ===\n")

        order = await get_order_by_name(arguments["order_id"])
        if not order:
            return [types.TextContent(type="text", text=json.dumps({"error": f"Order {arguments['order_id']} not found"}))]

        info = await _extract_info(order)

        result = {
            "id":                 order["name"],
            "shopify_id":         order["id"],
            "created_at":         order["created_at"],
            "fulfillment_status": order.get("fulfillment_status", "unfulfilled"),
            "financial_status":   order.get("financial_status"),
            "total":              order.get("total_price"),
            "currency":           order.get("currency"),
            "customer": {
                "name":  info["name"],
                "email": info["email"],
                "phone": info["phone"],
                "city":  info["city"],
            },
            "shipping_address": order.get("shipping_address") or {},
            "billing_address":  order.get("billing_address")  or {},
            "items": [
                {"name": li["name"], "qty": li["quantity"],
                 "sku": li.get("sku", ""), "price": li["price"]}
                for li in order.get("line_items", [])
            ],
            "fulfillments": [
                {"status": f["status"], "tracking": f.get("tracking_numbers", [])}
                for f in order.get("fulfillments", [])
            ]
        }
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

    elif name == "list_unfulfilled_orders":
        limit = arguments.get("limit", 10)
        data  = await rest_get("/orders.json", {"fulfillment_status": "unfulfilled", "status": "any", "limit": limit})
        orders = []
        for o in data.get("orders", []):
            # Fetch full order for each to get complete address
            try:
                full = (await rest_get(f"/orders/{o['id']}.json")).get("order", o)
            except Exception:
                full = o
            info = await _extract_info(full)
            orders.append({
                "id":         full["name"],
                "shopify_id": full["id"],
                "status":     full.get("fulfillment_status", "unfulfilled"),
                "total":      full.get("total_price"),
                "customer":   info["name"],
                "city":       info["city"],
                "email":      info["email"],
                "items":      [li["name"] for li in full.get("line_items", [])]
            })
        return [types.TextContent(type="text", text=json.dumps({"unfulfilled_orders": orders, "count": len(orders)}, indent=2))]

    elif name == "fulfill_order":
        order = await get_order_by_name(arguments["order_id"])
        if not order:
            return [types.TextContent(type="text", text=json.dumps({"error": f"Order {arguments['order_id']} not found"}))]

        shopify_id   = order["id"]
        awb          = arguments["awb"]
        carrier      = arguments["carrier"]
        tracking_url = arguments.get("tracking_url", f"https://www.delhivery.com/track/package/{awb}")

        update_body = {
            "order": {
                "id": shopify_id,
                "note": f"AWB: {awb} | Carrier: {carrier} | Track: {tracking_url}",
                "note_attributes": [
                    {"name": "AWB Number",   "value": awb},
                    {"name": "Carrier",      "value": carrier},
                    {"name": "Tracking URL", "value": tracking_url}
                ]
            }
        }

        result  = await rest_put(f"/orders/{shopify_id}.json", update_body)
        updated = result.get("order", {})

        if updated.get("id"):
            return [types.TextContent(type="text", text=json.dumps({
                "success":      True,
                "status":       "awb_tagged",
                "order_id":     order["name"],
                "shopify_id":   shopify_id,
                "awb":          awb,
                "carrier":      carrier,
                "tracking_url": tracking_url,
                "message":      f"AWB tagged on order {order['name']}. AWB: {awb} | Carrier: {carrier}"
            }, indent=2))]

        return [types.TextContent(type="text", text=json.dumps({
            "error": "Order update returned no order object", "raw": result
        }))]

    elif name == "get_order_items":
        order = await get_order_by_name(arguments["order_id"])
        if not order:
            return [types.TextContent(type="text", text=json.dumps({"error": f"Order {arguments['order_id']} not found"}))]
        items = [
            {"name": li["name"], "sku": li.get("sku", ""), "qty": li["quantity"], "price": li["price"]}
            for li in order.get("line_items", [])
        ]
        return [types.TextContent(type="text", text=json.dumps({
            "order_id": order["name"], "items": items, "total": order.get("total_price")
        }, indent=2))]

    elif name == "create_test_order":
        cname = arguments.get("customer_name", "Demo Customer")
        parts = cname.split(" ", 1)
        draft_body = {
            "draft_order": {
                "line_items": [{
                    "title": arguments.get("product_title", "Demo T-Shirt"),
                    "price": "999.00", "quantity": 1, "requires_shipping": True
                }],
                "customer": {
                    "first_name": parts[0],
                    "last_name":  parts[1] if len(parts) > 1 else "Customer",
                    "email":      "demo@agentic-ecom.com"
                },
                "shipping_address": {
                    "first_name": parts[0],
                    "last_name":  parts[1] if len(parts) > 1 else "Customer",
                    "address1":   "42 MG Road", "city": "Bangalore",
                    "province":   "Karnataka",  "zip": "560001",
                    "country":    "IN",          "phone": "+91-9876543210"
                }
            }
        }
        draft    = await rest_post("/draft_orders.json", draft_body)
        draft_id = draft.get("draft_order", {}).get("id")
        if not draft_id:
            return [types.TextContent(type="text", text=json.dumps({"error": f"Draft creation failed: {draft}"}))]
        completed  = await rest_post(f"/draft_orders/{draft_id}/complete.json", {"payment_pending": False})
        o          = completed.get("draft_order", {})
        order_id   = o.get("order_id")
        order_name = "Unknown"
        if order_id:
            od         = await rest_get(f"/orders/{order_id}.json")
            order_name = od.get("order", {}).get("name", f"#{order_id}")
        return [types.TextContent(type="text", text=json.dumps({
            "success":    True,
            "order_id":   order_name,
            "shopify_id": order_id,
            "status":     "unfulfilled",
            "message":    f"Created order {order_name}"
        }, indent=2))]

    return [types.TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]

  except Exception as e:
    import traceback
    return [types.TextContent(type="text", text=json.dumps({
        "error":     str(e)[:500],
        "traceback": traceback.format_exc()[:500]
    }))]


async def main():
    async with stdio_server() as (r, w):
        await server.run(r, w, server.create_initialization_options())

if __name__ == "__main__":
    asyncio.run(main())
