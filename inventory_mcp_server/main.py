"""
Inventory MCP Server — HTTP-based MCP server (NOT stdio).
Exposes inventory tools via REST endpoints.
Agents discover and call tools via HTTP — works on public Render URL.

Endpoints:
  GET  /tools/list         → list available tools
  POST /tools/call         → call a tool by name
  GET  /.well-known/mcp    → MCP server metadata
  GET  /health             → health check
  GET  /inventory          → read current Airtable inventory
"""
import os, json
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI(title="Inventory MCP Server", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

AIRTABLE_TOKEN    = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID  = os.getenv("AIRTABLE_BASE_ID", "appwYcZ3Iw5fdEdWX")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "tbl7AkKbiFVaGZNIs")

AIRTABLE_BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"

def airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }

# ── Tool definitions ───────────────────────────────────────────────────────────
TOOLS = [
    {
        "name": "inventory_refilled",
        "description": "Restock inventory when a returned item is received back. Updates Airtable with new stock count.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sku":           {"type": "string", "description": "Product SKU e.g. LAPTOP-BAG-15"},
                "product_name":  {"type": "string", "description": "Product display name"},
                "qty":           {"type": "integer", "description": "Quantity to restock"},
                "order_id":      {"type": "string", "description": "Source order ID for this return"},
                "return_reason": {"type": "string", "description": "Reason for return"}
            },
            "required": ["sku", "product_name", "qty", "order_id"]
        }
    },
    {
        "name": "get_stock_level",
        "description": "Get current stock level for a SKU from Airtable inventory.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string", "description": "Product SKU to check"}
            },
            "required": ["sku"]
        }
    },
    {
        "name": "list_inventory",
        "description": "List all SKUs and their current stock levels from Airtable.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]


@app.get("/health")
def health():
    return {"status": "ok", "service": "inventory_mcp_server"}


@app.get("/.well-known/mcp")
def mcp_metadata():
    return {
        "name":        "Inventory MCP Server",
        "description": "HTTP MCP server for inventory management via Airtable",
        "version":     "1.0.0",
        "tools_url":   "/tools/list",
        "call_url":    "/tools/call"
    }


@app.get("/tools/list")
def list_tools():
    return {"tools": TOOLS}


@app.post("/tools/call")
async def call_tool(body: dict):
    name      = body.get("name")
    arguments = body.get("arguments", {})

    if name == "inventory_refilled":
        return await _inventory_refilled(arguments)
    elif name == "get_stock_level":
        return await _get_stock_level(arguments)
    elif name == "list_inventory":
        return await _list_inventory()
    else:
        raise HTTPException(status_code=404, detail=f"Unknown tool: {name}")


@app.get("/inventory")
async def get_inventory():
    """Direct endpoint to read inventory — used by dashboard."""
    result = await _list_inventory()
    return result


# ── Tool implementations ───────────────────────────────────────────────────────

async def _find_record_by_sku(sku: str) -> dict | None:
    """Search Airtable for existing record with this SKU."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            AIRTABLE_BASE_URL,
            headers=airtable_headers(),
            params={"filterByFormula": f"{{SKU}}='{sku}'"}
        )
        r.raise_for_status()
        records = r.json().get("records", [])
        return records[0] if records else None


async def _inventory_refilled(args: dict) -> dict:
    sku           = args["sku"]
    product_name  = args["product_name"]
    qty           = int(args.get("qty", 1))
    order_id      = args["order_id"]
    return_reason = args.get("return_reason", "customer_return")
    now           = datetime.now(timezone.utc).isoformat()

    existing = await _find_record_by_sku(sku)

    async with httpx.AsyncClient(timeout=15.0) as client:
        if existing:
            # Update existing record — add qty to current stock
            record_id    = existing["id"]
            current_stock = existing["fields"].get("Stock", 0)
            new_stock     = current_stock + qty

            r = await client.patch(
                f"{AIRTABLE_BASE_URL}/{record_id}",
                headers=airtable_headers(),
                json={"fields": {
                    "Stock":         new_stock,
                    "Last Updated":  now[:10],
                    "Return Reason": f"{return_reason} (Order: {order_id})"
                }}
            )
            r.raise_for_status()
            record = r.json()
        else:
            # Create new record
            r = await client.post(
                AIRTABLE_BASE_URL,
                headers=airtable_headers(),
                json={"fields": {
                    "SKU":           sku,
                    "Product Name":  product_name,
                    "Stock":         qty,
                    "Last Updated":  now[:10],
                    "Return Reason": f"{return_reason} (Order: {order_id})"
                }}
            )
            r.raise_for_status()
            record        = r.json()
            current_stock = 0
            new_stock     = qty

    return {
        "success":        True,
        "tool":           "inventory_refilled",
        "sku":            sku,
        "product_name":   product_name,
        "previous_stock": current_stock if existing else 0,
        "new_stock":      new_stock,
        "qty_added":      qty,
        "order_id":       order_id,
        "return_reason":  return_reason,
        "airtable_record_id": record.get("id"),
        "message": f"Inventory restocked: {sku} +{qty} units. Stock: {new_stock}"
    }


async def _get_stock_level(args: dict) -> dict:
    sku      = args["sku"]
    existing = await _find_record_by_sku(sku)
    if existing:
        return {
            "sku":          sku,
            "product_name": existing["fields"].get("Product Name", ""),
            "stock":        existing["fields"].get("Stock", 0),
            "last_updated": existing["fields"].get("Last Updated", "")
        }
    return {"sku": sku, "stock": 0, "message": "SKU not found in inventory"}


async def _list_inventory() -> dict:
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(AIRTABLE_BASE_URL, headers=airtable_headers())
        r.raise_for_status()
        records = r.json().get("records", [])

    items = []
    for rec in records:
        f = rec.get("fields", {})
        items.append({
            "sku":          f.get("SKU", ""),
            "product_name": f.get("Product Name", ""),
            "stock":        f.get("Stock", 0),
            "last_updated": f.get("Last Updated", ""),
            "return_reason": f.get("Return Reason", "")
        })
    return {"inventory": items, "count": len(items)}


@app.delete("/inventory/reset")
async def reset_inventory():
    """Delete all Airtable records - called on order_service startup to sync with reset orders."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(AIRTABLE_BASE_URL, headers=airtable_headers())
        r.raise_for_status()
        records = r.json().get("records", [])
        deleted = []
        for rec in records:
            rid = rec["id"]
            dr  = await client.delete(f"{AIRTABLE_BASE_URL}/{rid}", headers=airtable_headers())
            if dr.status_code == 200:
                deleted.append(rid)
    return {"success": True, "deleted": len(deleted), "message": "Airtable inventory cleared"}
