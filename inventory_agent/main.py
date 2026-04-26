"""
Inventory Agent — A2A Remote Agent
Receives restock requests, discovers tools via HTTP MCP server, calls inventory_refilled.
This is real MCP — tool discovery at runtime via HTTP, not hardcoded.
"""
import os, uuid
from datetime import datetime, timezone
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI(title="Inventory Agent", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

AGENT_BASE_URL         = os.getenv("AGENT_BASE_URL", "https://ecom-inventory-agent.onrender.com")
INVENTORY_MCP_URL      = os.getenv("INVENTORY_MCP_URL", "https://ecom-inventory-mcp.onrender.com")

AGENT_CARD = {
    "name":        "Inventory Agent",
    "description": "Restocks inventory when items are returned. Discovers and calls tools via HTTP MCP server backed by Airtable.",
    "supportedInterfaces": [{
        "url":             AGENT_BASE_URL,
        "protocolBinding": "JSONRPC",
        "protocolVersion": "1.0"
    }],
    "provider":  {"organization": "Ecom A2A Platform"},
    "version":   "1.0.0",
    "capabilities": {
        "streaming": False, "pushNotifications": False,
        "stateTransitionHistory": True, "extendedAgentCard": False
    },
    "defaultInputModes":  ["application/json"],
    "defaultOutputModes": ["application/json"],
    "skills": [{
        "id":          "restock_inventory",
        "name":        "Restock Inventory",
        "description": "Discovers inventory tools via MCP and restocks Airtable when items are returned.",
        "tags":        ["inventory", "restock", "mcp", "airtable"],
        "examples":    ["Restock inventory for returned order ORD-005"],
        "inputModes":  ["application/json"],
        "outputModes": ["application/json"]
    }],
    "signatures": None, "securitySchemes": None, "security": None
}


@app.get("/.well-known/agent-card.json")
def agent_card():
    return JSONResponse(AGENT_CARD)


@app.get("/health")
def health():
    return {"status": "ok", "service": "inventory_agent"}


@app.post("/")
async def handle_message(request: Request):
    try:
        body   = await request.json()
        req_id = body.get("id", "req-001")

        if body.get("method") != "SendMessage":
            return JSONResponse({
                "jsonrpc": "2.0", "id": req_id,
                "error": {"code": -32601, "message": "Method not found"}
            })

        parts         = body.get("params", {}).get("message", {}).get("parts", [])
        data          = next((p.get("data") for p in parts if "data" in p), {})
        order         = data.get("order", data)
        return_reason = data.get("return_reason", "customer_return")

        order_id = order.get("id", "UNKNOWN")
        items    = order.get("items", [])

        # ── Step 1: Discover tools via HTTP MCP server ────────────
        # Real MCP: agent asks "what tools exist?" at runtime
        async with httpx.AsyncClient(timeout=15.0) as client:
            tools_r    = await client.get(f"{INVENTORY_MCP_URL}/tools/list")
            tools_data = tools_r.json()

        tools      = tools_data.get("tools", [])
        tool_names = [t["name"] for t in tools]

        # Verify inventory_refilled tool is available
        if "inventory_refilled" not in tool_names:
            raise Exception(f"inventory_refilled tool not found. Available: {tool_names}")

        # ── Step 2: Call inventory_refilled for each item ─────────
        restock_results = []
        for item in items:
            sku          = item.get("sku", "UNKNOWN-SKU")
            product_name = item.get("name", "Unknown Product")
            qty          = item.get("qty", 1)

            if not sku or sku == "UNKNOWN-SKU":
                continue

            async with httpx.AsyncClient(timeout=15.0) as client:
                call_r = await client.post(
                    f"{INVENTORY_MCP_URL}/tools/call",
                    json={
                        "name": "inventory_refilled",
                        "arguments": {
                            "sku":           sku,
                            "product_name":  product_name,
                            "qty":           qty,
                            "order_id":      order_id,
                            "return_reason": return_reason
                        }
                    }
                )
                call_r.raise_for_status()
                restock_result = call_r.json()
                restock_results.append(restock_result)

        result = {
            "success":         True,
            "order_id":        order_id,
            "status":          "restocked",
            "mcp_server":      INVENTORY_MCP_URL,
            "tools_discovered": tool_names,
            "tool_called":     "inventory_refilled",
            "items_restocked": restock_results,
            "restocked_at":    datetime.now(timezone.utc).isoformat(),
            "message":         f"Inventory restocked for {len(restock_results)} item(s) from order {order_id} via MCP"
        }

        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "result": {"task": {
                "id": str(uuid.uuid4()), "contextId": str(uuid.uuid4()),
                "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{"artifactId": str(uuid.uuid4()), "name": "inventory_result",
                    "parts": [{"kind": "data", "data": result, "mediaType": "application/json"}]
                }]
            }}
        })

    except Exception as e:
        return JSONResponse({
            "jsonrpc": "2.0", "id": "req-001",
            "error": {"code": -32000, "message": str(e)[:300]}
        })
