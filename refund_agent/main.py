"""
Refund Agent — A2A Remote Agent
Processes refunds for returned orders. Calculates refund amount, triggers inventory restock via Inventory Agent.
"""
import os, uuid
from datetime import datetime, timezone
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI(title="Refund Agent", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

AGENT_BASE_URL      = os.getenv("AGENT_BASE_URL", "https://ecom-refund-agent.onrender.com")
INVENTORY_AGENT_URL = os.getenv("INVENTORY_AGENT_URL", "https://ecom-inventory-agent.onrender.com")

AGENT_CARD = {
    "name":        "Refund Agent",
    "description": "Processes refunds for returned ecommerce orders. Calculates refund amount and triggers inventory restock.",
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
        "id":          "process_refund",
        "name":        "Process Refund",
        "description": "Calculate and process refund. Triggers inventory restock via Inventory Agent.",
        "tags":        ["refund", "payment", "inventory"],
        "examples":    ["Process refund for ORD-005"],
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
    return {"status": "ok", "service": "refund_agent"}


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

        parts  = body.get("params", {}).get("message", {}).get("parts", [])
        data   = next((p.get("data") for p in parts if "data" in p), {})
        order  = data.get("order", data)

        order_id      = order.get("id", "UNKNOWN")
        total         = float(order.get("total", 0))
        items         = order.get("items", [])
        return_reason = data.get("return_reason", "customer_request")

        # ── Step 1: Calculate refund ──────────────────────────────
        # Full refund for returns (can add partial logic here)
        refund_amount  = total
        refund_id      = f"REF-{uuid.uuid4().hex[:8].upper()}"
        refunded_at    = datetime.now(timezone.utc).isoformat()

        # ── Step 2: Trigger Inventory Agent via A2A ───────────────
        inventory_rid = f"req-{uuid.uuid4().hex[:8]}"

        inventory_payload = {
            "jsonrpc": "2.0", "id": inventory_rid, "method": "SendMessage",
            "params": {"message": {"role": "user", "messageId": str(uuid.uuid4()),
                "parts": [
                    {"kind": "text", "text": f"Restock inventory for returned order {order_id}"},
                    {"kind": "data", "data": {
                        "order":         order,
                        "return_reason": return_reason
                    }, "mediaType": "application/json"}
                ]}}
        }

        inventory_result = {}
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                card_r    = await client.get(f"{INVENTORY_AGENT_URL}/.well-known/agent-card.json")
                card      = card_r.json()
                agent_url = card.get("supportedInterfaces", [{}])[0].get("url", INVENTORY_AGENT_URL)

                inv_r    = await client.post(agent_url, json=inventory_payload, timeout=60.0)
                inv_resp = inv_r.json()

            artifacts = inv_resp.get("result", {}).get("task", {}).get("artifacts", [])
            for a in artifacts:
                for p in a.get("parts", []):
                    if "data" in p:
                        inventory_result = p["data"]
                        break
        except Exception as inv_err:
            inventory_result = {"error": str(inv_err), "note": "Refund processed but inventory restock failed"}

        result = {
            "success":          True,
            "order_id":         order_id,
            "refund_id":        refund_id,
            "refund_amount":    refund_amount,
            "currency":         order.get("currency", "INR"),
            "refund_method":    "original_payment",
            "refunded_at":      refunded_at,
            "return_reason":    return_reason,
            "status":           "refunded",
            "inventory_restock": inventory_result,
            "message":          f"Refund of Rs.{refund_amount} processed for {order_id}. Ref: {refund_id}"
        }

        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "result": {"task": {
                "id": str(uuid.uuid4()), "contextId": str(uuid.uuid4()),
                "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{"artifactId": str(uuid.uuid4()), "name": "refund_result",
                    "parts": [{"kind": "data", "data": result, "mediaType": "application/json"}]
                }]
            }}
        })

    except Exception as e:
        return JSONResponse({
            "jsonrpc": "2.0", "id": "req-001",
            "error": {"code": -32000, "message": str(e)[:300]}
        })
