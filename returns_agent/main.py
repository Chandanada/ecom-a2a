"""
Returns Agent — A2A Remote Agent
Handles return requests: verifies eligibility, generates reverse pickup AWB via Logistics Agent.
"""
import os, uuid
from datetime import datetime, timezone
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx

app = FastAPI(title="Returns Agent", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

AGENT_BASE_URL      = os.getenv("AGENT_BASE_URL", "https://ecom-returns-agent.onrender.com")
LOGISTICS_AGENT_URL = os.getenv("LOGISTICS_AGENT_URL", "https://ecom-logistics-agent.onrender.com")
RETURN_WINDOW_DAYS  = int(os.getenv("RETURN_WINDOW_DAYS", "10"))

AGENT_CARD = {
    "name":        "Returns Agent",
    "description": "Processes ecommerce return requests. Verifies return eligibility within window, generates reverse pickup AWB via Logistics Agent.",
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
        "id":          "process_return",
        "name":        "Process Return",
        "description": "Verify return eligibility and generate reverse pickup AWB.",
        "tags":        ["returns", "reverse_logistics", "awb"],
        "examples":    ["Process return for ORD-005, reason: wrong item delivered"],
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
    return {"status": "ok", "service": "returns_agent"}


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
        reason = data.get("return_reason", "customer_request")

        order_id   = order.get("id", "UNKNOWN")
        shipped_at = order.get("shipped_at")

        # ── Step 1: Verify return window ──────────────────────────
        if shipped_at:
            try:
                shipped_dt  = datetime.fromisoformat(shipped_at.replace("Z", "+00:00"))
                now         = datetime.now(timezone.utc)
                days_since  = (now - shipped_dt).days
                if days_since > RETURN_WINDOW_DAYS:
                    return JSONResponse({
                        "jsonrpc": "2.0", "id": req_id,
                        "result": {"task": {
                            "id": str(uuid.uuid4()), "contextId": str(uuid.uuid4()),
                            "status": {"state": "TASK_STATE_FAILED"},
                            "artifacts": [{"artifactId": str(uuid.uuid4()), "name": "return_result", "parts": [{
                                "kind": "data", "mediaType": "application/json",
                                "data": {
                                    "success": False,
                                    "error": f"Return window expired. Order shipped {days_since} days ago. Window: {RETURN_WINDOW_DAYS} days.",
                                    "order_id": order_id
                                }
                            }]}]
                        }}
                    })
            except Exception:
                pass  # If date parse fails, allow return

        # ── Step 2: Call Logistics Agent for reverse AWB ──────────
        logistics_rid = f"req-{uuid.uuid4().hex[:8]}"

        # Mark as reverse pickup in order data
        reverse_order = {**order, "id": f"RETURN-{order_id}", "type": "reverse_pickup"}

        logistics_payload = {
            "jsonrpc": "2.0", "id": logistics_rid, "method": "SendMessage",
            "params": {"message": {"role": "user", "messageId": str(uuid.uuid4()),
                "parts": [
                    {"kind": "text", "text": f"Generate reverse pickup AWB for return of {order_id}"},
                    {"kind": "data", "data": {"order": reverse_order}, "mediaType": "application/json"}
                ]}}
        }

        # Discover logistics agent URL
        async with httpx.AsyncClient(timeout=30.0) as client:
            card_r     = await client.get(f"{LOGISTICS_AGENT_URL}/.well-known/agent-card.json")
            card       = card_r.json()
            agent_url  = card.get("supportedInterfaces", [{}])[0].get("url", LOGISTICS_AGENT_URL)

            logistics_r = await client.post(agent_url, json=logistics_payload, timeout=60.0)
            logistics_resp = logistics_r.json()

        # Extract AWB from logistics response
        artifacts  = logistics_resp.get("result", {}).get("task", {}).get("artifacts", [])
        awb_data   = None
        for a in artifacts:
            for p in a.get("parts", []):
                if "data" in p:
                    awb_data = p["data"]
                    break

        if not awb_data or not awb_data.get("awb"):
            raise Exception("Logistics Agent did not return AWB")

        reverse_awb     = awb_data["awb"]
        carrier         = awb_data.get("carrier", "Delhivery")
        tracking_url    = awb_data.get("tracking_url", f"https://www.delhivery.com/track/package/{reverse_awb}")

        result = {
            "success":         True,
            "order_id":        order_id,
            "return_reason":   reason,
            "reverse_awb":     reverse_awb,
            "carrier":         carrier,
            "tracking_url":    tracking_url,
            "status":          "return_initiated",
            "return_initiated_at": datetime.now(timezone.utc).isoformat(),
            "message":         f"Return initiated for {order_id}. Reverse AWB: {reverse_awb} via {carrier}"
        }

        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "result": {"task": {
                "id": str(uuid.uuid4()), "contextId": str(uuid.uuid4()),
                "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{"artifactId": str(uuid.uuid4()), "name": "return_result",
                    "parts": [{"kind": "data", "data": result, "mediaType": "application/json"}]
                }]
            }}
        })

    except Exception as e:
        return JSONResponse({
            "jsonrpc": "2.0", "id": "req-001",
            "error": {"code": -32000, "message": str(e)[:300]}
        })
