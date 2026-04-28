"""
Logistics Agent — A2A Remote Agent
Exposes /.well-known/agent-card.json per Google A2A spec.
Accepts A2A SendMessage with order details.
Calls Shiprocket API to generate real AWB.
Returns AWB number as A2A artifact.
"""
import os, uuid, httpx, re
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

AGENT_BASE_URL      = os.getenv("AGENT_BASE_URL", "http://localhost:8001")
SHIPROCKET_EMAIL    = os.getenv("SHIPROCKET_EMAIL", "")
SHIPROCKET_PASSWORD = os.getenv("SHIPROCKET_PASSWORD", "")
ORDER_SERVICE_URL   = os.getenv("ORDER_SERVICE_URL", "http://localhost:8000")

# Methods we accept — covers all callers (dashboard, agent_ecom, direct curl)
SUPPORTED_METHODS = {"SendMessage", "message/send", "messages/send", "generate_awb"}

# Regex to pull ORD-XXX from plain-text prompts
ORDER_ID_RE = re.compile(r"\bORD-\d+\b", re.IGNORECASE)

app = FastAPI(title="Logistics Agent", version="1.0.0")

# ── CORS — allow any origin so agent card + POST work from anywhere ────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── A2A Agent Card ────────────────────────────────────────────────────────────
AGENT_CARD = {
    "name":        "Logistics Agent",
    "description": "Generates AWB (Air Waybill) tracking numbers for ecommerce orders via Shiprocket. Integrates with FedEx, Delhivery, BlueDart.",
    "supportedInterfaces": [{
        "url":             AGENT_BASE_URL,
        "protocolBinding": "JSONRPC",
        "protocolVersion": "1.0"
    }],
    "provider": {"organization": "Ecom A2A Platform"},
    "version":  "1.0.0",
    "capabilities": {
        "streaming":              False,
        "pushNotifications":      False,
        "stateTransitionHistory": True,
        "extendedAgentCard":      False
    },
    "defaultInputModes":  ["application/json"],
    "defaultOutputModes": ["application/json"],
    "skills": [{
        "id":          "generate_awb",
        "name":        "Generate AWB",
        "description": "Creates shipment and generates AWB tracking number via Shiprocket logistics network.",
        "tags":        ["logistics", "awb", "shipping", "shiprocket", "fedex"],
        "examples":    ["Generate AWB for order ORD-001 to ship to Mumbai"],
        "inputModes":  ["application/json"],
        "outputModes": ["application/json"]
    }],
    "signatures":      None,
    "securitySchemes": None,
    "security":        None
}


@app.get("/.well-known/agent-card.json")
def agent_card():
    return JSONResponse(AGENT_CARD)


@app.get("/health")
def health():
    return {"status": "ok", "service": "logistics_agent"}


# ── Shiprocket token cache ─────────────────────────────────────────────────────
_sr_token = None

async def get_shiprocket_token() -> str:
    global _sr_token
    if _sr_token:
        return _sr_token
    if not SHIPROCKET_EMAIL or not SHIPROCKET_PASSWORD:
        return None  # mock mode
    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://apiv2.shiprocket.in/v1/external/auth/login",
            json={"email": SHIPROCKET_EMAIL, "password": SHIPROCKET_PASSWORD},
            timeout=15.0
        )
        data = r.json()
        _sr_token = data.get("token")
        return _sr_token


async def fetch_order_from_service(order_id: str) -> dict:
    """Pull order data from ORDER_SERVICE_URL when only order ID given in prompt."""
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(f"{ORDER_SERVICE_URL}/orders/{order_id}")
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        print(f"[fetch_order] {order_id} failed: {e}")
    return {}


async def create_shiprocket_shipment(order: dict) -> dict:
    """Create real Shiprocket shipment and get AWB."""
    token = await get_shiprocket_token()

    if not token:
        # Mock mode — real AWB format, generated locally
        mock_awb = f"SR{uuid.uuid4().int % 10**10:010d}"
        return {
            "awb":          mock_awb,
            "carrier":      "Delhivery (Mock)",
            "tracking_url": f"https://www.delhivery.com/track/package/{mock_awb}",
            "shipment_id":  f"SHIP-{uuid.uuid4().hex[:8].upper()}",
            "mode":         "mock"
        }

    addr     = order.get("shipping_address", {})
    items    = order.get("items", [])
    customer = order.get("customer", {})

    payload = {
        "order_id":        order["id"],
        "order_date":      datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
        "pickup_location": "Primary",
        "billing_customer_name": customer.get("name", ""),
        "billing_last_name":     "",
        "billing_address":       addr.get("line1", ""),
        "billing_city":          addr.get("city", ""),
        "billing_pincode":       addr.get("pincode", ""),
        "billing_state":         addr.get("state", ""),
        "billing_country":       "India",
        "billing_email":         customer.get("email", ""),
        "billing_phone":         customer.get("phone", ""),
        "shipping_is_billing":   True,
        "order_items": [{
            "name":          item.get("name", ""),
            "sku":           item.get("sku", "SKU001"),
            "units":         item.get("qty", 1),
            "selling_price": str(item.get("price", 0))
        } for item in items],
        "payment_method": "Prepaid",
        "sub_total":      order.get("total", 0),
        "length": 15, "breadth": 10, "height": 5, "weight": 0.5
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            "https://apiv2.shiprocket.in/v1/external/orders/create/adhoc",
            json=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )
        order_data  = r.json()
        shipment_id = order_data.get("shipment_id")

        if not shipment_id:
            raise Exception(f"Shiprocket order creation failed: {order_data}")

        r2 = await client.post(
            "https://apiv2.shiprocket.in/v1/external/courier/assign/awb",
            json={"shipment_id": str(shipment_id)},
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )
        awb_data = r2.json()
        awb      = awb_data.get("response", {}).get("data", {}).get("awb_code")
        courier  = awb_data.get("response", {}).get("data", {}).get("courier_name", "Shiprocket")

        return {
            "awb":          awb,
            "carrier":      courier,
            "tracking_url": f"https://shiprocket.co/tracking/{awb}",
            "shipment_id":  str(shipment_id),
            "mode":         "live"
        }


def extract_text_from_parts(parts: list) -> str:
    texts = []
    for part in parts:
        text = part.get("text")
        if isinstance(text, str):
            texts.append(text)
    return " ".join(texts)


# ── A2A SendMessage handler ───────────────────────────────────────────────────
@app.post("/")
async def handle_message(request: Request):
    try:
        body   = await request.json()
        req_id = body.get("id", "req-001")
        method = body.get("method", "")

        # Accept any supported method name
        if method not in SUPPORTED_METHODS:
            return JSONResponse({
                "jsonrpc": "2.0", "id": req_id,
                "error": {"code": -32601, "message": f"Method '{method}' not supported. Use one of: {sorted(SUPPORTED_METHODS)}"}
            })

        params  = body.get("params", {})
        message = params.get("message", {})
        parts   = message.get("parts", [])

        # Try structured data part first
        data  = next((p.get("data") for p in parts if "data" in p), {})
        order = data.get("order", data) if data else {}

        # If no structured order, try parsing order ID from text and fetch it
        if not order.get("id"):
            prompt_text = extract_text_from_parts(parts)
            match       = ORDER_ID_RE.search(prompt_text)
            if match:
                order_id = match.group(0).upper()
                print(f"[handle_message] No data part — fetching {order_id} from order service")
                order = await fetch_order_from_service(order_id)

        if not order.get("id"):
            return JSONResponse({
                "jsonrpc": "2.0", "id": req_id,
                "error": {"code": -32600, "message": "order.id required — pass data part with order dict, or include ORD-XXX in text"}
            })

        result     = await create_shiprocket_shipment(order)
        task_id    = str(uuid.uuid4())
        context_id = str(uuid.uuid4())

        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "result": {
                "task": {
                    "id":        task_id,
                    "contextId": context_id,
                    "status":    {"state": "TASK_STATE_COMPLETED"},
                    "artifacts": [{
                        "artifactId": str(uuid.uuid4()),
                        "name":       "awb_result",
                        "parts": [{
                            "kind":      "data",
                            "data":      result,
                            "mediaType": "application/json"
                        }]
                    }]
                }
            }
        })

    except Exception as e:
        return JSONResponse({
            "jsonrpc": "2.0", "id": "req-001",
            "error": {"code": -32000, "message": str(e)[:300]}
        }, status_code=200)
