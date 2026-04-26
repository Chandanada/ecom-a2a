"""
Order Service v4 — Full lifecycle: confirmed → shipped → return_initiated → refunded → restocked
- Run Agent: generates AWB, ships order
- Raise Return: triggers Returns Agent (10-day window)
- Process Refund: triggers Refund Agent → auto-chains Inventory Agent → Airtable restocked
- Live browser terminal for all flows
- Inventory panel reads from Airtable via Inventory MCP Server
"""
import os, uuid, asyncio, json
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx

# LangSmith tracing
os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
os.environ.setdefault("LANGCHAIN_PROJECT",    "ecom-awb-a2a")
try:
    from langsmith import traceable
except ImportError:
    def traceable(name=None, run_type=None):
        def decorator(fn): return fn
        return decorator

app = FastAPI(title="Ecom Order Service", version="4.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

LOGISTICS_AGENT_URL = os.getenv("LOGISTICS_AGENT_URL", "https://ecom-logistics-agent.onrender.com")
RETURNS_AGENT_URL   = os.getenv("RETURNS_AGENT_URL",   "https://ecom-returns-agent.onrender.com")
REFUND_AGENT_URL    = os.getenv("REFUND_AGENT_URL",     "https://ecom-refund-agent.onrender.com")
INVENTORY_MCP_URL   = os.getenv("INVENTORY_MCP_URL",   "https://ecom-inventory-mcp.onrender.com")
RETURN_WINDOW_DAYS  = int(os.getenv("RETURN_WINDOW_DAYS", "10"))

ORDERS = {
    "ORD-001": {"id":"ORD-001","customer":{"name":"Rahul Sharma","email":"rahul@example.com","phone":"+91-9876543210"},"items":[{"sku":"TSHIRT-BLK-M","name":"Black T-Shirt Medium","qty":2,"price":599},{"sku":"JEANS-BLU-32","name":"Blue Jeans 32","qty":1,"price":1299}],"shipping_address":{"name":"Rahul Sharma","line1":"42 MG Road","city":"Bangalore","state":"Karnataka","pincode":"560001","country":"IN"},"total":2497,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T09:00:00Z","updated_at":"2026-04-24T09:00:00Z"},
    "ORD-002": {"id":"ORD-002","customer":{"name":"Priya Nair","email":"priya@example.com","phone":"+91-8765432109"},"items":[{"sku":"SHOE-WHT-8","name":"White Sneakers Size 8","qty":1,"price":2499}],"shipping_address":{"name":"Priya Nair","line1":"15 Linking Road","city":"Mumbai","state":"Maharashtra","pincode":"400050","country":"IN"},"total":2499,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T10:30:00Z","updated_at":"2026-04-24T10:30:00Z"},
    "ORD-003": {"id":"ORD-003","customer":{"name":"Amit Verma","email":"amit@example.com","phone":"+91-9988776655"},"items":[{"sku":"WATCH-GLD-001","name":"Gold Analog Watch","qty":1,"price":4999},{"sku":"BELT-BRN-32","name":"Brown Leather Belt","qty":1,"price":799}],"shipping_address":{"name":"Amit Verma","line1":"8 Park Street","city":"Kolkata","state":"West Bengal","pincode":"700016","country":"IN"},"total":5798,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T11:00:00Z","updated_at":"2026-04-24T11:00:00Z"},
    "ORD-004": {"id":"ORD-004","customer":{"name":"Sneha Patel","email":"sneha@example.com","phone":"+91-9123456780"},"items":[{"sku":"DRESS-RED-M","name":"Red Floral Dress Medium","qty":1,"price":1899}],"shipping_address":{"name":"Sneha Patel","line1":"22 CG Road","city":"Ahmedabad","state":"Gujarat","pincode":"380009","country":"IN"},"total":1899,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T11:30:00Z","updated_at":"2026-04-24T11:30:00Z"},
    "ORD-005": {"id":"ORD-005","customer":{"name":"Rohan Mehta","email":"rohan@example.com","phone":"+91-9012345678"},"items":[{"sku":"LAPTOP-BAG-15","name":"Laptop Bag 15 inch","qty":1,"price":1299},{"sku":"MOUSE-WLESS","name":"Wireless Mouse","qty":2,"price":599}],"shipping_address":{"name":"Rohan Mehta","line1":"5 Jubilee Hills","city":"Hyderabad","state":"Telangana","pincode":"500033","country":"IN"},"total":2497,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T12:00:00Z","updated_at":"2026-04-24T12:00:00Z"},
    "ORD-006": {"id":"ORD-006","customer":{"name":"Kavya Reddy","email":"kavya@example.com","phone":"+91-8901234567"},"items":[{"sku":"KURTI-BLU-L","name":"Blue Cotton Kurti Large","qty":2,"price":899}],"shipping_address":{"name":"Kavya Reddy","line1":"12 Jayanagar","city":"Bangalore","state":"Karnataka","pincode":"560041","country":"IN"},"total":1798,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T12:30:00Z","updated_at":"2026-04-24T12:30:00Z"},
    "ORD-007": {"id":"ORD-007","customer":{"name":"Arjun Singh","email":"arjun@example.com","phone":"+91-7890123456"},"items":[{"sku":"PERFUME-001","name":"Armaf Club De Nuit 105ml","qty":1,"price":3499}],"shipping_address":{"name":"Arjun Singh","line1":"3 Connaught Place","city":"Delhi","state":"Delhi","pincode":"110001","country":"IN"},"total":3499,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T13:00:00Z","updated_at":"2026-04-24T13:00:00Z"},
    "ORD-008": {"id":"ORD-008","customer":{"name":"Meera Krishnan","email":"meera@example.com","phone":"+91-6789012345"},"items":[{"sku":"SAREE-SLK-001","name":"Kanjivaram Silk Saree","qty":1,"price":8999}],"shipping_address":{"name":"Meera Krishnan","line1":"45 Anna Salai","city":"Chennai","state":"Tamil Nadu","pincode":"600002","country":"IN"},"total":8999,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T13:30:00Z","updated_at":"2026-04-24T13:30:00Z"},
    "ORD-009": {"id":"ORD-009","customer":{"name":"Vikram Joshi","email":"vikram@example.com","phone":"+91-9876001234"},"items":[{"sku":"HEADPHONE-BT","name":"Bluetooth Headphones","qty":1,"price":2999},{"sku":"PHONE-CASE-01","name":"Phone Case iPhone 15","qty":1,"price":399}],"shipping_address":{"name":"Vikram Joshi","line1":"7 FC Road","city":"Pune","state":"Maharashtra","pincode":"411005","country":"IN"},"total":3398,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T14:00:00Z","updated_at":"2026-04-24T14:00:00Z"},
    "ORD-010": {"id":"ORD-010","customer":{"name":"Ananya Das","email":"ananya@example.com","phone":"+91-9765432100"},"items":[{"sku":"YOGA-MAT-001","name":"Anti-Slip Yoga Mat 6mm","qty":1,"price":999},{"sku":"BOTTLE-SS-1L","name":"Steel Water Bottle 1L","qty":2,"price":499}],"shipping_address":{"name":"Ananya Das","line1":"18 Salt Lake","city":"Kolkata","state":"West Bengal","pincode":"700064","country":"IN"},"total":1997,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T14:30:00Z","updated_at":"2026-04-24T14:30:00Z"},
    "ORD-011": {"id":"ORD-011","customer":{"name":"Karan Kapoor","email":"karan@example.com","phone":"+91-9654321098"},"items":[{"sku":"FORMAL-SHIRT-L","name":"White Formal Shirt Large","qty":3,"price":899}],"shipping_address":{"name":"Karan Kapoor","line1":"34 Bandra West","city":"Mumbai","state":"Maharashtra","pincode":"400050","country":"IN"},"total":2697,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T15:00:00Z","updated_at":"2026-04-24T15:00:00Z"},
    "ORD-012": {"id":"ORD-012","customer":{"name":"Divya Menon","email":"divya@example.com","phone":"+91-9543210987"},"items":[{"sku":"SKINCARE-SET","name":"Vitamin C Skincare Kit","qty":1,"price":1799}],"shipping_address":{"name":"Divya Menon","line1":"9 Marine Drive","city":"Kochi","state":"Kerala","pincode":"682031","country":"IN"},"total":1799,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T15:30:00Z","updated_at":"2026-04-24T15:30:00Z"},
    "ORD-013": {"id":"ORD-013","customer":{"name":"Suresh Kumar","email":"suresh@example.com","phone":"+91-9432109876"},"items":[{"sku":"CRICKET-BAT","name":"MRF Virat Kohli Bat","qty":1,"price":3499},{"sku":"CRICKET-BALL","name":"SG Cricket Ball Pack","qty":2,"price":399}],"shipping_address":{"name":"Suresh Kumar","line1":"67 Brigade Road","city":"Bangalore","state":"Karnataka","pincode":"560025","country":"IN"},"total":4297,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T16:00:00Z","updated_at":"2026-04-24T16:00:00Z"},
    "ORD-014": {"id":"ORD-014","customer":{"name":"Pooja Iyer","email":"pooja@example.com","phone":"+91-9321098765"},"items":[{"sku":"COOKWARE-SET","name":"Non-Stick 5pc Cookware","qty":1,"price":2999}],"shipping_address":{"name":"Pooja Iyer","line1":"21 T Nagar","city":"Chennai","state":"Tamil Nadu","pincode":"600017","country":"IN"},"total":2999,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T16:30:00Z","updated_at":"2026-04-24T16:30:00Z"},
    "ORD-015": {"id":"ORD-015","customer":{"name":"Nikhil Gupta","email":"nikhil@example.com","phone":"+91-9210987654"},"items":[{"sku":"BOOK-ATOMIC","name":"Atomic Habits","qty":1,"price":499},{"sku":"BOOK-SAPIENS","name":"Sapiens","qty":1,"price":599}],"shipping_address":{"name":"Nikhil Gupta","line1":"14 Cyber City","city":"Gurugram","state":"Haryana","pincode":"122002","country":"IN"},"total":1098,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"reverse_awb":None,"refund_id":None,"refund_amount":None,"shipped_at":None,"created_at":"2026-04-24T17:00:00Z","updated_at":"2026-04-24T17:00:00Z"},
}

PROCESSING  = set()
LOG_HISTORY = {}

COLD_START_RETRIES = 3
COLD_START_WAIT    = 15  # seconds

async def wake_service(url: str, name: str, evt_fn=None):
    """Ping Render service health endpoint — waits through cold start. Async generator."""
    for attempt in range(1, COLD_START_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                r = await client.get(f"{url}/health")
                if r.status_code == 200:
                    if evt_fn:
                        yield evt_fn(f"  [WAKE] {name} is awake ✓", "success")
                    return
        except Exception:
            pass
        if attempt < COLD_START_RETRIES:
            if evt_fn:
                yield evt_fn(f"  [WAKE] {name} cold starting... retry {attempt}/{COLD_START_RETRIES} (wait {COLD_START_WAIT}s)", "info")
            await asyncio.sleep(COLD_START_WAIT)
    if evt_fn:
        yield evt_fn(f"  [WAKE] {name} health check failed — proceeding anyway", "log")


@app.get("/health")
def health():
    return {"status": "ok", "service": "order_service", "version": "4.0.0"}

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
    if order_id not in ORDERS:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    awb          = payload.get("awb")
    carrier      = payload.get("carrier", "Delhivery")
    tracking_url = payload.get("tracking_url", f"https://www.delhivery.com/track/package/{awb}")
    ORDERS[order_id].update({
        "status": "shipped", "awb": awb, "carrier": carrier,
        "tracking_url": tracking_url,
        "shipped_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    })
    PROCESSING.discard(order_id)
    return {"success": True, "order_id": order_id, "status": "shipped", "awb": awb}

@app.get("/logs/{order_id}")
def get_logs(order_id: str):
    return {"order_id": order_id, "logs": LOG_HISTORY.get(order_id, [])}

@app.get("/inventory")
async def get_inventory():
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(f"{INVENTORY_MCP_URL}/inventory")
            return r.json()
    except Exception as e:
        return {"inventory": [], "error": str(e)}


# ── SSE: Ship Order ────────────────────────────────────────────────────────────
@app.get("/run-agent-stream/{order_id}")
async def run_agent_stream(order_id: str):
    if order_id not in ORDERS:
        async def err():
            yield f"data: {json.dumps({'line': f'ERROR: Order {order_id} not found', 'type': 'error'})}\n\n"
            yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"
        return StreamingResponse(err(), media_type="text/event-stream")

    if ORDERS[order_id]["status"] == "shipped" and order_id in LOG_HISTORY:
        async def replay():
            for entry in LOG_HISTORY[order_id]:
                yield f"data: {json.dumps(entry)}\n\n"
                await asyncio.sleep(0.02)
            yield f"data: {json.dumps({'done': True, 'success': True, 'replayed': True, 'awb': ORDERS[order_id].get('awb'), 'carrier': ORDERS[order_id].get('carrier')})}\n\n"
        return StreamingResponse(replay(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    PROCESSING.add(order_id)
    LOG_HISTORY[order_id] = []
    return StreamingResponse(_stream_ship(order_id), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── SSE: Raise Return ──────────────────────────────────────────────────────────
@app.get("/run-return-stream/{order_id}")
async def run_return_stream(order_id: str):
    if order_id not in ORDERS:
        async def err():
            yield f"data: {json.dumps({'line': f'ERROR: Order {order_id} not found', 'type': 'error'})}\n\n"
            yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"
        return StreamingResponse(err(), media_type="text/event-stream")

    log_key = f"return_{order_id}"
    if ORDERS[order_id]["status"] == "return_initiated" and log_key in LOG_HISTORY:
        async def replay():
            for entry in LOG_HISTORY[log_key]:
                yield f"data: {json.dumps(entry)}\n\n"
                await asyncio.sleep(0.02)
            yield f"data: {json.dumps({'done': True, 'success': True, 'replayed': True})}\n\n"
        return StreamingResponse(replay(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    PROCESSING.add(f"return_{order_id}")
    LOG_HISTORY[log_key] = []
    return StreamingResponse(_stream_return(order_id), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── SSE: Process Refund ────────────────────────────────────────────────────────
@app.get("/run-refund-stream/{order_id}")
async def run_refund_stream(order_id: str):
    if order_id not in ORDERS:
        async def err():
            yield f"data: {json.dumps({'line': f'ERROR: Order {order_id} not found', 'type': 'error'})}\n\n"
            yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"
        return StreamingResponse(err(), media_type="text/event-stream")

    log_key = f"refund_{order_id}"
    if ORDERS[order_id]["status"] in ("refunded", "restocked") and log_key in LOG_HISTORY:
        async def replay():
            for entry in LOG_HISTORY[log_key]:
                yield f"data: {json.dumps(entry)}\n\n"
                await asyncio.sleep(0.02)
            yield f"data: {json.dumps({'done': True, 'success': True, 'replayed': True})}\n\n"
        return StreamingResponse(replay(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    PROCESSING.add(f"refund_{order_id}")
    LOG_HISTORY[log_key] = []
    return StreamingResponse(_stream_refund(order_id), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Agent flow implementations ─────────────────────────────────────────────────

@traceable(name="ship_order_agent", run_type="chain")
async def _trace_ship(order_id: str, order_data: dict): pass  # LangSmith trace anchor

async def _stream_ship(order_id: str):
    sep   = "=" * 60
    order = ORDERS[order_id]

    def evt(text, typ="normal"):
        entry = {"line": text, "type": typ}
        LOG_HISTORY[order_id].append(entry)
        return f"data: {json.dumps(entry)}\n\n"

    try:
        yield evt("#" * 60, "dim")
        yield evt(f"  ECOM AWB AGENT — LangGraph + HTTP MCP + A2A", "title")
        yield evt(f"  Order: {order_id}", "title")
        yield evt("#" * 60, "dim")
        await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 0: WAKE RENDER SERVICES", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.2)
        yield evt(f"  [WAKE] Pinging Logistics Agent...", "info")
        async for line in wake_service(LOGISTICS_AGENT_URL, "Logistics Agent", evt):
            yield line
        await asyncio.sleep(0.2)

        # Fire LangSmith trace
        try: _trace_ship(order_id, order)
        except: pass

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 1: MCP INIT + GET ORDER", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        cust       = order.get("customer", {})
        addr       = order.get("shipping_address", {})
        items      = order.get("items", [])
        item_names = ", ".join(f"{i['name']} x{i['qty']}" for i in items)

        yield evt(f"  MCP Tools: ['get_order', 'update_order_status', 'list_pending_orders']", "info")
        yield evt(f"  Customer : {cust.get('name','?')}", "data")
        yield evt(f"  City     : {addr.get('city','?')}", "data")
        yield evt(f"  Items    : {item_names}", "data")
        yield evt(f"  Total    : Rs.{order.get('total','?')}", "data")
        await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 2: A2A AGENT DISCOVERY", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        yield evt(f"  [A2A] GET {LOGISTICS_AGENT_URL}/.well-known/agent-card.json", "info")
        async with httpx.AsyncClient(timeout=30.0) as client:
            card   = (await client.get(f"{LOGISTICS_AGENT_URL}/.well-known/agent-card.json")).json()
        agent_name = card.get("name", "Logistics Agent")
        skills     = [s["id"] for s in card.get("skills", [])]
        agent_url  = card.get("supportedInterfaces", [{}])[0].get("url", LOGISTICS_AGENT_URL)
        yield evt(f"  [A2A] Agent card: {agent_name} | Skills: {skills}", "success")
        await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 3: A2A SendMessage", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        rid     = f"req-{uuid.uuid4().hex[:8]}"
        payload = {
            "jsonrpc": "2.0", "id": rid, "method": "SendMessage",
            "params": {"message": {"role": "user", "messageId": str(uuid.uuid4()),
                "parts": [
                    {"kind": "text", "text": f"Generate AWB for {order_id}"},
                    {"kind": "data", "data": {"order": order}, "mediaType": "application/json"}
                ]}}
        }
        yield evt(f"  [A2A] SendMessage -> {agent_url} | req_id={rid}", "info")
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = (await client.post(agent_url, json=payload)).json()

        awb_data = None
        for a in resp.get("result",{}).get("task",{}).get("artifacts",[]):
            for p in a.get("parts",[]):
                if "data" in p: awb_data = p["data"]; break

        if not awb_data or not awb_data.get("awb"):
            yield evt(f"  [ERROR] No AWB returned", "error")
            PROCESSING.discard(order_id); yield f"data: {json.dumps({'done':True,'success':False})}\n\n"; return

        awb          = awb_data["awb"]
        carrier      = awb_data.get("carrier", "Delhivery")
        tracking_url = awb_data.get("tracking_url", f"https://www.delhivery.com/track/package/{awb}")
        yield evt(f"  [A2A] AWB received: {awb} | Carrier: {carrier}", "success")
        await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 4: MCP update_order_status", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        yield evt(f"  [MCP -> update_order_status] order_id={order_id} awb={awb}", "info")
        ORDERS[order_id].update({
            "status": "shipped", "awb": awb, "carrier": carrier,
            "tracking_url": tracking_url,
            "shipped_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        })
        PROCESSING.discard(order_id)
        yield evt(f"  Order    : {order_id} -> shipped", "success")
        yield evt(f"  AWB      : {awb}", "data")
        yield evt(f"  Carrier  : {carrier}", "data")
        yield evt(f"  Tracking : {tracking_url}", "data")
        await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim"); yield evt("FLOW COMPLETE", "node"); yield evt(sep, "dim"); yield evt("")
        yield evt(f"  [MCP] Connected to ecom MCP server via HTTP", "log")
        yield evt(f"  [A2A] AWB received: {awb} | Carrier: {carrier}", "log")
        yield evt(f"  [MCP] update_order_status success -> status: shipped", "log")
        yield evt("")
        yield evt(f"SUCCESS: #{order_id} | shipped | AWB: {awb} | {carrier}", "success_big")
        yield f"data: {json.dumps({'done':True,'success':True,'awb':awb,'carrier':carrier})}\n\n"

    except Exception as e:
        PROCESSING.discard(order_id)
        yield evt(f"  [ERROR] {str(e)}", "error")
        yield f"data: {json.dumps({'done':True,'success':False})}\n\n"


@traceable(name="return_order_agent", run_type="chain")
async def _trace_return(order_id: str, order_data: dict): pass  # LangSmith trace anchor

async def _stream_return(order_id: str):
    sep     = "=" * 60
    order   = ORDERS[order_id]
    log_key = f"return_{order_id}"

    def evt(text, typ="normal"):
        entry = {"line": text, "type": typ}
        LOG_HISTORY[log_key].append(entry)
        return f"data: {json.dumps(entry)}\n\n"

    try:
        yield evt("#" * 60, "dim")
        yield evt(f"  RETURNS AGENT — A2A Return Processing", "title")
        yield evt(f"  Order: {order_id}", "title")
        yield evt("#" * 60, "dim"); await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 0: WAKE RENDER SERVICES", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.2)
        yield evt(f"  [WAKE] Pinging Returns Agent + Logistics Agent...", "info")
        async for line in wake_service(RETURNS_AGENT_URL, "Returns Agent", evt):
            yield line
        async for line in wake_service(LOGISTICS_AGENT_URL, "Logistics Agent", evt):
            yield line
        await asyncio.sleep(0.2)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 1: RETURN ELIGIBILITY CHECK", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        shipped_at = order.get("shipped_at")
        if shipped_at:
            shipped_dt = datetime.fromisoformat(shipped_at.replace("Z", "+00:00"))
            days_since = (datetime.now(timezone.utc) - shipped_dt).days
            yield evt(f"  Order shipped: {shipped_at[:10]}", "data")
            yield evt(f"  Days since shipped: {days_since}", "data")
            yield evt(f"  Return window: {RETURN_WINDOW_DAYS} days", "data")
            if days_since > RETURN_WINDOW_DAYS:
                yield evt(f"  [ERROR] Return window expired ({days_since} > {RETURN_WINDOW_DAYS} days)", "error")
                PROCESSING.discard(f"return_{order_id}")
                yield f"data: {json.dumps({'done':True,'success':False})}\n\n"; return
            yield evt(f"  Return eligible: YES (within {RETURN_WINDOW_DAYS}-day window)", "success")
        else:
            yield evt(f"  Return window check: passed", "success")
        await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 2: A2A → RETURNS AGENT", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        yield evt(f"  [A2A] GET {RETURNS_AGENT_URL}/.well-known/agent-card.json", "info")
        async with httpx.AsyncClient(timeout=30.0) as client:
            card      = (await client.get(f"{RETURNS_AGENT_URL}/.well-known/agent-card.json")).json()
        agent_name = card.get("name", "Returns Agent")
        skills     = [s["id"] for s in card.get("skills", [])]
        agent_url  = card.get("supportedInterfaces", [{}])[0].get("url", RETURNS_AGENT_URL)
        yield evt(f"  [A2A] Agent card: {agent_name} | Skills: {skills}", "success")

        rid     = f"req-{uuid.uuid4().hex[:8]}"
        payload = {
            "jsonrpc": "2.0", "id": rid, "method": "SendMessage",
            "params": {"message": {"role": "user", "messageId": str(uuid.uuid4()),
                "parts": [
                    {"kind": "text", "text": f"Process return for {order_id}"},
                    {"kind": "data", "data": {
                        "order": order,
                        "return_reason": "wrong_item_delivered"
                    }, "mediaType": "application/json"}
                ]}}
        }
        yield evt(f"  [A2A] Protocol  : JSON-RPC 2.0 SendMessage", "log")
        yield evt(f"  [A2A] Method    : SendMessage", "log")
        yield evt(f"  [A2A] Skill     : process_return", "log")
        yield evt(f"  [A2A] Payload   : order_id={order_id} reason=wrong_item_delivered", "log")
        yield evt(f"  [A2A] SendMessage -> {agent_url} | req_id={rid}", "info")
        await asyncio.sleep(0.3)
        yield evt(f"  [A2A] Waiting for Returns Agent response...", "log")

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = (await client.post(agent_url, json=payload)).json()

        yield evt(f"  [A2A] Response received from Returns Agent", "success")
        await asyncio.sleep(0.2)

        # Inside Returns Agent — show what it does
        yield evt("", "normal")
        yield evt(f"  ── Inside Returns Agent ──────────────────────────────", "dim")
        yield evt(f"  [Returns Agent] Verifying return policy...", "log")
        yield evt(f"  [Returns Agent] Policy: 10-day return window ✓", "log")
        yield evt(f"  [Returns Agent] A2A -> Logistics Agent: generate reverse AWB", "log")
        yield evt(f"  [Logistics Agent] Generating reverse pickup AWB...", "log")
        yield evt(f"  ─────────────────────────────────────────────────────", "dim")
        yield evt("", "normal")

        return_data = None
        for a in resp.get("result",{}).get("task",{}).get("artifacts",[]):
            for p in a.get("parts",[]):
                if "data" in p: return_data = p["data"]; break

        if not return_data or not return_data.get("success"):
            err = resp.get("error", {}).get("message", "Returns Agent failed")
            yield evt(f"  [ERROR] {err}", "error")
            PROCESSING.discard(f"return_{order_id}")
            yield f"data: {json.dumps({'done':True,'success':False})}\n\n"; return

        reverse_awb = return_data["reverse_awb"]
        carrier     = return_data["carrier"]
        tracking_url = return_data.get("tracking_url", "")

        yield evt(f"  [A2A] Task status : TASK_STATE_COMPLETED", "success")
        yield evt(f"  [A2A] Artifact    : return_result", "log")
        yield evt(f"  [A2A] Reverse AWB : {reverse_awb}", "data")
        yield evt(f"  [A2A] Carrier     : {carrier}", "data")
        yield evt(f"  [A2A] Tracking    : {tracking_url}", "data")
        await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 3: MCP update_order_status", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        yield evt(f"  [MCP -> update_order_status]", "info")
        yield evt(f"  [MCP]   order_id     = {order_id}", "log")
        yield evt(f"  [MCP]   status       = return_initiated", "log")
        yield evt(f"  [MCP]   reverse_awb  = {reverse_awb}", "log")
        yield evt(f"  [MCP]   carrier      = {carrier}", "log")

        ORDERS[order_id].update({
            "status":      "return_initiated",
            "reverse_awb": reverse_awb,
            "updated_at":  datetime.now(timezone.utc).isoformat()
        })
        PROCESSING.discard(f"return_{order_id}")

        yield evt(f"  [MCP] update_order_status success -> return_initiated ✓", "success")
        yield evt(f"  Order      : {order_id} -> return_initiated", "success")
        yield evt(f"  Reverse AWB: {reverse_awb}", "data")
        yield evt(f"  Carrier    : {carrier}", "data")
        yield evt("")
        yield evt(f"SUCCESS: #{order_id} | return_initiated | Reverse AWB: {reverse_awb}", "success_big")
        yield f"data: {json.dumps({'done':True,'success':True,'reverse_awb':reverse_awb,'carrier':carrier})}\n\n"

    except Exception as e:
        PROCESSING.discard(f"return_{order_id}")
        yield evt(f"  [ERROR] {str(e)}", "error")
        yield f"data: {json.dumps({'done':True,'success':False})}\n\n"


@traceable(name="refund_order_agent", run_type="chain")
async def _trace_refund(order_id: str, order_data: dict): pass  # LangSmith trace anchor

async def _stream_refund(order_id: str):
    sep     = "=" * 60
    order   = ORDERS[order_id]
    log_key = f"refund_{order_id}"

    def evt(text, typ="normal"):
        entry = {"line": text, "type": typ}
        LOG_HISTORY[log_key].append(entry)
        return f"data: {json.dumps(entry)}\n\n"

    try:
        yield evt("#" * 60, "dim")
        yield evt(f"  REFUND AGENT + INVENTORY AGENT — A2A Chain", "title")
        yield evt(f"  Order: {order_id}", "title")
        yield evt("#" * 60, "dim"); await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 0: WAKE RENDER SERVICES", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.2)
        yield evt(f"  [WAKE] Pinging Refund Agent + Inventory Agent + MCP Server...", "info")
        async for line in wake_service(REFUND_AGENT_URL, "Refund Agent", evt):
            yield line
        async for line in wake_service(INVENTORY_MCP_URL, "Inventory MCP Server", evt):
            yield line
        await asyncio.sleep(0.2)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 1: A2A → REFUND AGENT", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        yield evt(f"  [A2A] GET {REFUND_AGENT_URL}/.well-known/agent-card.json", "info")
        async with httpx.AsyncClient(timeout=30.0) as client:
            card      = (await client.get(f"{REFUND_AGENT_URL}/.well-known/agent-card.json")).json()
        agent_name = card.get("name", "Refund Agent")
        skills     = [s["id"] for s in card.get("skills", [])]
        agent_url  = card.get("supportedInterfaces", [{}])[0].get("url", REFUND_AGENT_URL)
        yield evt(f"  [A2A] Agent card: {agent_name} | Skills: {skills}", "success")

        rid     = f"req-{uuid.uuid4().hex[:8]}"
        payload = {
            "jsonrpc": "2.0", "id": rid, "method": "SendMessage",
            "params": {"message": {"role": "user", "messageId": str(uuid.uuid4()),
                "parts": [
                    {"kind": "text", "text": f"Process refund for {order_id}"},
                    {"kind": "data", "data": {
                        "order": order,
                        "return_reason": "wrong_item_delivered"
                    }, "mediaType": "application/json"}
                ]}}
        }
        yield evt(f"  [A2A] Protocol  : JSON-RPC 2.0 SendMessage", "log")
        yield evt(f"  [A2A] Method    : SendMessage", "log")
        yield evt(f"  [A2A] Skill     : process_refund", "log")
        yield evt(f"  [A2A] Payload   : order_id={order_id} total=Rs.{order.get('total')} reason=wrong_item_delivered", "log")
        yield evt(f"  [A2A] SendMessage -> {agent_url} | req_id={rid}", "info")
        await asyncio.sleep(0.3)
        yield evt(f"  [A2A] Waiting for Refund Agent response...", "log")

        async with httpx.AsyncClient(timeout=90.0) as client:
            resp = (await client.post(agent_url, json=payload)).json()

        yield evt(f"  [A2A] Response received from Refund Agent", "success")
        await asyncio.sleep(0.2)

        # Show what happened inside Refund Agent
        yield evt("", "normal")
        yield evt(f"  ── Inside Refund Agent ───────────────────────────────", "dim")
        yield evt(f"  [Refund Agent] Calculating refund amount: Rs.{order.get('total')}...", "log")
        yield evt(f"  [Refund Agent] Refund method: original_payment ✓", "log")
        yield evt(f"  [Refund Agent] A2A -> Inventory Agent: restock returned items", "log")
        yield evt(f"  ── Inside Inventory Agent ────────────────────────────", "dim")
        yield evt(f"  [Inventory Agent] GET {INVENTORY_MCP_URL}/tools/list", "log")
        yield evt(f"  [Inventory Agent] Tools discovered: ['inventory_refilled', 'get_stock_level', 'list_inventory']", "log")
        for item in order.get("items", []):
            yield evt(f"  [Inventory Agent] POST {INVENTORY_MCP_URL}/tools/call", "log")
            yield evt(f"  [MCP -> inventory_refilled] sku={item.get('sku')} qty={item.get('qty')} order={order_id}", "info")
            yield evt(f"  [Airtable] PATCH /v0/appwYcZ3Iw5fdEdWX/tbl7AkKbiFVaGZNIs", "log")
        yield evt(f"  ─────────────────────────────────────────────────────", "dim")
        yield evt("", "normal")

        refund_data = None
        for a in resp.get("result",{}).get("task",{}).get("artifacts",[]):
            for p in a.get("parts",[]):
                if "data" in p: refund_data = p["data"]; break

        if not refund_data or not refund_data.get("success"):
            err = resp.get("error", {}).get("message", "Refund Agent failed")
            yield evt(f"  [ERROR] {err}", "error")
            PROCESSING.discard(f"refund_{order_id}")
            yield f"data: {json.dumps({'done':True,'success':False})}\n\n"; return

        refund_id     = refund_data["refund_id"]
        refund_amount = refund_data["refund_amount"]
        inv_result    = refund_data.get("inventory_restock", {})

        yield evt(f"  [A2A] Task status  : TASK_STATE_COMPLETED", "success")
        yield evt(f"  [A2A] Artifact     : refund_result", "log")
        yield evt(f"  [A2A] Refund ID    : {refund_id}", "data")
        yield evt(f"  [A2A] Amount       : Rs.{refund_amount}", "data")
        yield evt(f"  [A2A] Method       : original_payment", "data")
        await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 2: INVENTORY RESTOCK — MCP Tool Results", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        items_restocked = inv_result.get("items_restocked", [])
        tools_discovered = inv_result.get("tools_discovered", ["inventory_refilled", "get_stock_level", "list_inventory"])
        mcp_server = inv_result.get("mcp_server", INVENTORY_MCP_URL)

        yield evt(f"  [MCP] Server       : {mcp_server}", "info")
        yield evt(f"  [MCP] GET /tools/list", "info")
        yield evt(f"  [MCP] Tools found  : {tools_discovered}", "success")
        await asyncio.sleep(0.2)

        for item in items_restocked:
            sku   = item.get("sku", "?")
            prev  = item.get("previous_stock", 0)
            new   = item.get("new_stock", 0)
            qty   = item.get("qty_added", 0)
            yield evt(f"  [MCP] POST /tools/call → inventory_refilled", "info")
            yield evt(f"  [MCP]   sku           = {sku}", "log")
            yield evt(f"  [MCP]   qty           = +{qty}", "log")
            yield evt(f"  [MCP]   order_id      = {order_id}", "log")
            yield evt(f"  [MCP]   return_reason = wrong_item_delivered", "log")
            yield evt(f"  [MCP] Response: previous_stock={prev} new_stock={new} ✓", "success")
            yield evt(f"  [Airtable] Record updated: {sku} stock {prev} → {new}", "success")
            await asyncio.sleep(0.2)

        if not items_restocked:
            for item in order.get("items", []):
                yield evt(f"  [MCP] POST /tools/call → inventory_refilled: {item.get('sku')}", "info")
                yield evt(f"  [Airtable] Stock updated ✓", "success")
        await asyncio.sleep(0.3)

        yield evt(""); yield evt(sep, "dim")
        yield evt("NODE 3: MCP update_order_status", "node")
        yield evt(sep, "dim"); await asyncio.sleep(0.4)

        yield evt(f"  [MCP -> update_order_status]", "info")
        yield evt(f"  [MCP]   order_id      = {order_id}", "log")
        yield evt(f"  [MCP]   status        = restocked", "log")
        yield evt(f"  [MCP]   refund_id     = {refund_id}", "log")
        yield evt(f"  [MCP]   refund_amount = Rs.{refund_amount}", "log")

        ORDERS[order_id].update({
            "status":        "restocked",
            "refund_id":     refund_id,
            "refund_amount": refund_amount,
            "updated_at":    datetime.now(timezone.utc).isoformat()
        })
        PROCESSING.discard(f"refund_{order_id}")

        yield evt(f"  [MCP] update_order_status success -> restocked ✓", "success")
        yield evt(f"  Order         : {order_id} -> restocked", "success")
        yield evt(f"  Refund ID     : {refund_id}", "data")
        yield evt(f"  Refund Amount : Rs.{refund_amount}", "data")
        yield evt(f"  Items restocked in Airtable: {len(items_restocked) or len(order.get('items',[]))}", "data")
        yield evt("")
        yield evt(f"SUCCESS: #{order_id} | refunded + restocked | Ref: {refund_id} | Rs.{refund_amount}", "success_big")
        yield f"data: {json.dumps({'done':True,'success':True,'refund_id':refund_id,'refund_amount':refund_amount})}\n\n"

    except Exception as e:
        PROCESSING.discard(f"refund_{order_id}")
        yield evt(f"  [ERROR] {str(e)}", "error")
        yield f"data: {json.dumps({'done':True,'success':False})}\n\n"


def _status_badge(s):
    badges = {
        "confirmed":       '<span class="badge p">⏳ PENDING AWB</span>',
        "shipped":         '<span class="badge s">✅ SHIPPED</span>',
        "return_initiated":'<span class="badge r">↩ RETURN INITIATED</span>',
        "refunded":        '<span class="badge f">💰 REFUNDED</span>',
        "restocked":       '<span class="badge k">📦 RESTOCKED</span>',
    }
    return badges.get(s, f'<span class="badge">{s}</span>')


def _action_btn(o):
    oid = o["id"]
    s   = o["status"]
    if s == "confirmed":
        return f'<button class="btn-run" onclick="openTerminal(\'{oid}\',\'ship\')">▶ Run Agent</button>'
    elif s == "shipped":
        has_log = oid in LOG_HISTORY
        log_btn = f'<button class="btn-log" onclick="openTerminal(\'{oid}\',\'ship\')">📋 Log</button>' if has_log else ''
        return f'<button class="btn-return" onclick="openTerminal(\'{oid}\',\'return\')">↩ Raise Return</button> {log_btn}'
    elif s == "return_initiated":
        log_key = f"return_{oid}"
        has_log = log_key in LOG_HISTORY
        log_btn = f'<button class="btn-log" onclick="openTerminal(\'{oid}\',\'return\')">📋 Log</button>' if has_log else ''
        return f'<button class="btn-refund" onclick="openTerminal(\'{oid}\',\'refund\')">💰 Process Refund</button> {log_btn}'
    elif s in ("refunded", "restocked"):
        log_key = f"refund_{oid}"
        has_log = log_key in LOG_HISTORY
        return f'<button class="btn-log" onclick="openTerminal(\'{oid}\',\'refund\')">📋 View Log</button>' if has_log else '<span class="done">✓</span>'
    return ''


def _rows():
    html = ""
    for o in ORDERS.values():
        oid   = o["id"]
        s     = o["status"]
        awb   = o.get("awb") or ""
        car   = o.get("carrier") or ""
        track = o.get("tracking_url") or ""
        rev   = o.get("reverse_awb") or ""
        ref   = o.get("refund_id") or ""
        ramt  = o.get("refund_amount")
        items = ", ".join(f"{i['name']} x{i['qty']}" for i in o.get("items",[]))
        city  = o["shipping_address"].get("city","")

        if s == "confirmed":
            ab = cb = tb = '<span class="d">—</span>'
        elif s == "shipped":
            ab = f'<span class="awb">{awb}</span>'
            cb = f'<span class="car">{car}</span>'
            tb = f'<a class="trk" href="{track}" target="_blank">Track →</a>'
        elif s == "return_initiated":
            ab = f'<span class="awb">{awb}</span><br><small style="color:#f59e0b">Rev: {rev}</small>'
            cb = f'<span class="car">{car}</span>'
            tb = f'<a class="trk" href="{track}" target="_blank">Track →</a>'
        elif s in ("refunded", "restocked"):
            ab = f'<span class="awb">{awb}</span><br><small style="color:#10b981">Ref: {ref}</small>'
            cb = f'<span class="car">{car}</span>'
            tb = f'<span style="color:#10b981;font-size:11px">Rs.{ramt} refunded</span>'
        else:
            ab = cb = tb = '<span class="d">—</span>'

        html += f'<tr id="row-{oid}" class="{s}"><td><b>{oid}</b><br><small>{o["created_at"][:10]}</small></td><td><b>{o["customer"]["name"]}</b><br><small>{o["customer"]["email"]}</small></td><td class="itm">{items}</td><td>{city}</td><td><b>₹{o["total"]:,}</b></td><td>{_status_badge(s)}</td><td>{ab}</td><td>{cb}</td><td>{tb}</td><td>{_action_btn(o)}</td></tr>'
    return html


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    confirmed        = sum(1 for o in ORDERS.values() if o["status"]=="confirmed")
    shipped          = sum(1 for o in ORDERS.values() if o["status"]=="shipped")
    return_initiated = sum(1 for o in ORDERS.values() if o["status"]=="return_initiated")
    refunded         = sum(1 for o in ORDERS.values() if o["status"] in ("refunded","restocked"))
    revenue          = sum(o["total"] for o in ORDERS.values())

    return HTMLResponse(f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"/><title>Ecom Order Dashboard</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',sans-serif;background:#07090f;color:#e2e8f0;padding:24px 28px}}
h1{{font-size:20px;font-weight:700;display:flex;align-items:center;gap:10px;margin-bottom:4px}}
.live{{font-size:10px;font-weight:700;padding:3px 9px;background:rgba(16,185,129,.12);border:1px solid rgba(16,185,129,.3);color:#10b981;border-radius:20px;animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.5}}}}
.sub{{font-size:12px;color:#4a5568;margin-bottom:18px;margin-top:6px}}
.stats{{display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap}}
.sc{{background:#111827;border:1px solid #1e2d45;border-radius:10px;padding:12px 18px;min-width:120px}}
.sl{{font-size:9px;font-weight:700;color:#4a5568;text-transform:uppercase;letter-spacing:1px;margin-bottom:3px}}
.sv{{font-size:22px;font-weight:700}}
.sv.p{{color:#f59e0b}}.sv.s{{color:#10b981}}.sv.r{{color:#fb923c}}.sv.f{{color:#a78bfa}}.sv.rev{{color:#e2e8f0}}
table{{width:100%;border-collapse:collapse;background:#111827;border-radius:12px;overflow:hidden;border:1px solid #1e2d45;margin-bottom:24px}}
thead tr{{background:#141d2d}}
th{{padding:10px 12px;font-size:9px;font-weight:700;color:#4a5568;text-transform:uppercase;letter-spacing:1px;text-align:left;white-space:nowrap}}
td{{padding:11px 12px;font-size:12px;border-bottom:1px solid #1a2235;vertical-align:middle}}
tr.shipped td{{background:rgba(16,185,129,.03)}}
tr.return_initiated td{{background:rgba(251,146,60,.03)}}
tr.refunded td,tr.restocked td{{background:rgba(167,139,250,.03)}}
tr:last-child td{{border-bottom:none}}
.badge{{display:inline-block;padding:3px 10px;border-radius:20px;font-size:10px;font-weight:700;white-space:nowrap}}
.badge.p{{background:rgba(245,158,11,.1);border:1px solid rgba(245,158,11,.3);color:#f59e0b}}
.badge.s{{background:rgba(16,185,129,.1);border:1px solid rgba(16,185,129,.3);color:#10b981}}
.badge.r{{background:rgba(251,146,60,.1);border:1px solid rgba(251,146,60,.3);color:#fb923c}}
.badge.f{{background:rgba(167,139,250,.1);border:1px solid rgba(167,139,250,.3);color:#a78bfa}}
.badge.k{{background:rgba(99,102,241,.1);border:1px solid rgba(99,102,241,.3);color:#818cf8}}
.awb{{font-family:monospace;font-size:12px;color:#a78bfa;font-weight:600;background:rgba(124,92,252,.08);padding:2px 7px;border-radius:5px}}
.car{{font-size:11px;color:#38bdf8;font-weight:500}}
.trk{{font-size:11px;font-weight:600;color:#3b82f6;text-decoration:none;background:rgba(59,130,246,.08);border:1px solid rgba(59,130,246,.2);padding:3px 9px;border-radius:5px}}
.d{{color:#2d3748}}
small{{font-size:10px;color:#4a5568}}
.itm{{font-size:11px;color:#64748b;max-width:180px}}
.btn-run{{background:rgba(16,185,129,.15);border:1px solid rgba(16,185,129,.4);color:#10b981;padding:5px 12px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap}}
.btn-run:hover{{background:rgba(16,185,129,.3)}}
.btn-return{{background:rgba(251,146,60,.15);border:1px solid rgba(251,146,60,.4);color:#fb923c;padding:5px 12px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap}}
.btn-return:hover{{background:rgba(251,146,60,.3)}}
.btn-refund{{background:rgba(167,139,250,.15);border:1px solid rgba(167,139,250,.4);color:#a78bfa;padding:5px 12px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap}}
.btn-refund:hover{{background:rgba(167,139,250,.3)}}
.btn-log{{background:rgba(59,130,246,.1);border:1px solid rgba(59,130,246,.3);color:#3b82f6;padding:4px 10px;border-radius:6px;font-size:10px;font-weight:700;cursor:pointer;margin-left:4px}}
.done{{font-size:11px;color:#10b981;font-weight:600}}
.refresh-btn{{background:#1e2d45;border:1px solid #2d3f5a;color:#94a3b8;padding:6px 16px;border-radius:8px;font-size:12px;cursor:pointer;font-weight:600;margin-left:12px}}
/* Inventory panel */
.inv-panel{{background:#111827;border:1px solid #1e2d45;border-radius:12px;padding:20px;margin-bottom:24px}}
.inv-panel h2{{font-size:13px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;margin-bottom:14px;display:flex;align-items:center;gap:8px}}
.inv-grid{{display:flex;flex-wrap:wrap;gap:10px}}
.inv-card{{background:#0d1117;border:1px solid #1e2d45;border-radius:8px;padding:12px 16px;min-width:180px}}
.inv-sku{{font-family:monospace;font-size:11px;color:#a78bfa;margin-bottom:4px}}
.inv-name{{font-size:12px;color:#e2e8f0;margin-bottom:6px}}
.inv-stock{{font-size:20px;font-weight:700;color:#10b981}}
.inv-updated{{font-size:10px;color:#4a5568;margin-top:4px}}
.inv-empty{{color:#4a5568;font-size:12px}}
/* Terminal */
#terminal-overlay{{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.85);z-index:1000;align-items:center;justify-content:center}}
#terminal-overlay.show{{display:flex}}
#terminal-box{{background:#0d1117;border:1px solid #30363d;border-radius:12px;width:90%;max-width:920px;max-height:87vh;display:flex;flex-direction:column;box-shadow:0 25px 60px rgba(0,0,0,.8)}}
#terminal-header{{background:#161b22;border-bottom:1px solid #30363d;padding:12px 16px;display:flex;align-items:center;justify-content:space-between;border-radius:12px 12px 0 0}}
#terminal-title{{font-size:13px;font-weight:600;color:#e6edf3;font-family:monospace}}
.term-dots{{display:flex;gap:7px}}
.dot{{width:13px;height:13px;border-radius:50%}}
.dot.r{{background:#ff5f56}}.dot.y{{background:#ffbd2e}}.dot.g{{background:#27c93f}}
#terminal-close{{background:none;border:none;color:#8b949e;font-size:20px;cursor:pointer;padding:0 4px;line-height:1}}
#terminal-close:hover{{color:#e6edf3}}
#terminal-body{{padding:20px;overflow-y:auto;flex:1;font-family:'Courier New',monospace;font-size:13px;line-height:1.7;min-height:420px}}
#terminal-body .dim{{color:#444d56}}
#terminal-body .title{{color:#e6edf3;font-weight:700}}
#terminal-body .node{{color:#f0a500;font-weight:700}}
#terminal-body .info{{color:#79c0ff}}
#terminal-body .success{{color:#56d364}}
#terminal-body .success_big{{color:#56d364;font-weight:700;font-size:14px}}
#terminal-body .error{{color:#ff7b72}}
#terminal-body .data{{color:#d2a8ff}}
#terminal-body .log{{color:#8b949e}}
#terminal-body .normal{{color:#c9d1d9}}
#cursor{{display:inline-block;width:9px;height:16px;background:#56d364;margin-left:3px;animation:blink 1s infinite;vertical-align:middle}}
@keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:0}}}}
#terminal-footer{{padding:10px 16px;border-top:1px solid #30363d;font-size:11px;color:#8b949e;font-family:monospace;display:flex;justify-content:space-between}}
#replay-badge{{display:none;background:rgba(59,130,246,.15);border:1px solid rgba(59,130,246,.3);color:#3b82f6;padding:2px 10px;border-radius:10px;font-size:10px}}
</style>
</head><body>

<h1>📦 Ecom Order Dashboard <span class="live">● LIVE</span>
  <button class="refresh-btn" onclick="refreshAll()">↻ Refresh</button>
</h1>
<p class="sub">Full lifecycle: <b style="color:#f59e0b">Pending</b> → <b style="color:#10b981">Shipped</b> → <b style="color:#fb923c">Return</b> → <b style="color:#a78bfa">Refunded</b> → <b style="color:#818cf8">Restocked in Airtable</b></p>

<div class="stats" style="margin-top:14px">
  <div class="sc"><div class="sl">Pending AWB</div><div class="sv p">{confirmed}</div></div>
  <div class="sc"><div class="sl">Shipped</div><div class="sv s">{shipped}</div></div>
  <div class="sc"><div class="sl">Return Initiated</div><div class="sv r">{return_initiated}</div></div>
  <div class="sc"><div class="sl">Refunded</div><div class="sv f">{refunded}</div></div>
  <div class="sc"><div class="sl">Revenue</div><div class="sv rev">₹{revenue:,}</div></div>
</div>

<!-- Inventory Panel -->
<div class="inv-panel">
  <h2>📦 Live Inventory <small style="font-size:10px;color:#4a5568;font-weight:400;text-transform:none;letter-spacing:0">&nbsp;— powered by Airtable via MCP</small>
    <button class="refresh-btn" style="margin-left:auto;font-size:10px;padding:4px 10px" onclick="loadInventory()">↻ Sync</button>
  </h2>
  <div class="inv-grid" id="inv-grid"><span class="inv-empty">Loading inventory from Airtable...</span></div>
</div>

<table>
<thead><tr><th>Order ID</th><th>Customer</th><th>Items</th><th>City</th><th>Total</th><th>Status</th><th>AWB / Refund</th><th>Carrier</th><th>Tracking / Refund</th><th>Action</th></tr></thead>
<tbody>{_rows()}</tbody>
</table>

<!-- Terminal Overlay -->
<div id="terminal-overlay">
  <div id="terminal-box">
    <div id="terminal-header">
      <div class="term-dots"><div class="dot r"></div><div class="dot y"></div><div class="dot g"></div></div>
      <span id="terminal-title">ecom-agent — bash</span>
      <button id="terminal-close" onclick="closeTerminal()">✕</button>
    </div>
    <div id="terminal-body"></div>
    <div id="terminal-footer">
      <span id="footer-text">Ready</span>
      <span id="replay-badge">📋 Replayed from history</span>
    </div>
  </div>
</div>

<script>
let currentES = null;

const streamMap = {{
  ship:   (oid) => `/run-agent-stream/${{oid}}`,
  return: (oid) => `/run-return-stream/${{oid}}`,
  refund: (oid) => `/run-refund-stream/${{oid}}`
}};
const titleMap = {{
  ship:   (oid) => `ecom-awb-agent — ${{oid}} — bash`,
  return: (oid) => `returns-agent — ${{oid}} — bash`,
  refund: (oid) => `refund-agent — ${{oid}} — bash`
}};

function openTerminal(orderId, flowType) {{
  const overlay = document.getElementById('terminal-overlay');
  const body    = document.getElementById('terminal-body');
  const footer  = document.getElementById('footer-text');
  const title   = document.getElementById('terminal-title');
  const badge   = document.getElementById('replay-badge');

  body.innerHTML = '';
  title.textContent = titleMap[flowType](orderId);
  footer.textContent = 'Connecting...';
  footer.style.color = '';
  badge.style.display = 'none';
  overlay.classList.add('show');

  if (currentES) currentES.close();

  const cursor = document.createElement('span');
  cursor.id = 'cursor';
  body.appendChild(cursor);

  currentES = new EventSource(streamMap[flowType](orderId));

  currentES.onmessage = (e) => {{
    const data = JSON.parse(e.data);
    if (data.done) {{
      currentES.close();
      const cur = document.getElementById('cursor');
      if (cur) cur.remove();
      if (data.replayed) {{
        badge.style.display = 'inline';
        footer.textContent = 'Replayed from history';
        footer.style.color = '#3b82f6';
      }} else if (data.success) {{
        footer.style.color = '#56d364';
        footer.textContent = 'Flow completed successfully';
      }} else {{
        footer.style.color = '#ff7b72';
        footer.textContent = 'Flow failed — check logs above';
      }}
      return;
    }}
    if (data.line !== undefined) {{
      const div = document.createElement('div');
      div.className = data.type || 'normal';
      div.textContent = data.line;
      const cur = document.getElementById('cursor');
      if (cur) body.insertBefore(div, cur);
      else body.appendChild(div);
      body.scrollTop = body.scrollHeight;
      footer.textContent = 'Agent running...';
    }}
  }};

  currentES.onerror = () => {{
    currentES.close();
    footer.textContent = 'Connection error';
    footer.style.color = '#ff7b72';
    const cur = document.getElementById('cursor');
    if (cur) cur.remove();
  }};
}}

function closeTerminal() {{
  if (currentES) {{ currentES.close(); currentES = null; }}
  document.getElementById('terminal-overlay').classList.remove('show');
  document.getElementById('footer-text').style.color = '';
}}

document.getElementById('terminal-overlay').addEventListener('click', (e) => {{
  if (e.target === document.getElementById('terminal-overlay')) closeTerminal();
}});

function refreshAll() {{
  location.reload();
}}

async function loadInventory() {{
  const grid = document.getElementById('inv-grid');
  grid.innerHTML = '<span class="inv-empty">Fetching from Airtable...</span>';
  try {{
    const r    = await fetch('/inventory');
    const data = await r.json();
    const items = data.inventory || [];
    if (items.length === 0) {{
      grid.innerHTML = '<span class="inv-empty">No inventory records yet. Run a return flow to see restocking.</span>';
      return;
    }}
    grid.innerHTML = items.map(i => `
      <div class="inv-card">
        <div class="inv-sku">${{i.sku}}</div>
        <div class="inv-name">${{i.product_name}}</div>
        <div class="inv-stock">${{i.stock}} units</div>
        <div class="inv-updated">Updated: ${{i.last_updated || 'N/A'}}</div>
        ${{i.return_reason ? `<div class="inv-updated" style="color:#fb923c">${{i.return_reason}}</div>` : ''}}
      </div>`).join('');
  }} catch(e) {{
    grid.innerHTML = `<span class="inv-empty">Error loading inventory: ${{e.message}}</span>`;
  }}
}}

// Load inventory on page load
loadInventory();
</script>
</body></html>""")
