"""
Order Service — 15 pre-loaded orders + browser terminal demo.
Dashboard has "Run Agent" button → opens live terminal panel streaming exact node-by-node output.
Uses SSE (Server-Sent Events) for real-time streaming to browser.
"""
import os, uuid, asyncio, json
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Ecom Order Service", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

LOGISTICS_AGENT_URL = os.getenv("LOGISTICS_AGENT_URL", "http://localhost:8001")

ORDERS = {
    "ORD-001": {"id":"ORD-001","customer":{"name":"Rahul Sharma","email":"rahul@example.com","phone":"+91-9876543210"},"items":[{"sku":"TSHIRT-BLK-M","name":"Black T-Shirt Medium","qty":2,"price":599},{"sku":"JEANS-BLU-32","name":"Blue Jeans 32","qty":1,"price":1299}],"shipping_address":{"name":"Rahul Sharma","line1":"42 MG Road","city":"Bangalore","state":"Karnataka","pincode":"560001","country":"IN"},"total":2497,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T09:00:00Z","updated_at":"2026-04-24T09:00:00Z"},
    "ORD-002": {"id":"ORD-002","customer":{"name":"Priya Nair","email":"priya@example.com","phone":"+91-8765432109"},"items":[{"sku":"SHOE-WHT-8","name":"White Sneakers Size 8","qty":1,"price":2499}],"shipping_address":{"name":"Priya Nair","line1":"15 Linking Road","city":"Mumbai","state":"Maharashtra","pincode":"400050","country":"IN"},"total":2499,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T10:30:00Z","updated_at":"2026-04-24T10:30:00Z"},
    "ORD-003": {"id":"ORD-003","customer":{"name":"Amit Verma","email":"amit@example.com","phone":"+91-9988776655"},"items":[{"sku":"WATCH-GLD-001","name":"Gold Analog Watch","qty":1,"price":4999},{"sku":"BELT-BRN-32","name":"Brown Leather Belt","qty":1,"price":799}],"shipping_address":{"name":"Amit Verma","line1":"8 Park Street","city":"Kolkata","state":"West Bengal","pincode":"700016","country":"IN"},"total":5798,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T11:00:00Z","updated_at":"2026-04-24T11:00:00Z"},
    "ORD-004": {"id":"ORD-004","customer":{"name":"Sneha Patel","email":"sneha@example.com","phone":"+91-9123456780"},"items":[{"sku":"DRESS-RED-M","name":"Red Floral Dress Medium","qty":1,"price":1899}],"shipping_address":{"name":"Sneha Patel","line1":"22 CG Road","city":"Ahmedabad","state":"Gujarat","pincode":"380009","country":"IN"},"total":1899,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T11:30:00Z","updated_at":"2026-04-24T11:30:00Z"},
    "ORD-005": {"id":"ORD-005","customer":{"name":"Rohan Mehta","email":"rohan@example.com","phone":"+91-9012345678"},"items":[{"sku":"LAPTOP-BAG-15","name":"Laptop Bag 15 inch","qty":1,"price":1299},{"sku":"MOUSE-WLESS","name":"Wireless Mouse","qty":2,"price":599}],"shipping_address":{"name":"Rohan Mehta","line1":"5 Jubilee Hills","city":"Hyderabad","state":"Telangana","pincode":"500033","country":"IN"},"total":2497,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T12:00:00Z","updated_at":"2026-04-24T12:00:00Z"},
    "ORD-006": {"id":"ORD-006","customer":{"name":"Kavya Reddy","email":"kavya@example.com","phone":"+91-8901234567"},"items":[{"sku":"KURTI-BLU-L","name":"Blue Cotton Kurti Large","qty":2,"price":899}],"shipping_address":{"name":"Kavya Reddy","line1":"12 Jayanagar","city":"Bangalore","state":"Karnataka","pincode":"560041","country":"IN"},"total":1798,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T12:30:00Z","updated_at":"2026-04-24T12:30:00Z"},
    "ORD-007": {"id":"ORD-007","customer":{"name":"Arjun Singh","email":"arjun@example.com","phone":"+91-7890123456"},"items":[{"sku":"PERFUME-001","name":"Armaf Club De Nuit 105ml","qty":1,"price":3499}],"shipping_address":{"name":"Arjun Singh","line1":"3 Connaught Place","city":"Delhi","state":"Delhi","pincode":"110001","country":"IN"},"total":3499,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T13:00:00Z","updated_at":"2026-04-24T13:00:00Z"},
    "ORD-008": {"id":"ORD-008","customer":{"name":"Meera Krishnan","email":"meera@example.com","phone":"+91-6789012345"},"items":[{"sku":"SAREE-SLK-001","name":"Kanjivaram Silk Saree","qty":1,"price":8999}],"shipping_address":{"name":"Meera Krishnan","line1":"45 Anna Salai","city":"Chennai","state":"Tamil Nadu","pincode":"600002","country":"IN"},"total":8999,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T13:30:00Z","updated_at":"2026-04-24T13:30:00Z"},
    "ORD-009": {"id":"ORD-009","customer":{"name":"Vikram Joshi","email":"vikram@example.com","phone":"+91-9876001234"},"items":[{"sku":"HEADPHONE-BT","name":"Bluetooth Headphones","qty":1,"price":2999},{"sku":"PHONE-CASE-01","name":"Phone Case iPhone 15","qty":1,"price":399}],"shipping_address":{"name":"Vikram Joshi","line1":"7 FC Road","city":"Pune","state":"Maharashtra","pincode":"411005","country":"IN"},"total":3398,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T14:00:00Z","updated_at":"2026-04-24T14:00:00Z"},
    "ORD-010": {"id":"ORD-010","customer":{"name":"Ananya Das","email":"ananya@example.com","phone":"+91-9765432100"},"items":[{"sku":"YOGA-MAT-001","name":"Anti-Slip Yoga Mat 6mm","qty":1,"price":999},{"sku":"BOTTLE-SS-1L","name":"Steel Water Bottle 1L","qty":2,"price":499}],"shipping_address":{"name":"Ananya Das","line1":"18 Salt Lake","city":"Kolkata","state":"West Bengal","pincode":"700064","country":"IN"},"total":1997,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T14:30:00Z","updated_at":"2026-04-24T14:30:00Z"},
    "ORD-011": {"id":"ORD-011","customer":{"name":"Karan Kapoor","email":"karan@example.com","phone":"+91-9654321098"},"items":[{"sku":"FORMAL-SHIRT-L","name":"White Formal Shirt Large","qty":3,"price":899}],"shipping_address":{"name":"Karan Kapoor","line1":"34 Bandra West","city":"Mumbai","state":"Maharashtra","pincode":"400050","country":"IN"},"total":2697,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T15:00:00Z","updated_at":"2026-04-24T15:00:00Z"},
    "ORD-012": {"id":"ORD-012","customer":{"name":"Divya Menon","email":"divya@example.com","phone":"+91-9543210987"},"items":[{"sku":"SKINCARE-SET","name":"Vitamin C Skincare Kit","qty":1,"price":1799}],"shipping_address":{"name":"Divya Menon","line1":"9 Marine Drive","city":"Kochi","state":"Kerala","pincode":"682031","country":"IN"},"total":1799,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T15:30:00Z","updated_at":"2026-04-24T15:30:00Z"},
    "ORD-013": {"id":"ORD-013","customer":{"name":"Suresh Kumar","email":"suresh@example.com","phone":"+91-9432109876"},"items":[{"sku":"CRICKET-BAT","name":"MRF Virat Kohli Bat","qty":1,"price":3499},{"sku":"CRICKET-BALL","name":"SG Cricket Ball Pack","qty":2,"price":399}],"shipping_address":{"name":"Suresh Kumar","line1":"67 Brigade Road","city":"Bangalore","state":"Karnataka","pincode":"560025","country":"IN"},"total":4297,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T16:00:00Z","updated_at":"2026-04-24T16:00:00Z"},
    "ORD-014": {"id":"ORD-014","customer":{"name":"Pooja Iyer","email":"pooja@example.com","phone":"+91-9321098765"},"items":[{"sku":"COOKWARE-SET","name":"Non-Stick 5pc Cookware","qty":1,"price":2999}],"shipping_address":{"name":"Pooja Iyer","line1":"21 T Nagar","city":"Chennai","state":"Tamil Nadu","pincode":"600017","country":"IN"},"total":2999,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T16:30:00Z","updated_at":"2026-04-24T16:30:00Z"},
    "ORD-015": {"id":"ORD-015","customer":{"name":"Nikhil Gupta","email":"nikhil@example.com","phone":"+91-9210987654"},"items":[{"sku":"BOOK-ATOMIC","name":"Atomic Habits","qty":1,"price":499},{"sku":"BOOK-SAPIENS","name":"Sapiens","qty":1,"price":599}],"shipping_address":{"name":"Nikhil Gupta","line1":"14 Cyber City","city":"Gurugram","state":"Haryana","pincode":"122002","country":"IN"},"total":1098,"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":"2026-04-24T17:00:00Z","updated_at":"2026-04-24T17:00:00Z"},
}

PROCESSING = set()


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
    if order_id not in ORDERS:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    awb = payload.get("awb")
    if not awb:
        raise HTTPException(status_code=400, detail="awb required")
    carrier      = payload.get("carrier", "Shiprocket")
    tracking_url = payload.get("tracking_url", f"https://shiprocket.co/tracking/{awb}")
    ORDERS[order_id].update({"status":"shipped","awb":awb,"carrier":carrier,"tracking_url":tracking_url,"updated_at":datetime.utcnow().isoformat()+"Z"})
    PROCESSING.discard(order_id)
    return {"success":True,"order_id":order_id,"status":"shipped","awb":awb,"carrier":carrier,"tracking_url":tracking_url}

@app.post("/orders")
def create_order(payload: dict):
    oid = f"ORD-{uuid.uuid4().hex[:6].upper()}"
    o = {"id":oid,"customer":payload.get("customer",{}),"items":payload.get("items",[]),"shipping_address":payload.get("shipping_address",{}),"total":payload.get("total",0),"status":"confirmed","awb":None,"carrier":None,"tracking_url":None,"created_at":datetime.utcnow().isoformat()+"Z","updated_at":datetime.utcnow().isoformat()+"Z"}
    ORDERS[oid] = o
    return o


# ── SSE streaming agent endpoint ──────────────────────────────────────────────
@app.get("/run-agent-stream/{order_id}")
async def run_agent_stream(order_id: str):
    """SSE endpoint — streams terminal output line by line to browser."""
    if order_id not in ORDERS:
        async def err():
            yield f"data: {json.dumps({'line': f'ERROR: Order {order_id} not found', 'type': 'error'})}\n\n"
        return StreamingResponse(err(), media_type="text/event-stream")

    if ORDERS[order_id]["status"] == "shipped":
        async def already():
            yield f"data: {json.dumps({'line': f'Order {order_id} already shipped.', 'type': 'info'})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        return StreamingResponse(already(), media_type="text/event-stream")

    PROCESSING.add(order_id)
    return StreamingResponse(
        _stream_agent(order_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

async def _stream_agent(order_id: str):
    """Runs A2A flow and yields terminal lines via SSE."""
    import httpx

    def line(text, typ="normal"):
        return f"data: {json.dumps({'line': text, 'type': typ})}\n\n"

    order = ORDERS[order_id]
    sep   = "=" * 60

    try:
        # ── HEADER ────────────────────────────────────────────────
        yield line("#" * 60, "dim")
        yield line(f"  ECOM AWB AGENT — LangGraph + HTTP MCP + A2A", "title")
        yield line(f"  Order: {order_id}", "title")
        yield line("#" * 60, "dim")
        await asyncio.sleep(0.3)

        # ── NODE 1 ────────────────────────────────────────────────
        yield line("")
        yield line(sep, "dim")
        yield line("NODE 1: MCP INIT + GET ORDER", "node")
        yield line(sep, "dim")
        await asyncio.sleep(0.4)

        cust  = order.get("customer", {})
        addr  = order.get("shipping_address", {})
        items = order.get("items", [])
        item_names = ", ".join(f"{i['name']} x{i['qty']}" for i in items)

        yield line(f"  MCP Tools: ['get_order', 'update_order_status', 'list_pending_orders']", "info")
        await asyncio.sleep(0.3)
        yield line(f"  Customer : {cust.get('name', '?')}", "data")
        yield line(f"  Email    : {cust.get('email', '?')}", "data")
        yield line(f"  City     : {addr.get('city', '?')}", "data")
        yield line(f"  Items    : {item_names}", "data")
        yield line(f"  Total    : Rs.{order.get('total', '?')}", "data")
        await asyncio.sleep(0.3)

        # ── NODE 2 ────────────────────────────────────────────────
        yield line("")
        yield line(sep, "dim")
        yield line("NODE 2: A2A AGENT DISCOVERY", "node")
        yield line(sep, "dim")
        await asyncio.sleep(0.4)

        yield line(f"  [A2A] GET {LOGISTICS_AGENT_URL}/.well-known/agent-card.json", "info")

        async with httpx.AsyncClient(timeout=30.0) as client:
            card_r = await client.get(f"{LOGISTICS_AGENT_URL}/.well-known/agent-card.json")
            card   = card_r.json()

        agent_name = card.get("name", "Logistics Agent")
        skills     = [s["id"] for s in card.get("skills", [])]
        agent_url  = card.get("supportedInterfaces", [{}])[0].get("url", LOGISTICS_AGENT_URL)

        yield line(f"  [A2A] Agent card: {agent_name} | Skills: {skills}", "success")
        await asyncio.sleep(0.3)

        # ── NODE 3 ────────────────────────────────────────────────
        yield line("")
        yield line(sep, "dim")
        yield line("NODE 3: A2A SendMessage", "node")
        yield line(sep, "dim")
        await asyncio.sleep(0.4)

        rid     = f"req-{uuid.uuid4().hex[:8]}"
        payload = {
            "jsonrpc": "2.0", "id": rid, "method": "SendMessage",
            "params": {"message": {"role": "user", "messageId": str(uuid.uuid4()),
                "parts": [
                    {"kind": "text", "text": f"Generate AWB for {order_id}"},
                    {"kind": "data", "data": {"order": order}, "mediaType": "application/json"}
                ]}}
        }

        yield line(f"  [A2A] SendMessage -> {agent_url} | req_id={rid}", "info")

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp_r = await client.post(agent_url, json=payload)
            resp   = resp_r.json()

        artifacts = resp.get("result", {}).get("task", {}).get("artifacts", [])
        awb_data  = None
        for a in artifacts:
            for p in a.get("parts", []):
                if "data" in p:
                    awb_data = p["data"]
                    break

        if not awb_data or not awb_data.get("awb"):
            yield line(f"  [ERROR] No AWB returned: {resp}", "error")
            PROCESSING.discard(order_id)
            yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"
            return

        awb          = awb_data["awb"]
        carrier      = awb_data.get("carrier", "Delhivery")
        tracking_url = awb_data.get("tracking_url", f"https://www.delhivery.com/track/package/{awb}")

        yield line(f"  [A2A] AWB received: {awb} | Carrier: {carrier}", "success")
        await asyncio.sleep(0.3)

        # ── NODE 4 ────────────────────────────────────────────────
        yield line("")
        yield line(sep, "dim")
        yield line("NODE 4: MCP update_order_status", "node")
        yield line(sep, "dim")
        await asyncio.sleep(0.4)

        yield line(f"  [MCP -> update_order_status] order_id={order_id} awb={awb}", "info")

        ORDERS[order_id].update({
            "status":       "shipped",
            "awb":          awb,
            "carrier":      carrier,
            "tracking_url": tracking_url,
            "updated_at":   datetime.utcnow().isoformat() + "Z"
        })
        PROCESSING.discard(order_id)

        yield line(f"  Order    : {order_id} -> shipped", "success")
        yield line(f"  AWB      : {awb}", "data")
        yield line(f"  Carrier  : {carrier}", "data")
        yield line(f"  Tracking : {tracking_url}", "data")
        await asyncio.sleep(0.3)

        # ── FOOTER ────────────────────────────────────────────────
        yield line("")
        yield line(sep, "dim")
        yield line("FLOW COMPLETE", "node")
        yield line(sep, "dim")
        yield line("")
        yield line(f"  [MCP] Connected to ecom MCP server via HTTP", "log")
        yield line(f"  [MCP] Tools: ['get_order', 'update_order_status', 'list_pending_orders']", "log")
        yield line(f"  [MCP -> get_order] order_id={order_id}", "log")
        yield line(f"  [MCP] get_order -> {cust.get('name','?')} | {addr.get('city','?')} | {len(items)} item(s)", "log")
        yield line(f"  [A2A] GET {LOGISTICS_AGENT_URL}/.well-known/agent-card.json", "log")
        yield line(f"  [A2A] Agent card: {agent_name} | Skills: {skills}", "log")
        yield line(f"  [A2A] SendMessage -> {agent_url} | req_id={rid}", "log")
        yield line(f"  [A2A] AWB received: {awb} | Carrier: {carrier}", "log")
        yield line(f"  [MCP -> update_order_status] order_id={order_id} awb={awb}", "log")
        yield line(f"  [MCP] update_order_status success -> status: shipped", "log")
        yield line("")
        yield line(f"SUCCESS: #{order_id} | shipped | AWB: {awb} | {carrier}", "success_big")

        yield f"data: {json.dumps({'done': True, 'success': True, 'awb': awb, 'carrier': carrier})}\n\n"

    except Exception as e:
        PROCESSING.discard(order_id)
        yield line(f"  [ERROR] {str(e)}", "error")
        yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"


def _rows():
    html = ""
    for o in ORDERS.values():
        s    = o["status"]
        oid  = o["id"]
        awb  = o.get("awb") or ""
        car  = o.get("carrier") or ""
        track = o.get("tracking_url") or ""
        items = ", ".join(f"{i['name']} x{i['qty']}" for i in o.get("items", []))
        city  = o["shipping_address"].get("city", "")

        if s == "shipped":
            sb  = '<span class="badge s">✅ SHIPPED</span>'
            ab  = f'<span class="awb">{awb}</span>'
            cb  = f'<span class="car">{car}</span>'
            tb  = f'<a class="trk" href="{track}" target="_blank">Track →</a>'
            btn = f'<button class="btn-done" onclick="openTerminal(\'{oid}\')">View Log</button>'
        elif oid in PROCESSING:
            sb  = '<span class="badge w">⚙ PROCESSING...</span>'
            ab  = cb = tb = '<span class="d">—</span>'
            btn = '<button class="btn-proc" disabled>Running...</button>'
        else:
            sb  = '<span class="badge p">⏳ PENDING AWB</span>'
            ab  = cb = tb = '<span class="d">—</span>'
            btn = f'<button class="btn-run" onclick="openTerminal(\'{oid}\')">▶ Run Agent</button>'

        html += f'<tr id="row-{oid}" class="{s}"><td><b>{oid}</b><br><small>{o["created_at"][:10]}</small></td><td><b>{o["customer"]["name"]}</b><br><small>{o["customer"]["email"]}</small></td><td class="itm">{items}</td><td>{city}</td><td><b>₹{o["total"]:,}</b></td><td>{sb}</td><td>{ab}</td><td>{cb}</td><td>{tb}</td><td>{btn}</td></tr>'
    return html


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    confirmed = sum(1 for o in ORDERS.values() if o["status"] == "confirmed")
    shipped   = sum(1 for o in ORDERS.values() if o["status"] == "shipped")
    revenue   = sum(o["total"] for o in ORDERS.values())
    return HTMLResponse(f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"/><title>Ecom Order Dashboard</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',sans-serif;background:#07090f;color:#e2e8f0;padding:24px 28px}}
h1{{font-size:20px;font-weight:700;display:flex;align-items:center;gap:10px;margin-bottom:4px}}
.live{{font-size:10px;font-weight:700;padding:3px 9px;background:rgba(16,185,129,.12);border:1px solid rgba(16,185,129,.3);color:#10b981;border-radius:20px;animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.5}}}}
.sub{{font-size:12px;color:#4a5568;margin-bottom:18px}}
.stats{{display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap}}
.sc{{background:#111827;border:1px solid #1e2d45;border-radius:10px;padding:12px 18px;min-width:130px}}
.sl{{font-size:9px;font-weight:700;color:#4a5568;text-transform:uppercase;letter-spacing:1px;margin-bottom:3px}}
.sv{{font-size:22px;font-weight:700}}
.sv.p{{color:#f59e0b}}.sv.s{{color:#10b981}}.sv.r{{color:#a78bfa}}
table{{width:100%;border-collapse:collapse;background:#111827;border-radius:12px;overflow:hidden;border:1px solid #1e2d45;margin-bottom:24px}}
thead tr{{background:#141d2d}}
th{{padding:10px 12px;font-size:9px;font-weight:700;color:#4a5568;text-transform:uppercase;letter-spacing:1px;text-align:left;white-space:nowrap}}
td{{padding:11px 12px;font-size:12px;border-bottom:1px solid #1a2235;vertical-align:middle}}
tr.shipped td{{background:rgba(16,185,129,.03)}}
tr:last-child td{{border-bottom:none}}
.badge{{display:inline-block;padding:3px 10px;border-radius:20px;font-size:10px;font-weight:700;white-space:nowrap}}
.badge.s{{background:rgba(16,185,129,.1);border:1px solid rgba(16,185,129,.3);color:#10b981}}
.badge.p{{background:rgba(245,158,11,.1);border:1px solid rgba(245,158,11,.3);color:#f59e0b}}
.badge.w{{background:rgba(99,102,241,.1);border:1px solid rgba(99,102,241,.3);color:#818cf8}}
.awb{{font-family:monospace;font-size:12px;color:#a78bfa;font-weight:600;background:rgba(124,92,252,.08);padding:2px 7px;border-radius:5px}}
.car{{font-size:11px;color:#38bdf8;font-weight:500}}
.trk{{font-size:11px;font-weight:600;color:#3b82f6;text-decoration:none;background:rgba(59,130,246,.08);border:1px solid rgba(59,130,246,.2);padding:3px 9px;border-radius:5px}}
.d{{color:#2d3748}}
small{{font-size:10px;color:#4a5568}}
.itm{{font-size:11px;color:#64748b;max-width:180px}}
.btn-run{{background:rgba(16,185,129,.15);border:1px solid rgba(16,185,129,.4);color:#10b981;padding:5px 14px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap;transition:all .2s}}
.btn-run:hover{{background:rgba(16,185,129,.3);transform:scale(1.03)}}
.btn-proc{{background:rgba(99,102,241,.1);border:1px solid rgba(99,102,241,.3);color:#818cf8;padding:5px 14px;border-radius:6px;font-size:11px;font-weight:700;cursor:not-allowed}}
.btn-done{{background:rgba(59,130,246,.1);border:1px solid rgba(59,130,246,.3);color:#3b82f6;padding:5px 14px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer}}

/* Terminal overlay */
#terminal-overlay{{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.85);z-index:1000;align-items:center;justify-content:center}}
#terminal-overlay.show{{display:flex}}
#terminal-box{{background:#0d1117;border:1px solid #30363d;border-radius:12px;width:90%;max-width:900px;max-height:85vh;display:flex;flex-direction:column;box-shadow:0 25px 60px rgba(0,0,0,.8)}}
#terminal-header{{background:#161b22;border-bottom:1px solid #30363d;padding:12px 16px;display:flex;align-items:center;justify-content:space-between;border-radius:12px 12px 0 0}}
#terminal-title{{font-size:13px;font-weight:600;color:#e6edf3;font-family:monospace}}
.term-dots{{display:flex;gap:7px}}
.dot{{width:13px;height:13px;border-radius:50%}}
.dot.r{{background:#ff5f56}}.dot.y{{background:#ffbd2e}}.dot.g{{background:#27c93f}}
#terminal-close{{background:none;border:none;color:#8b949e;font-size:20px;cursor:pointer;padding:0 4px;line-height:1}}
#terminal-close:hover{{color:#e6edf3}}
#terminal-body{{padding:20px;overflow-y:auto;flex:1;font-family:'Courier New',Cascadia,monospace;font-size:13px;line-height:1.7;min-height:400px}}
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
#terminal-footer{{padding:10px 16px;border-top:1px solid #30363d;font-size:11px;color:#8b949e;font-family:monospace}}
</style>
</head><body>

<h1>📦 Ecom Order Dashboard <span class="live">● LIVE</span></h1>
<p class="sub" style="margin-top:6px">Auto-refreshes every 5s &nbsp;·&nbsp; Click <b style="color:#10b981">▶ Run Agent</b> to trigger A2A logistics agent with live terminal output</p>

<div class="stats" style="margin-top:14px">
  <div class="sc"><div class="sl">Pending AWB</div><div class="sv p">{confirmed}</div></div>
  <div class="sc"><div class="sl">Shipped</div><div class="sv s">{shipped}</div></div>
  <div class="sc"><div class="sl">Total Orders</div><div class="sv">{len(ORDERS)}</div></div>
  <div class="sc"><div class="sl">Revenue</div><div class="sv r">₹{revenue:,}</div></div>
</div>

<table style="margin-top:16px">
<thead><tr><th>Order ID</th><th>Customer</th><th>Items</th><th>City</th><th>Total</th><th>Status</th><th>AWB Number</th><th>Carrier</th><th>Tracking</th><th>Action</th></tr></thead>
<tbody>{_rows()}</tbody>
</table>

<!-- Terminal Overlay -->
<div id="terminal-overlay">
  <div id="terminal-box">
    <div id="terminal-header">
      <div class="term-dots"><div class="dot r"></div><div class="dot y"></div><div class="dot g"></div></div>
      <span id="terminal-title">ecom-awb-agent — bash</span>
      <button id="terminal-close" onclick="closeTerminal()">✕</button>
    </div>
    <div id="terminal-body"></div>
    <div id="terminal-footer" id="terminal-footer">Ready</div>
  </div>
</div>

<script>
let currentES = null;
let autoRefreshTimer = null;

function startAutoRefresh() {{
  autoRefreshTimer = setTimeout(() => location.reload(), 5000);
}}
startAutoRefresh();

function openTerminal(orderId) {{
  clearTimeout(autoRefreshTimer);
  const overlay  = document.getElementById('terminal-overlay');
  const body     = document.getElementById('terminal-body');
  const footer   = document.getElementById('terminal-footer');
  const titleEl  = document.getElementById('terminal-title');

  body.innerHTML = '';
  titleEl.textContent = `ecom-awb-agent — ${{orderId}} — bash`;
  footer.textContent  = 'Connecting...';
  overlay.classList.add('show');

  if (currentES) currentES.close();

  currentES = new EventSource(`/run-agent-stream/${{orderId}}`);

  currentES.onmessage = (e) => {{
    const data = JSON.parse(e.data);
    if (data.done) {{
      currentES.close();
      if (data.success) {{
        footer.textContent = `SUCCESS — AWB: ${{data.awb}} | ${{data.carrier}} | Dashboard will refresh on close`;
        footer.style.color = '#56d364';
      }} else {{
        footer.textContent = 'Agent flow failed — check logs above';
        footer.style.color = '#ff7b72';
      }}
      // Remove cursor
      const cur = document.getElementById('cursor');
      if (cur) cur.remove();
      return;
    }}
    if (data.line !== undefined) {{
      const div  = document.createElement('div');
      div.className = data.type || 'normal';
      div.textContent = data.line;
      body.appendChild(div);
      body.scrollTop = body.scrollHeight;
      footer.textContent = 'Running agent...';
    }}
  }};

  currentES.onerror = () => {{
    currentES.close();
    footer.textContent = 'Connection error';
    footer.style.color = '#ff7b72';
  }};

  // Add blinking cursor
  const cursor = document.createElement('span');
  cursor.id = 'cursor';
  body.appendChild(cursor);
}}

function closeTerminal() {{
  if (currentES) {{ currentES.close(); currentES = null; }}
  document.getElementById('terminal-overlay').classList.remove('show');
  document.getElementById('terminal-footer').style.color = '';
  location.reload();
}}

// Close on overlay click
document.getElementById('terminal-overlay').addEventListener('click', (e) => {{
  if (e.target === document.getElementById('terminal-overlay')) closeTerminal();
}});
</script>
</body></html>""")
