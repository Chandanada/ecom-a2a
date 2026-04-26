"""
Order Service — 15 pre-loaded orders + built-in A2A agent trigger.
Dashboard has "Run Agent" button per order — no local command needed.
POST /run-agent/{order_id} → calls logistics agent → updates order → dashboard refreshes.
"""
import os, uuid, httpx, asyncio
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse

app = FastAPI(title="Ecom Order Service", version="2.0.0")

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

# Track which orders are currently being processed
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


# ── A2A Agent Trigger — runs entirely on Render, no local command needed ──────
@app.post("/run-agent/{order_id}")
async def run_agent(order_id: str, background_tasks: BackgroundTasks):
    if order_id not in ORDERS:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    if ORDERS[order_id]["status"] == "shipped":
        return {"status": "already_shipped", "order_id": order_id}
    if order_id in PROCESSING:
        return {"status": "already_processing", "order_id": order_id}

    PROCESSING.add(order_id)
    background_tasks.add_task(_run_agent_flow, order_id)
    return {"status": "processing", "order_id": order_id, "message": "Agent triggered. Dashboard will update in ~10s."}


async def _run_agent_flow(order_id: str):
    """Full A2A flow: discover logistics agent → SendMessage → get AWB → update order."""
    try:
        order = ORDERS[order_id]

        # Step 1: Discover logistics agent via A2A agent card
        async with httpx.AsyncClient(timeout=30.0) as client:
            card_r = await client.get(f"{LOGISTICS_AGENT_URL}/.well-known/agent-card.json")
            card   = card_r.json()

        agent_url = card.get("supportedInterfaces", [{}])[0].get("url", LOGISTICS_AGENT_URL)

        # Step 2: A2A SendMessage — request AWB generation
        rid     = f"req-{uuid.uuid4().hex[:8]}"
        payload = {
            "jsonrpc": "2.0", "id": rid, "method": "SendMessage",
            "params": {"message": {"role": "user", "messageId": str(uuid.uuid4()),
                "parts": [
                    {"kind": "text", "text": f"Generate AWB for {order_id}"},
                    {"kind": "data", "data": {"order": order}, "mediaType": "application/json"}
                ]}}
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp_r = await client.post(agent_url, json=payload)
            resp   = resp_r.json()

        # Step 3: Extract AWB from A2A artifact
        artifacts = resp.get("result", {}).get("task", {}).get("artifacts", [])
        awb_data  = None
        for a in artifacts:
            for p in a.get("parts", []):
                if "data" in p:
                    awb_data = p["data"]
                    break

        if not awb_data or not awb_data.get("awb"):
            PROCESSING.discard(order_id)
            return

        # Step 4: Update order in-memory — dashboard picks this up on next refresh
        awb          = awb_data["awb"]
        carrier      = awb_data.get("carrier", "Delhivery")
        tracking_url = awb_data.get("tracking_url", f"https://www.delhivery.com/track/package/{awb}")

        ORDERS[order_id].update({
            "status":       "shipped",
            "awb":          awb,
            "carrier":      carrier,
            "tracking_url": tracking_url,
            "updated_at":   datetime.utcnow().isoformat() + "Z"
        })
        PROCESSING.discard(order_id)

    except Exception:
        PROCESSING.discard(order_id)


def _rows():
    html = ""
    for o in ORDERS.values():
        s     = o["status"]
        oid   = o["id"]
        awb   = o.get("awb") or ""
        car   = o.get("carrier") or ""
        track = o.get("tracking_url") or ""
        items = ", ".join(f"{i['name']} x{i['qty']}" for i in o.get("items", []))
        city  = o["shipping_address"].get("city", "")

        if s == "shipped":
            sb  = '<span class="badge s">✅ SHIPPED</span>'
            ab  = f'<span class="awb">{awb}</span>'
            cb  = f'<span class="car">{car}</span>'
            tb  = f'<a class="trk" href="{track}" target="_blank">Track →</a>'
            btn = '<span class="done">✓ Done</span>'
        elif oid in PROCESSING:
            sb  = '<span class="badge w">⚙ PROCESSING...</span>'
            ab  = cb = tb = '<span class="d">—</span>'
            btn = '<button class="btn-proc" disabled>Running...</button>'
        else:
            sb  = '<span class="badge p">⏳ PENDING AWB</span>'
            ab  = cb = tb = '<span class="d">—</span>'
            btn = f'<button class="btn-run" onclick="runAgent(\'{oid}\', this)">▶ Run Agent</button>'

        html += f'<tr class="{s}"><td><b>{oid}</b><br><small>{o["created_at"][:10]}</small></td><td><b>{o["customer"]["name"]}</b><br><small>{o["customer"]["email"]}</small></td><td class="itm">{items}</td><td>{city}</td><td><b>₹{o["total"]:,}</b></td><td>{sb}</td><td>{ab}</td><td>{cb}</td><td>{tb}</td><td>{btn}</td></tr>'
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
.live{{font-size:10px;font-weight:700;padding:3px 9px;background:rgba(16,185,129,.12);border:1px solid rgba(16,185,129,.3);color:#10b981;border-radius:20px;animation:p 2s infinite}}
@keyframes p{{0%,100%{{opacity:1}}50%{{opacity:.5}}}}
.sub{{font-size:12px;color:#4a5568;margin-bottom:18px}}
.stats{{display:flex;gap:10px;margin-bottom:18px}}
.sc{{background:#111827;border:1px solid #1e2d45;border-radius:10px;padding:12px 18px;min-width:130px}}
.sl{{font-size:9px;font-weight:700;color:#4a5568;text-transform:uppercase;letter-spacing:1px;margin-bottom:3px}}
.sv{{font-size:22px;font-weight:700}}
.sv.p{{color:#f59e0b}}.sv.s{{color:#10b981}}.sv.r{{color:#a78bfa}}
table{{width:100%;border-collapse:collapse;background:#111827;border-radius:12px;overflow:hidden;border:1px solid #1e2d45}}
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
.btn-run{{background:rgba(16,185,129,.15);border:1px solid rgba(16,185,129,.4);color:#10b981;padding:5px 12px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer;white-space:nowrap}}
.btn-run:hover{{background:rgba(16,185,129,.3)}}
.btn-proc{{background:rgba(99,102,241,.1);border:1px solid rgba(99,102,241,.3);color:#818cf8;padding:5px 12px;border-radius:6px;font-size:11px;font-weight:700;cursor:not-allowed}}
.done{{font-size:11px;color:#10b981;font-weight:600}}
.foot{{font-size:11px;color:#2d3748;text-align:right;margin-top:10px}}
</style>
<script>
// Auto-refresh every 4 seconds
setInterval(() => location.reload(), 4000);

async function runAgent(orderId, btn) {{
    btn.disabled = true;
    btn.textContent = 'Starting...';
    btn.className = 'btn-proc';
    try {{
        const r = await fetch('/run-agent/' + orderId, {{method: 'POST'}});
        const d = await r.json();
        btn.textContent = 'Running...';
    }} catch(e) {{
        btn.textContent = 'Error';
    }}
}}
</script>
</head><body>
<h1>📦 Ecom Order Dashboard <span class="live">● LIVE</span></h1>
<p class="sub" style="margin-top:6px">Auto-refreshes every 4s &nbsp;·&nbsp; Click <b style="color:#10b981">▶ Run Agent</b> on any pending order to trigger A2A logistics agent</p>
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
<p class="foot">Dashboard auto-refreshes · Agent runs on Render · No local setup needed</p>
</body></html>""")
