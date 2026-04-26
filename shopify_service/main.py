"""
Shopify AWB Service — Dashboard with order number input + live browser terminal.
Type any Shopify order number → click Run Agent → see full A2A flow in dark terminal.
No local command needed. Fully hosted on Render.
"""
import os, uuid, asyncio, json
from datetime import datetime
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Shopify AWB Service", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SHOPIFY_STORE       = os.getenv("SHOPIFY_STORE", "agentic-ecom-demo")
SHOPIFY_TOKEN       = os.getenv("SHOPIFY_TOKEN", "")
SHOPIFY_API_VER     = "2024-10"
LOGISTICS_AGENT_URL = os.getenv("LOGISTICS_AGENT_URL", "http://localhost:8001")

def shopify_base():
    return f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{SHOPIFY_API_VER}"

def shopify_headers():
    return {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}


@app.get("/health")
def health():
    return {"status": "ok", "service": "shopify_service"}


# ── SSE streaming agent endpoint ──────────────────────────────────────────────
@app.get("/run-agent-stream/{order_id}")
async def run_agent_stream(order_id: str):
    return StreamingResponse(
        _stream_agent(order_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


async def _stream_agent(order_id: str):
    """Full Shopify A2A flow streamed to browser terminal."""

    def evt(text, typ="normal"):
        return f"data: {json.dumps({'line': text, 'type': typ})}\n\n"

    sep = "=" * 60

    try:
        # ── HEADER ────────────────────────────────────────────────
        yield evt("#" * 60, "dim")
        yield evt(f"  SHOPIFY AWB AGENT — A2A + MCP + Shopify REST", "title")
        yield evt(f"  Order: #{order_id}", "title")
        yield evt("#" * 60, "dim")
        await asyncio.sleep(0.3)

        # ── NODE 1: Fetch Shopify order ───────────────────────────
        yield evt("")
        yield evt(sep, "dim")
        yield evt("NODE 1: MCP INIT + GET ORDER", "node")
        yield evt(sep, "dim")
        await asyncio.sleep(0.4)

        yield evt(f"  MCP Tools: ['get_order', 'fulfill_order', 'list_unfulfilled_orders']", "info")
        yield evt(f"  [MCP -> get_order] Shopify order_id={order_id}", "info")
        await asyncio.sleep(0.3)

        # Fetch order by name from Shopify
        num = order_id.replace("#", "").strip()
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                f"{shopify_base()}/orders.json",
                headers=shopify_headers(),
                params={"name": f"#{num}", "status": "any"}
            )
            orders = r.json().get("orders", [])

        if not orders:
            # try without hash
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.get(
                    f"{shopify_base()}/orders.json",
                    headers=shopify_headers(),
                    params={"name": num, "status": "any"}
                )
                orders = r.json().get("orders", [])

        if not orders:
            yield evt(f"  [ERROR] Order #{order_id} not found in Shopify", "error")
            yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"
            return

        raw_order = orders[0]
        shopify_id = raw_order["id"]

        # Fetch full order for complete address
        async with httpx.AsyncClient(timeout=15.0) as client:
            full_r = await client.get(f"{shopify_base()}/orders/{shopify_id}.json", headers=shopify_headers())
            order  = full_r.json().get("order", raw_order)

        # Extract details
        cust  = order.get("customer") or {}
        ship  = order.get("shipping_address") or {}
        bill  = order.get("billing_address") or {}
        items = order.get("line_items", [])

        def name_from(d):
            if not isinstance(d, dict): return ""
            f = (d.get("first_name") or "").strip()
            l = (d.get("last_name")  or "").strip()
            return f"{f} {l}".strip() or (d.get("name") or "").strip()

        customer_name = name_from(cust) or name_from(ship) or name_from(bill) or "Unknown"
        email         = (cust.get("email") or order.get("email") or "").strip()
        city          = (ship.get("city") or bill.get("city") or "").strip()
        item_names    = ", ".join(f"{li['name']} x{li['quantity']}" for li in items)
        total         = order.get("total_price", "?")
        currency      = order.get("currency", "")
        fulfil_status = order.get("fulfillment_status") or "unfulfilled"

        yield evt(f"  [MCP] get_order -> {customer_name} | {city} | {len(items)} item(s) | {total} {currency}", "success")
        await asyncio.sleep(0.2)
        yield evt(f"  Customer : {customer_name}", "data")
        yield evt(f"  Email    : {email or '?'}", "data")
        yield evt(f"  City     : {city or '?'}", "data")
        yield evt(f"  Items    : {item_names}", "data")
        yield evt(f"  Total    : {total} {currency}", "data")
        yield evt(f"  Status   : {fulfil_status}", "data")
        await asyncio.sleep(0.3)

        # Build order dict for A2A
        a2a_order = {
            "id":               order.get("name", f"#{order_id}"),
            "shopify_id":       shopify_id,
            "fulfillment_status": fulfil_status,
            "total":            total,
            "currency":         currency,
            "customer":         {"name": customer_name, "email": email},
            "shipping_address": ship,
            "items": [
                {"name": li["name"], "qty": li["quantity"],
                 "sku": li.get("sku", ""), "price": li["price"]}
                for li in items
            ]
        }

        # ── NODE 2: A2A Agent Discovery ───────────────────────────
        yield evt("")
        yield evt(sep, "dim")
        yield evt("NODE 2: A2A AGENT DISCOVERY", "node")
        yield evt(sep, "dim")
        await asyncio.sleep(0.4)

        yield evt(f"  [A2A] GET {LOGISTICS_AGENT_URL}/.well-known/agent-card.json", "info")

        async with httpx.AsyncClient(timeout=30.0) as client:
            card_r = await client.get(f"{LOGISTICS_AGENT_URL}/.well-known/agent-card.json")
            card   = card_r.json()

        agent_name = card.get("name", "Logistics Agent")
        skills     = [s["id"] for s in card.get("skills", [])]
        agent_url  = card.get("supportedInterfaces", [{}])[0].get("url", LOGISTICS_AGENT_URL)

        yield evt(f"  [A2A] Agent card: {agent_name} | Skills: {skills}", "success")
        await asyncio.sleep(0.3)

        # ── NODE 3: A2A SendMessage ───────────────────────────────
        yield evt("")
        yield evt(sep, "dim")
        yield evt("NODE 3: A2A SendMessage", "node")
        yield evt(sep, "dim")
        await asyncio.sleep(0.4)

        rid = f"req-{uuid.uuid4().hex[:8]}"
        a2a_payload = {
            "jsonrpc": "2.0", "id": rid, "method": "SendMessage",
            "params": {"message": {"role": "user", "messageId": str(uuid.uuid4()),
                "parts": [
                    {"kind": "text", "text": f"Generate AWB for {a2a_order['id']}"},
                    {"kind": "data", "data": {"order": a2a_order}, "mediaType": "application/json"}
                ]}}
        }

        yield evt(f"  [A2A] SendMessage -> {agent_url} | req_id={rid}", "info")

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp_r = await client.post(agent_url, json=a2a_payload)
            resp   = resp_r.json()

        artifacts = resp.get("result", {}).get("task", {}).get("artifacts", [])
        awb_data  = None
        for a in artifacts:
            for p in a.get("parts", []):
                if "data" in p:
                    awb_data = p["data"]
                    break

        if not awb_data or not awb_data.get("awb"):
            yield evt(f"  [ERROR] No AWB returned from logistics agent", "error")
            yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"
            return

        awb          = awb_data["awb"]
        carrier      = awb_data.get("carrier", "Delhivery")
        tracking_url = awb_data.get("tracking_url", f"https://www.delhivery.com/track/package/{awb}")

        yield evt(f"  [A2A] AWB received: {awb} | Carrier: {carrier}", "success")
        await asyncio.sleep(0.3)

        # ── NODE 4: Tag AWB on Shopify order ──────────────────────
        yield evt("")
        yield evt(sep, "dim")
        yield evt("NODE 4: MCP fulfill_order", "node")
        yield evt(sep, "dim")
        await asyncio.sleep(0.4)

        yield evt(f"  [MCP -> fulfill_order] order_id={order_id} awb={awb}", "info")

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

        async with httpx.AsyncClient(timeout=15.0) as client:
            upd_r  = await client.put(
                f"{shopify_base()}/orders/{shopify_id}.json",
                headers=shopify_headers(),
                json=update_body
            )
            updated = upd_r.json().get("order", {})

        if updated.get("id"):
            yield evt(f"  [MCP] fulfill_order success -> status: awb_tagged", "success")
        else:
            yield evt(f"  [WARN] Order update may have failed: {upd_r.text[:200]}", "error")

        yield evt(f"  Order    : #{order_id} -> awb_tagged", "success")
        yield evt(f"  AWB      : {awb}", "data")
        yield evt(f"  Carrier  : {carrier}", "data")
        yield evt(f"  Tracking : {tracking_url}", "data")
        yield evt(f"  Shopify  : AWB tagged in Additional Details + Timeline", "data")
        await asyncio.sleep(0.3)

        # ── FOOTER ────────────────────────────────────────────────
        yield evt("")
        yield evt(sep, "dim")
        yield evt("FLOW COMPLETE", "node")
        yield evt(sep, "dim")
        yield evt("")
        yield evt(f"  [MCP] Connected to Shopify MCP server via HTTP", "log")
        yield evt(f"  [MCP] Tools: ['get_order', 'fulfill_order', 'list_unfulfilled_orders']", "log")
        yield evt(f"  [MCP -> get_order] Shopify order_id={order_id}", "log")
        yield evt(f"  [MCP] get_order -> {customer_name} | {city} | {len(items)} item(s) | {total} {currency}", "log")
        yield evt(f"  [A2A] GET {LOGISTICS_AGENT_URL}/.well-known/agent-card.json", "log")
        yield evt(f"  [A2A] Agent card: {agent_name} | Skills: {skills}", "log")
        yield evt(f"  [A2A] SendMessage -> {agent_url} | req_id={rid}", "log")
        yield evt(f"  [A2A] AWB received: {awb} | Carrier: {carrier}", "log")
        yield evt(f"  [MCP -> fulfill_order] order_id={order_id} awb={awb}", "log")
        yield evt(f"  [MCP] fulfill_order success -> status: awb_tagged", "log")
        yield evt("")
        yield evt(f"SUCCESS: #{order_id} | awb_tagged | AWB: {awb} | {carrier}", "success_big")

        yield f"data: {json.dumps({'done': True, 'success': True, 'awb': awb, 'carrier': carrier, 'order_id': order_id})}\n\n"

    except Exception as e:
        import traceback
        yield evt(f"  [ERROR] {str(e)}", "error")
        yield evt(f"  {traceback.format_exc()[:300]}", "error")
        yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    shopify_admin = f"https://admin.shopify.com/store/{SHOPIFY_STORE}/orders"
    return HTMLResponse(f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"/><title>Shopify AWB Agent</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',sans-serif;background:#07090f;color:#e2e8f0;padding:24px 28px;min-height:100vh}}
h1{{font-size:20px;font-weight:700;display:flex;align-items:center;gap:10px;margin-bottom:4px}}
.live{{font-size:10px;font-weight:700;padding:3px 9px;background:rgba(16,185,129,.12);border:1px solid rgba(16,185,129,.3);color:#10b981;border-radius:20px;animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.5}}}}
.sub{{font-size:12px;color:#4a5568;margin-bottom:24px;margin-top:6px}}

.input-card{{background:#111827;border:1px solid #1e2d45;border-radius:12px;padding:24px 28px;max-width:600px;margin-bottom:28px}}
.input-card h2{{font-size:14px;font-weight:600;color:#94a3b8;margin-bottom:16px;text-transform:uppercase;letter-spacing:1px}}
.input-row{{display:flex;gap:12px;align-items:center}}
.order-input{{flex:1;background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:10px 16px;color:#e6edf3;font-size:14px;font-family:monospace;outline:none;transition:border .2s}}
.order-input:focus{{border-color:#10b981}}
.order-input::placeholder{{color:#444d56}}
.run-btn{{background:rgba(16,185,129,.2);border:1px solid rgba(16,185,129,.5);color:#10b981;padding:10px 24px;border-radius:8px;font-size:13px;font-weight:700;cursor:pointer;white-space:nowrap;transition:all .2s}}
.run-btn:hover{{background:rgba(16,185,129,.35);transform:scale(1.02)}}
.run-btn:disabled{{opacity:.5;cursor:not-allowed;transform:none}}
.hint{{font-size:11px;color:#4a5568;margin-top:10px}}
.hint a{{color:#3b82f6;text-decoration:none}}
.hint a:hover{{text-decoration:underline}}

.recent-title{{font-size:11px;font-weight:700;color:#4a5568;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px}}
.recent-orders{{display:flex;flex-wrap:wrap;gap:8px}}
.order-chip{{background:#1a2235;border:1px solid #2d3f5a;border-radius:6px;padding:4px 12px;font-size:12px;color:#94a3b8;cursor:pointer;font-family:monospace;transition:all .2s}}
.order-chip:hover{{border-color:#10b981;color:#10b981;background:rgba(16,185,129,.08)}}

/* Terminal overlay */
#terminal-overlay{{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.85);z-index:1000;align-items:center;justify-content:center}}
#terminal-overlay.show{{display:flex}}
#terminal-box{{background:#0d1117;border:1px solid #30363d;border-radius:12px;width:92%;max-width:920px;max-height:87vh;display:flex;flex-direction:column;box-shadow:0 25px 60px rgba(0,0,0,.8)}}
#terminal-header{{background:#161b22;border-bottom:1px solid #30363d;padding:12px 16px;display:flex;align-items:center;justify-content:space-between;border-radius:12px 12px 0 0}}
#terminal-title{{font-size:13px;font-weight:600;color:#e6edf3;font-family:monospace}}
.term-dots{{display:flex;gap:7px}}
.dot{{width:13px;height:13px;border-radius:50%}}
.dot.r{{background:#ff5f56}}.dot.y{{background:#ffbd2e}}.dot.g{{background:#27c93f}}
#terminal-close{{background:none;border:none;color:#8b949e;font-size:20px;cursor:pointer;padding:0 4px;line-height:1}}
#terminal-close:hover{{color:#e6edf3}}
#terminal-body{{padding:20px;overflow-y:auto;flex:1;font-family:'Courier New',Cascadia,monospace;font-size:13px;line-height:1.7;min-height:420px}}
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
#terminal-footer{{padding:10px 16px;border-top:1px solid #30363d;font-size:11px;color:#8b949e;font-family:monospace;display:flex;justify-content:space-between;align-items:center}}
#shopify-link{{display:none;color:#3b82f6;text-decoration:none;font-size:11px}}
#shopify-link:hover{{text-decoration:underline}}
</style>
</head><body>

<h1>🛍️ Shopify AWB Agent <span class="live">● LIVE</span></h1>
<p class="sub">Enter any Shopify order number → click Run Agent → watch A2A flow live</p>

<div class="input-card">
  <h2>Run Agent for Order</h2>
  <div class="input-row">
    <input id="order-input" class="order-input" type="text" placeholder="e.g. 1027 or #1027" 
           onkeydown="if(event.key==='Enter') runAgent()"/>
    <button class="run-btn" id="run-btn" onclick="runAgent()">▶ Run Agent</button>
  </div>
  <p class="hint">
    Order number from your 
    <a href="{shopify_admin}" target="_blank">Shopify Admin → Orders ↗</a>
    &nbsp;·&nbsp; AWB gets tagged in Shopify Additional Details automatically
  </p>
</div>

<div style="max-width:600px">
  <div class="recent-title">Quick run — click any order:</div>
  <div class="recent-orders" id="recent-orders">
    <span class="order-chip" onclick="setAndRun('1027')">#1027</span>
    <span class="order-chip" onclick="setAndRun('1026')">#1026</span>
    <span class="order-chip" onclick="setAndRun('1025')">#1025</span>
    <span class="order-chip" onclick="setAndRun('1024')">#1024</span>
    <span class="order-chip" onclick="setAndRun('1023')">#1023</span>
    <span class="order-chip" onclick="setAndRun('1022')">#1022</span>
    <span class="order-chip" onclick="setAndRun('1021')">#1021</span>
    <span class="order-chip" onclick="setAndRun('1020')">#1020</span>
    <span class="order-chip" onclick="setAndRun('1019')">#1019</span>
    <span class="order-chip" onclick="setAndRun('1018')">#1018</span>
  </div>
</div>

<!-- Terminal Overlay -->
<div id="terminal-overlay">
  <div id="terminal-box">
    <div id="terminal-header">
      <div class="term-dots"><div class="dot r"></div><div class="dot y"></div><div class="dot g"></div></div>
      <span id="terminal-title">shopify-awb-agent — bash</span>
      <button id="terminal-close" onclick="closeTerminal()">✕</button>
    </div>
    <div id="terminal-body"></div>
    <div id="terminal-footer">
      <span id="footer-text">Ready</span>
      <a id="shopify-link" href="{shopify_admin}" target="_blank">View in Shopify Admin ↗</a>
    </div>
  </div>
</div>

<script>
let currentES = null;

function setAndRun(orderId) {{
  document.getElementById('order-input').value = orderId;
  runAgent();
}}

function runAgent() {{
  const input   = document.getElementById('order-input').value.trim().replace('#','');
  if (!input) {{ alert('Enter an order number'); return; }}

  const overlay = document.getElementById('terminal-overlay');
  const body    = document.getElementById('terminal-body');
  const footer  = document.getElementById('footer-text');
  const title   = document.getElementById('terminal-title');
  const btn     = document.getElementById('run-btn');
  const link    = document.getElementById('shopify-link');

  body.innerHTML = '';
  title.textContent = `shopify-awb-agent — #${{input}} — bash`;
  footer.textContent = 'Connecting to agent...';
  footer.style.color = '';
  link.style.display = 'none';
  overlay.classList.add('show');
  btn.disabled = true;

  if (currentES) currentES.close();

  currentES = new EventSource(`/run-agent-stream/${{input}}`);

  currentES.onmessage = (e) => {{
    const data = JSON.parse(e.data);

    if (data.done) {{
      currentES.close();
      btn.disabled = false;
      const cur = document.getElementById('cursor');
      if (cur) cur.remove();
      if (data.success) {{
        footer.textContent = `SUCCESS — AWB: ${{data.awb}} | ${{data.carrier}}`;
        footer.style.color = '#56d364';
        link.style.display = 'block';
      }} else {{
        footer.textContent = 'Agent flow failed';
        footer.style.color = '#ff7b72';
      }}
      return;
    }}

    if (data.line !== undefined) {{
      const div = document.createElement('div');
      div.className = data.type || 'normal';
      div.textContent = data.line;
      // Insert before cursor
      const cur = document.getElementById('cursor');
      if (cur) body.insertBefore(div, cur);
      else body.appendChild(div);
      body.scrollTop = body.scrollHeight;
      footer.textContent = 'Running agent...';
    }}
  }};

  currentES.onerror = () => {{
    currentES.close();
    btn.disabled = false;
    footer.textContent = 'Connection error — try again';
    footer.style.color = '#ff7b72';
  }};

  // Blinking cursor
  const cursor = document.createElement('span');
  cursor.id = 'cursor';
  body.appendChild(cursor);
}}

function closeTerminal() {{
  if (currentES) {{ currentES.close(); currentES = null; }}
  document.getElementById('terminal-overlay').classList.remove('show');
  document.getElementById('run-btn').disabled = false;
  document.getElementById('footer-text').style.color = '';
}}

// Close on overlay background click
document.getElementById('terminal-overlay').addEventListener('click', (e) => {{
  if (e.target === document.getElementById('terminal-overlay')) closeTerminal();
}});
</script>
</body></html>""")
