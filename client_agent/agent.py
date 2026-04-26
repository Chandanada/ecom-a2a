import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid, json, httpx, subprocess, threading
from typing import TypedDict, Annotated, Optional
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

load_dotenv()

LOGISTICS_AGENT_URL = os.getenv("LOGISTICS_AGENT_URL", "http://localhost:8001")
ORDER_SERVICE_URL   = os.getenv("ORDER_SERVICE_URL", "http://localhost:8000")
SHOPIFY_STORE       = os.getenv("SHOPIFY_STORE", "agentic-ecom-demo")
SHOPIFY_TOKEN       = os.getenv("SHOPIFY_TOKEN", "")
SHOPIFY_API_VER     = "2024-10"

os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
os.environ.setdefault("LANGCHAIN_PROJECT", "ecom-awb-a2a")


class MCPClient:
    """Real MCP stdio client — single instance reused across nodes."""
    def __init__(self):
        mcp_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "mcp_tools", "shopify_mcp_server.py"
        )
        env = {**os.environ,
               "SHOPIFY_STORE": SHOPIFY_STORE,
               "SHOPIFY_TOKEN": SHOPIFY_TOKEN}
        self._proc = subprocess.Popen(
            [sys.executable, mcp_path],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env
        )
        self._id = 0
        self._lock = threading.Lock()
        self._initialize()

    def _next_id(self):
        self._id += 1
        return self._id

    def _send(self, msg):
        data = (json.dumps(msg) + "\n").encode("utf-8")
        self._proc.stdin.write(data)
        self._proc.stdin.flush()

    def _recv(self):
        attempts = 0
        while attempts < 200:
            if self._proc.poll() is not None:
                stderr = self._proc.stderr.read()
                if isinstance(stderr, bytes): stderr = stderr.decode("utf-8", errors="replace")
                raise RuntimeError(f"MCP server died. stderr: {stderr[:500]}")
            raw = self._proc.stdout.readline()
            if not raw:
                attempts += 1
                continue
            line = raw.decode("utf-8", errors="replace").strip() if isinstance(raw, bytes) else str(raw).strip()
            if not line: continue
            if not (line.startswith("{") or line.startswith("[")): continue
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                attempts += 1
        raise RuntimeError("MCP server no response after 200 attempts")

    def _initialize(self):
        self._send({"jsonrpc":"2.0","id":self._next_id(),"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"clientInfo":{"name":"ecom-client","version":"1.0.0"}}})
        self._recv()
        self._send({"jsonrpc":"2.0","method":"notifications/initialized","params":{}})

    def list_tools(self):
        with self._lock:
            self._send({"jsonrpc":"2.0","id":self._next_id(),"method":"tools/list","params":{}})
            return self._recv().get("result",{}).get("tools",[])

    def call_tool(self, name, arguments):
        with self._lock:
            self._send({"jsonrpc":"2.0","id":self._next_id(),"method":"tools/call","params":{"name":name,"arguments":arguments}})
            content = self._recv().get("result",{}).get("content",[])
            texts = [c["text"] for c in content if c.get("type")=="text"]
            return texts[0] if texts else "{}"

    def close(self):
        try: self._proc.terminate()
        except: pass


class AgentState(TypedDict):
    messages:      Annotated[list, add_messages]
    order_id:      str
    order:         Optional[dict]
    agent_card:    Optional[dict]
    logistics_url: Optional[str]
    awb_result:    Optional[dict]
    final_status:  Optional[dict]
    error:         Optional[str]
    step_log:      list
    mcp_tools:     Optional[list]
    mcp_client:    Optional[object]


def _extract_customer_name(result: dict) -> str:
    """Get customer name — fallback to shipping_address if customer obj is empty."""
    cust = result.get("customer") or {}
    if isinstance(cust, str):
        return cust if cust and cust != "Unknown" else ""
    name = cust.get("name", "").strip()
    if name and name != "Unknown":
        return name
    # Fallback: shipping address name
    addr = result.get("shipping_address") or {}
    if isinstance(addr, dict):
        first = addr.get("first_name", "")
        last  = addr.get("last_name", "")
        addr_name = f"{first} {last}".strip()
        if addr_name:
            return addr_name
    return "Unknown"

def _extract_city(result: dict) -> str:
    """Get city from shipping_address."""
    addr = result.get("shipping_address") or {}
    if isinstance(addr, str):
        return "?"
    return addr.get("city") or "?"

def _extract_email(result: dict) -> str:
    cust = result.get("customer") or {}
    if isinstance(cust, dict):
        return cust.get("email", "")
    return ""


def node_get_order(state):
    order_id = state["order_id"]
    log      = state.get("step_log", [])
    print(f"\n{'='*60}\nNODE 1: MCP INIT + GET ORDER")

    mcp   = MCPClient()
    tools = mcp.list_tools()
    names = [t["name"] for t in tools]
    log.append(f"[MCP] Connected to MCP Server via stdio protocol")
    log.append(f"[MCP] Tools discovered: {names}")
    print(f"  MCP Tools: {names}")

    log.append(f"[MCP -> get_order] Shopify order_id={order_id}")
    result_str = mcp.call_tool("get_order", {"order_id": order_id})
    result = json.loads(result_str)

    if "error" in result:
        mcp.close()
        log.append(f"[ERROR] {result['error']}")
        return {"error": result["error"], "step_log": log}

    customer_name = _extract_customer_name(result)
    city          = result.get('customer', {}).get('city') or _extract_city(result)
    email         = _extract_email(result)
    items         = result.get("items", [])
    total         = result.get("total", "?")
    currency      = result.get("currency", "")
    item_names    = ", ".join(i.get("name", "") for i in items) if items else "?"

    log.append(f"[MCP] get_order -> {customer_name} | {city} | {len(items)} item(s) | {total} {currency}")
    print(f"  Customer : {customer_name}")
    print(f"  Email    : {email or '?'}")
    print(f"  City     : {city}")
    print(f"  Items    : {item_names}")
    print(f"  Total    : {total} {currency}")

    return {
        "order": result, "mcp_tools": names, "mcp_client": mcp, "step_log": log,
        "messages": [HumanMessage(content=f"Order {order_id} fetched via MCP")]
    }


def node_discover_agent(state):
    log = state.get("step_log", [])
    print(f"\n{'='*60}\nNODE 2: A2A AGENT DISCOVERY")
    log.append(f"[A2A] GET {LOGISTICS_AGENT_URL}/.well-known/agent-card.json")
    try:
        r    = httpx.get(f"{LOGISTICS_AGENT_URL}/.well-known/agent-card.json", timeout=15.0)
        card = r.json()
        name   = card.get("name")
        skills = [s["id"] for s in card.get("skills", [])]
        url    = card.get("supportedInterfaces", [{}])[0].get("url", LOGISTICS_AGENT_URL)
        log.append(f"[A2A] Agent card: {name} | Skills: {skills}")
        print(f"  Agent: {name} | Skills: {skills}")
        return {"agent_card": card, "logistics_url": url, "step_log": log,
                "messages": [AIMessage(content=f"Discovered {name}")]}
    except Exception as e:
        log.append(f"[ERROR] Discovery failed: {e}")
        return {"error": str(e), "step_log": log}


def node_send_a2a(state):
    log   = state.get("step_log", [])
    order = state["order"]
    url   = state["logistics_url"]
    rid   = f"req-{uuid.uuid4().hex[:8]}"
    print(f"\n{'='*60}\nNODE 3: A2A SendMessage")
    log.append(f"[A2A] SendMessage -> {url} | req_id={rid}")

    payload = {
        "jsonrpc": "2.0", "id": rid, "method": "SendMessage",
        "params": {"message": {"role": "user", "messageId": str(uuid.uuid4()),
            "parts": [
                {"kind": "text", "text": f"Generate AWB for {order.get('id','')}"},
                {"kind": "data", "data": {"order": order}, "mediaType": "application/json"}
            ]}}
    }
    try:
        r    = httpx.post(url, json=payload, timeout=60.0)
        resp = r.json()
        artifacts = resp.get("result",{}).get("task",{}).get("artifacts",[])
        awb_data  = None
        for a in artifacts:
            for p in a.get("parts",[]):
                if "data" in p: awb_data = p["data"]; break
        if not awb_data or not awb_data.get("awb"):
            err = resp.get("error",{}).get("message","No AWB returned")
            log.append(f"[ERROR] {err}")
            return {"error": err, "step_log": log}
        log.append(f"[A2A] AWB received: {awb_data['awb']} | Carrier: {awb_data.get('carrier')}")
        print(f"  AWB: {awb_data['awb']} | Carrier: {awb_data.get('carrier')}")
        return {"awb_result": awb_data, "step_log": log,
                "messages": [AIMessage(content=f"AWB: {awb_data['awb']}")]}
    except Exception as e:
        log.append(f"[ERROR] A2A failed: {e}")
        return {"error": str(e), "step_log": log}


def node_update_order(state):
    log      = state.get("step_log", [])
    order_id = state["order_id"]
    awb      = state["awb_result"]
    mcp      = state.get("mcp_client")

    print(f"\n{'='*60}\nNODE 4: MCP fulfill_order")
    log.append(f"[MCP -> fulfill_order] order_id={order_id} awb={awb['awb']}")

    if not mcp:
        mcp = MCPClient()

    try:
        result_str = mcp.call_tool("fulfill_order", {
            "order_id":    order_id,
            "awb":         awb["awb"],
            "carrier":     awb.get("carrier", "Delhivery"),
            "tracking_url": awb.get("tracking_url", f"https://www.delhivery.com/track/package/{awb['awb']}")
        })
        result = json.loads(result_str)
    finally:
        mcp.close()

    if "error" in result and not result.get("success"):
        log.append(f"[ERROR] {result['error']}")
        return {"error": result["error"], "step_log": log}

    status = result.get("status", "awb_tagged")
    log.append(f"[MCP] fulfill_order success -> status: {status}")
    print(f"  Order {order_id} -> {status}")
    print(f"  AWB      : {awb['awb']}")
    print(f"  Carrier  : {awb.get('carrier')}")
    print(f"  Tracking : {result.get('tracking_url')}")
    print(f"  Shopify  : AWB tagged in Additional Details + Timeline")

    return {
        "final_status": result, "step_log": log,
        "messages": [AIMessage(content=f"Order {order_id} {status}. AWB: {awb['awb']}")]
    }


def should_continue(state):
    return "end" if state.get("error") else "continue"


def build_graph():
    g = StateGraph(AgentState)
    g.add_node("get_order",      node_get_order)
    g.add_node("discover_agent", node_discover_agent)
    g.add_node("send_a2a",       node_send_a2a)
    g.add_node("update_order",   node_update_order)
    g.set_entry_point("get_order")
    g.add_conditional_edges("get_order",      should_continue, {"continue":"discover_agent","end":END})
    g.add_conditional_edges("discover_agent", should_continue, {"continue":"send_a2a","end":END})
    g.add_conditional_edges("send_a2a",       should_continue, {"continue":"update_order","end":END})
    g.add_edge("update_order", END)
    return g.compile()


def process_order(order_id):
    print(f"\n{'#'*60}\n  ECOM AWB AGENT - LangGraph + Real MCP + A2A\n  Order: {order_id}\n{'#'*60}")
    result = build_graph().invoke({
        "messages": [HumanMessage(content=f"Process {order_id}")],
        "order_id": order_id, "order": None, "agent_card": None,
        "logistics_url": None, "awb_result": None, "final_status": None,
        "error": None, "step_log": [], "mcp_tools": None, "mcp_client": None
    })
    print(f"\n{'='*60}\nFLOW COMPLETE\n")
    for s in result.get("step_log", []):
        print(f"  {s}")
    if result.get("error"):
        print(f"\nFAILED: {result['error']}")
        return result
    f = result.get("final_status", {})
    print(f"\nSUCCESS: #{f.get('order_id')} | {f.get('status')} | AWB: {f.get('awb')} | {f.get('carrier')}")
    return result


if __name__ == "__main__":
    order_id = sys.argv[1] if len(sys.argv) > 1 else "ORD-001"
    process_order(order_id)
