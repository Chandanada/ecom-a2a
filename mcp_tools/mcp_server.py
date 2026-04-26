"""
Ecom MCP Server — Real Model Context Protocol
Exposes ecommerce tools via official MCP protocol (stdio transport).
LangGraph client connects to this as an MCP client and discovers tools dynamically.

Tools exposed:
  - get_order(order_id)           → fetch order details
  - update_order_status(...)      → update AWB + carrier + tracking
  - list_pending_orders()         → list all unshipped orders
  - get_order_items(order_id)     → get just the line items
"""
import asyncio, os, httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://localhost:8000")

server = Server("ecom-mcp-server")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="get_order",
            description="Fetch complete order details from the ecommerce system including customer info, items, shipping address, and current fulfillment status.",
            inputSchema={
                "type": "object",
                "properties": {
                    "order_id": {
                        "type": "string",
                        "description": "The order ID e.g. ORD-001"
                    }
                },
                "required": ["order_id"]
            }
        ),
        types.Tool(
            name="update_order_status",
            description="Update order fulfillment status with AWB tracking number after logistics agent generates it. Sets status to 'shipped'.",
            inputSchema={
                "type": "object",
                "properties": {
                    "order_id": {
                        "type": "string",
                        "description": "The order ID to update"
                    },
                    "awb": {
                        "type": "string",
                        "description": "AWB (Air Waybill) tracking number from logistics carrier"
                    },
                    "carrier": {
                        "type": "string",
                        "description": "Logistics carrier name e.g. Delhivery, FedEx, BlueDart"
                    },
                    "tracking_url": {
                        "type": "string",
                        "description": "Full URL for tracking the shipment"
                    }
                },
                "required": ["order_id", "awb", "carrier", "tracking_url"]
            }
        ),
        types.Tool(
            name="list_pending_orders",
            description="List all ecommerce orders with status 'confirmed' that are waiting for AWB generation and shipment.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        types.Tool(
            name="get_order_items",
            description="Get just the line items for a specific order — SKUs, names, quantities, and prices.",
            inputSchema={
                "type": "object",
                "properties": {
                    "order_id": {
                        "type": "string",
                        "description": "The order ID"
                    }
                },
                "required": ["order_id"]
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    import json

    async with httpx.AsyncClient(timeout=10.0) as client:

        if name == "get_order":
            order_id = arguments["order_id"]
            r = await client.get(f"{ORDER_SERVICE_URL}/orders/{order_id}")
            if r.status_code == 404:
                return [types.TextContent(type="text", text=json.dumps({"error": f"Order {order_id} not found"}))]
            return [types.TextContent(type="text", text=json.dumps(r.json(), indent=2))]

        elif name == "update_order_status":
            order_id    = arguments["order_id"]
            awb         = arguments["awb"]
            carrier     = arguments["carrier"]
            tracking_url = arguments["tracking_url"]
            r = await client.post(
                f"{ORDER_SERVICE_URL}/orders/{order_id}/fulfill",
                json={"awb": awb, "carrier": carrier, "tracking_url": tracking_url}
            )
            return [types.TextContent(type="text", text=json.dumps(r.json(), indent=2))]

        elif name == "list_pending_orders":
            r = await client.get(f"{ORDER_SERVICE_URL}/orders")
            all_orders = r.json().get("orders", [])
            pending    = [o for o in all_orders if o.get("status") == "confirmed"]
            return [types.TextContent(type="text", text=json.dumps({
                "pending_orders": pending,
                "count":          len(pending),
                "message":        f"{len(pending)} orders waiting for AWB generation"
            }, indent=2))]

        elif name == "get_order_items":
            order_id = arguments["order_id"]
            r = await client.get(f"{ORDER_SERVICE_URL}/orders/{order_id}")
            if r.status_code == 404:
                return [types.TextContent(type="text", text=json.dumps({"error": f"Order {order_id} not found"}))]
            order = r.json()
            return [types.TextContent(type="text", text=json.dumps({
                "order_id": order_id,
                "items":    order.get("items", []),
                "total":    order.get("total", 0),
                "currency": "INR"
            }, indent=2))]

        else:
            return [types.TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
