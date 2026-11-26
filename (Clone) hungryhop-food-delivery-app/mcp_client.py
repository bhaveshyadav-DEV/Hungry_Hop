"""Minimal MCP (Model Context Protocol) client for the HungryHop app.

Speaks the Streamable HTTP transport (JSON-RPC over HTTP POST) so the
GenAI agent can discover and call the tools exposed by the HungryHop MCP
server. Enable by setting the MCP_SERVER_URL env var in app.yaml.
"""

import json
import urllib.request


class MCPClient:
    def __init__(self, base_url, timeout=60):
        self.url = base_url.rstrip("/")
        self.session_id = None
        self.timeout = timeout
        self._id = 0

    def _post(self, method, params=None, notify=False):
        payload = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        if not notify:
            self._id += 1
            payload["id"] = self._id
        req = urllib.request.Request(
            self.url,
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
            method="POST",
        )
        if self.session_id:
            req.add_header("Mcp-Session-Id", self.session_id)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            sid = resp.headers.get("Mcp-Session-Id")
            if sid:
                self.session_id = sid
            body = resp.read().decode("utf-8", "replace")
        if notify:
            return None
        candidates = []
        for line in body.splitlines():
            if line.startswith("data:"):
                try:
                    candidates.append(json.loads(line[5:].strip()))
                except ValueError:
                    pass
        if candidates:
            return candidates[-1]
        try:
            return json.loads(body)
        except ValueError:
            return None

    def initialize(self):
        """MCP handshake: initialize + initialized notification."""
        result = self._post(
            "initialize",
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "hungryhop-app", "version": "1.0"},
            },
        )
        self._post("notifications/initialized", notify=True)
        return result

    def list_tools(self):
        """Return the tool list exposed by the MCP server."""
        resp = self._post("tools/list", {}) or {}
        return (resp.get("result") or {}).get("tools") or []

    def call_tool(self, name, arguments):
        """Execute one tool on the MCP server and return its text output."""
        resp = self._post("tools/call", {"name": name, "arguments": arguments or {}}) or {}
        result = resp.get("result") or {}
        if result.get("isError"):
            return "ERROR: tool reported failure"
        texts = [
            c.get("text", "")
            for c in (result.get("content") or [])
            if c.get("type") == "text"
        ]
        return "\n".join(t for t in texts if t) or json.dumps(result)
