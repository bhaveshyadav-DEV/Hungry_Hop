"""HungryHop Food Delivery — Operations Dashboard (Databricks App).

Serves delivery KPIs, per-class metrics, top restaurants by GMV and the
Gen AI compensation decisions, all read live from Unity Catalog (hop catalog)
via the Databricks Statement Execution API.
"""
import os
import re
import time

from databricks.sdk import WorkspaceClient
from flask import Flask, render_template_string, request, jsonify

import ai_agent
import mcp_client

app = Flask(__name__)
w = WorkspaceClient()
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "473dc838db8a8c1d")
GENIE_SPACE_ID = "01f1dc2f60421756a0906e1cbc04a5c3"
MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "").strip()

KPI_SQL = """
SELECT COUNT(*) AS total_orders,
       SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
       SUM(CASE WHEN status = 'delivered' THEN order_value ELSE 0 END) AS gmv,
       AVG(CASE WHEN status = 'delivered' THEN actual_minutes END) AS avg_delivery_minutes,
       AVG(CASE WHEN status = 'cancelled' THEN 1.0 ELSE 0.0 END) AS cancellation_rate,
       AVG(CASE WHEN status = 'delivered' AND minutes_past_promise >
                 CASE delivery_class WHEN 'instant' THEN 5 ELSE 20 END
            THEN 1.0 ELSE 0.0 END) AS late_rate
FROM hop.analytics.fact_orders
"""

BY_CLASS_SQL = """
SELECT delivery_class, order_count, delivered_orders, cancellation_rate,
       avg_delivery_minutes, late_rate, gmv
FROM hop.analytics.v_metrics_by_class ORDER BY delivery_class
"""

TOP_RESTAURANTS_SQL = """
SELECT r.name AS restaurant, COUNT(*) AS orders,
       SUM(CASE WHEN f.status = 'delivered' THEN f.order_value ELSE 0 END) AS gmv,
       AVG(CASE WHEN f.status = 'delivered' THEN f.actual_minutes END) AS avg_minutes,
       AVG(CASE WHEN f.status = 'delivered' AND f.minutes_past_promise >
                 CASE f.delivery_class WHEN 'instant' THEN 5 ELSE 20 END
            THEN 1.0 ELSE 0.0 END) AS late_rate
FROM hop.analytics.fact_orders f
JOIN hop.analytics.dim_restaurant r ON f.restaurant_sk = r.restaurant_sk
GROUP BY r.name ORDER BY gmv DESC LIMIT 10
"""

COMPENSATION_SQL = """
SELECT ticket_id, fault, delivered, is_late, compensation_type, amount, reason_code
FROM hop.genai.compensation_decisions ORDER BY ticket_id
"""


def run_sql(sql):
    """Run one SQL statement via the Statement Execution REST API;
    return rows as a list of dicts (values are strings). Retries once."""
    import time

    for attempt in (1, 2):
        res = w.api_client.do(
            "POST",
            "/api/2.0/sql/statements",
            body={"statement": sql, "warehouse_id": WAREHOUSE_ID, "wait_timeout": "50s"},
        )
        state = res["status"]["state"]
        if state == "SUCCEEDED":
            cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
            return [dict(zip(cols, arr)) for arr in (res["result"].get("data_array") or [])]
        if attempt == 1:
            time.sleep(2)  # transient warehouse/launch errors: retry once
    err = res["status"].get("error") or {}
    raise RuntimeError(f"{state}: {err.get('message', 'no error detail')}")


def _f(v, digits=0):
    return "—" if v in (None, "") else f"{float(v):,.{digits}f}"


def _pct(v, digits=1):
    return "—" if v in (None, "") else f"{float(v) * 100:.{digits}f}%"


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>HungryHop Food Delivery</title>
<style>
  :root { --brand: #ff5722; --ink: #21252b; --muted: #6b7280; }
  * { box-sizing: border-box; margin: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #f4f5f7; color: var(--ink); }
  header { background: var(--brand); color: #fff; padding: 22px 36px; }
  header h1 { font-size: 26px; }
  header p { opacity: .9; margin-top: 4px; font-size: 13px; }
  main { max-width: 1180px; margin: 0 auto; padding: 26px 20px 60px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; }
  .card { background: #fff; border-radius: 12px; padding: 16px 18px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }
  .card .v { font-size: 24px; font-weight: 700; margin-top: 6px; }
  .card .l { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); }
  section { margin-top: 34px; }
  h2 { font-size: 16px; margin-bottom: 12px; }
  table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 10px; overflow: hidden;
         box-shadow: 0 1px 3px rgba(0,0,0,.08); font-size: 13px; }
  th { background: #eef0f2; text-align: left; padding: 9px 12px; font-size: 11px; text-transform: uppercase;
       letter-spacing: .05em; color: var(--muted); }
  td { padding: 8px 12px; border-top: 1px solid #f0f1f3; }
  .bar { height: 7px; border-radius: 4px; background: #e5e7eb; min-width: 90px; }
  .bar > span { display: block; height: 100%; border-radius: 4px; background: var(--brand); }
  .tag { display: inline-block; padding: 2px 9px; border-radius: 999px; font-size: 11px; font-weight: 600; }
  .tag.COUPON { background: #fef3c7; color: #92400e; }
  .tag.REFUND { background: #fee2e2; color: #991b1b; }
  .tag.NONE { background: #e5e7eb; color: #374151; }
  footer { margin-top: 46px; color: var(--muted); font-size: 12px; text-align: center; }
  .error { background: #fee2e2; color: #991b1b; border-radius: 10px; padding: 14px 18px; margin-top: 26px; }
</style>
</head>
<body>
<header>
  <h1>🍔 HungryHop Food Delivery</h1>
  <p>Operations dashboard · live from Unity Catalog (hop.analytics &amp; hop.genai)</p>
</header>
<main>
  {% if error %}
    <div class="error">Could not load data: <b>{{ error }}</b></div>
  {% endif %}

  {% if kpis %}
  <div class="cards">
    <div class="card"><div class="l">Total orders</div><div class="v">{{ kpis.total_orders }}</div></div>
    <div class="card"><div class="l">Delivered</div><div class="v">{{ kpis.delivered_orders }}</div></div>
    <div class="card"><div class="l">Cancelled</div><div class="v">{{ kpis.cancelled_orders }}</div></div>
    <div class="card"><div class="l">GMV (delivered)</div><div class="v">Rs {{ kpis.gmv }}</div></div>
    <div class="card"><div class="l">Avg delivery min</div><div class="v">{{ kpis.avg_min }}</div></div>
    <div class="card"><div class="l">Cancellation rate</div><div class="v">{{ kpis.cancellation_rate }}</div></div>
    <div class="card"><div class="l">Late rate</div><div class="v">{{ kpis.late_rate }}</div></div>
  </div>

  <section>
    <h2>Delivery metrics by class</h2>
    <table>
      <tr><th>Class</th><th>Orders</th><th>Delivered</th><th>Cancellation rate</th>
          <th>Avg delivery (min)</th><th>Late rate</th><th>GMV</th></tr>
      {% for r in by_class %}
      <tr><td><b>{{ r.delivery_class }}</b></td><td>{{ r.order_count }}</td><td>{{ r.delivered_orders }}</td>
          <td>{{ r.cancellation_rate }}</td><td>{{ r.avg_delivery_minutes }}</td>
          <td>{{ r.late_rate }}</td><td>Rs {{ r.gmv }}</td></tr>
      {% endfor %}
    </table>
  </section>

  <section>
    <h2>Top 10 restaurants by GMV</h2>
    <table>
      <tr><th>Restaurant</th><th>Orders</th><th>GMV</th><th>Avg delivery (min)</th>
          <th>Late rate</th><th>Share of top-10 GMV</th></tr>
      {% for r in top_restaurants %}
      <tr><td><b>{{ r.restaurant }}</b></td><td>{{ r.orders }}</td><td>Rs {{ r.gmv }}</td>
          <td>{{ r.avg_minutes }}</td><td>{{ r.late_rate }}</td>
          <td><div class="bar"><span style="width: {{ r.share }}%"></span></div></td></tr>
      {% endfor %}
    </table>
  </section>
  {% endif %}

  {% if compensation %}
  <section>
    <h2>Support tickets — Gen AI compensation decisions</h2>
    <table>
      <tr><th>Ticket</th><th>Fault</th><th>Delivered</th><th>Late</th>
          <th>Compensation</th><th>Amount</th><th>Reason code</th></tr>
      {% for r in compensation %}
      <tr><td>{{ r.ticket_id }}</td><td>{{ r.fault }}</td>
          <td>{{ "✅" if r.delivered == "true" else "—" }}</td>
          <td>{{ "⚠️" if r.is_late == "true" else "" }}</td>
          <td><span class="tag {{ r.compensation_type }}">{{ r.compensation_type }}</span></td>
          <td>Rs {{ r.amount }}</td><td>{{ r.reason_code }}</td></tr>
      {% endfor %}
    </table>
  </section>
  {% endif %}

  <footer>HungryHop Food Delivery · Databricks App · data: hop.analytics &amp; hop.genai</footer>
</main>

<!-- ================= AI Assistant Widget (independent component) ================= -->
<div id="aiw-root">
  <div id="aiw-tip" role="status">Chat with AI Assistant</div>
  <button id="aiw-trigger" aria-label="Chat with AI Assistant" aria-expanded="false" aria-controls="aiw-window">
    <span aria-hidden="true">✨</span>
    <span id="aiw-badge" hidden aria-hidden="true"></span>
  </button>

  <div id="aiw-window" role="dialog" aria-label="AI Assistant chat window">
    <div id="aiw-head">
      <div id="aiw-avatar" aria-hidden="true">🤖</div>
      <div class="aiw-headtext">
        <div class="aiw-name">AI Assistant</div>
        <div class="aiw-status"><span class="aiw-dot" aria-hidden="true"></span>Online</div>
      </div>
      <button class="aiw-hbtn" id="aiw-theme" aria-label="Toggle light or dark theme" title="Toggle theme">🌙</button>
      <button class="aiw-hbtn" id="aiw-clear" aria-label="Clear chat history" title="Clear chat">🗑️</button>
      <button class="aiw-hbtn" id="aiw-min" aria-label="Minimize chat window" title="Minimize">–</button>
      <button class="aiw-hbtn" id="aiw-close" aria-label="Close chat window" title="Close">✕</button>
    </div>
    <div id="aiw-messages" aria-live="polite" aria-label="Chat messages"></div>
    <div id="aiw-sugg" role="group" aria-label="Suggested questions"></div>
    <div id="aiw-inputbar">
      <input id="aiw-input" type="text" placeholder="Type your message..." aria-label="Type your message">
      <button id="aiw-send" aria-label="Send message">➤</button>
    </div>
  </div>
</div>

<style>
  /* --- AI Assistant Widget styles (scoped under #aiw-root) --- */
  #aiw-root, #aiw-root * { box-sizing: border-box; font-family: 'Segoe UI', system-ui, sans-serif; }
  #aiw-trigger {
    position: fixed; right: 24px; bottom: 24px; z-index: 10000;
    width: 60px; height: 60px; border-radius: 50%; border: 0; cursor: pointer;
    background: linear-gradient(135deg, #6366f1, #8b5cf6 55%, #d946ef);
    color: #fff; font-size: 27px; line-height: 1; padding: 0;
    display: flex; align-items: center; justify-content: center;
    animation: aiw-float 3s ease-in-out infinite, aiw-glow 2.4s ease-in-out infinite;
    transition: transform .25s ease;
  }
  #aiw-trigger:hover { transform: scale(1.1); }
  #aiw-trigger:focus-visible { outline: 3px solid #c4b5fd; outline-offset: 3px; }
  #aiw-badge {
    position: absolute; top: -3px; right: -3px; width: 15px; height: 15px; border-radius: 50%;
    background: #f43f5e; border: 2px solid #fff; animation: aiw-pulse 1.6s infinite;
  }
  #aiw-tip {
    position: fixed; right: 96px; bottom: 40px; z-index: 10000; max-width: 220px;
    background: #111827; color: #fff; padding: 8px 14px; border-radius: 10px 10px 2px 10px;
    font-size: 12.5px; opacity: 0; pointer-events: none; transform: translateX(8px);
    transition: opacity .3s ease, transform .3s ease; box-shadow: 0 4px 16px rgba(0,0,0,.25);
  }
  #aiw-tip.show { opacity: 1; transform: translateX(0); }
  #aiw-window {
    position: fixed; right: 24px; bottom: 100px; z-index: 10001;
    width: 384px; max-width: calc(100vw - 48px); height: 560px; max-height: calc(100vh - 140px);
    border-radius: 20px; overflow: hidden; display: flex; flex-direction: column;
    background: rgba(255,255,255,.72); backdrop-filter: blur(18px); -webkit-backdrop-filter: blur(18px);
    border: 1px solid rgba(255,255,255,.65); box-shadow: 0 24px 64px rgba(15,23,42,.28);
    visibility: hidden; pointer-events: none; opacity: 0;
    transform: translateY(26px) scale(.97);
    transition: transform .35s cubic-bezier(.2,.9,.3,1.2), opacity .3s ease, visibility .3s;
  }
  #aiw-window.aiw-open { visibility: visible; pointer-events: auto; opacity: 1; transform: translateY(0) scale(1); }
  #aiw-head {
    display: flex; align-items: center; gap: 10px; padding: 13px 14px; flex: 0 0 auto;
    background: linear-gradient(135deg, #6366f1, #8b5cf6); color: #fff;
  }
  #aiw-avatar {
    width: 38px; height: 38px; border-radius: 50%; background: rgba(255,255,255,.22);
    display: flex; align-items: center; justify-content: center; font-size: 20px;
  }
  .aiw-headtext { flex: 1; min-width: 0; }
  .aiw-name { font-weight: 700; font-size: 14px; }
  .aiw-status { font-size: 11px; opacity: .92; display: flex; align-items: center; gap: 5px; }
  .aiw-dot { width: 8px; height: 8px; border-radius: 50%; background: #4ade80; box-shadow: 0 0 6px #4ade80; animation: aiw-blink 2s infinite; }
  .aiw-hbtn {
    width: 30px; height: 30px; border: 0; border-radius: 9px; background: rgba(255,255,255,.16);
    color: #fff; cursor: pointer; font-size: 14px; display: flex; align-items: center; justify-content: center;
    transition: background .2s ease, transform .2s ease; padding: 0;
  }
  .aiw-hbtn:hover { background: rgba(255,255,255,.32); transform: translateY(-1px); }
  .aiw-hbtn:focus-visible { outline: 2px solid #fff; }
  #aiw-messages { flex: 1; overflow-y: auto; padding: 14px; display: flex; flex-direction: column; gap: 10px; scroll-behavior: smooth; }
  .aiw-msg {
    max-width: 84%; padding: 10px 13px; border-radius: 16px; font-size: 13.5px; line-height: 1.45;
    white-space: pre-wrap; word-break: break-word; box-shadow: 0 1px 3px rgba(0,0,0,.08);
    animation: aiw-msgin .3s ease;
  }
  .aiw-msg.aiw-user { align-self: flex-end; background: linear-gradient(135deg, #6366f1, #8b5cf6); color: #fff; border-bottom-right-radius: 5px; }
  .aiw-msg.aiw-bot { align-self: flex-start; background: rgba(238,240,247,.9); color: #1f2937; border-bottom-left-radius: 5px; }
  .aiw-time { display: block; font-size: 10px; opacity: .65; margin-top: 5px; }
  .aiw-msg code { background: rgba(99,102,241,.14); padding: 1px 5px; border-radius: 4px; font-size: 12px; }
  .aiw-typing { display: inline-flex; gap: 5px; align-items: center; padding: 13px 15px; }
  .aiw-typing i { width: 7px; height: 7px; border-radius: 50%; background: #8b5cf6; animation: aiw-tdots 1.2s infinite; }
  .aiw-typing i:nth-child(2) { animation-delay: .2s; }
  .aiw-typing i:nth-child(3) { animation-delay: .4s; }
  #aiw-sugg { display: flex; flex-wrap: wrap; gap: 8px; padding: 4px 14px 12px; flex: 0 0 auto; }
  .aiw-chip {
    border: 1px solid rgba(139,92,246,.35); background: rgba(139,92,246,.08); color: #6d28d9;
    padding: 7px 12px; border-radius: 999px; font-size: 12px; cursor: pointer;
    transition: all .2s ease; font-family: inherit;
  }
  .aiw-chip:hover { background: rgba(139,92,246,.18); transform: translateY(-1px); }
  .aiw-chip:focus-visible { outline: 2px solid #8b5cf6; }
  #aiw-inputbar { display: flex; gap: 8px; padding: 12px 14px; flex: 0 0 auto; border-top: 1px solid rgba(148,163,184,.25); align-items: center; }
  #aiw-input {
    flex: 1; border: 0; background: rgba(255,255,255,.9); border-radius: 999px; padding: 10px 16px;
    font-size: 13px; outline: none; box-shadow: inset 0 0 0 1px rgba(148,163,184,.35); color: #111827;
  }
  #aiw-input:focus { box-shadow: inset 0 0 0 2px #8b5cf6; }
  #aiw-send {
    width: 42px; height: 42px; border: 0; border-radius: 50%; flex: 0 0 auto; cursor: pointer;
    background: linear-gradient(135deg, #6366f1, #8b5cf6); color: #fff; font-size: 16px;
    display: flex; align-items: center; justify-content: center; transition: transform .2s ease, opacity .2s; padding: 0;
  }
  #aiw-send:hover { transform: scale(1.1); }
  #aiw-send:disabled { opacity: .55; cursor: wait; transform: none; }
  #aiw-root.aiw-dark #aiw-window { background: rgba(17,24,39,.78); border-color: rgba(148,163,184,.28); }
  #aiw-root.aiw-dark .aiw-msg.aiw-bot { background: rgba(51,65,85,.92); color: #e5e7eb; }
  #aiw-root.aiw-dark .aiw-time { color: #9ca3af; }
  #aiw-root.aiw-dark #aiw-input { background: rgba(17,24,39,.85); color: #e5e7eb; }
  #aiw-root.aiw-dark .aiw-chip { background: rgba(139,92,246,.16); color: #c4b5fd; border-color: rgba(139,92,246,.4); }
  @media (max-width: 480px) {
    #aiw-trigger { right: 16px; bottom: 16px; width: 54px; height: 54px; font-size: 24px; }
    #aiw-window { left: 12px; right: 12px; width: auto; bottom: 82px; height: 68vh; }
    #aiw-tip { right: 80px; }
  }
  @keyframes aiw-float { 0%, 100% { transform: translateY(0); } 50% { transform: translateY(-7px); } }
  @keyframes aiw-glow { 0%, 100% { box-shadow: 0 8px 24px rgba(99,102,241,.45), 0 0 0 0 rgba(217,70,239,.5); } 50% { box-shadow: 0 8px 34px rgba(99,102,241,.75), 0 0 0 16px rgba(217,70,239,0); } }
  @keyframes aiw-pulse { 0% { box-shadow: 0 0 0 0 rgba(244,63,94,.6); } 70% { box-shadow: 0 0 0 10px rgba(244,63,94,0); } 100% { box-shadow: 0 0 0 0 rgba(244,63,94,0); } }
  @keyframes aiw-blink { 50% { opacity: .45; } }
  @keyframes aiw-msgin { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: none; } }
  @keyframes aiw-tdots { 0%, 60%, 100% { transform: translateY(0); opacity: .45; } 30% { transform: translateY(-6px); opacity: 1; } }
</style>

<script src="/static/aiw.js?v=6"></script>

</body>
</html>"""


@app.route("/")
def index():
    error = None
    kpis, by_class, top_restaurants, compensation = {}, [], [], []
    try:
        row = run_sql(KPI_SQL)[0]
        kpis = {
            "total_orders": row["total_orders"],
            "delivered_orders": row["delivered_orders"],
            "cancelled_orders": row["cancelled_orders"],
            "gmv": _f(row["gmv"]),
            "avg_min": _f(row["avg_delivery_minutes"], 1),
            "cancellation_rate": _pct(row["cancellation_rate"]),
            "late_rate": _pct(row["late_rate"]),
        }
    except Exception as exc:  # noqa: BLE001
        error = f"KPIs: {exc}"

    try:
        by_class = [
            {
                "delivery_class": r["delivery_class"].capitalize(),
                "order_count": r["order_count"],
                "delivered_orders": r["delivered_orders"],
                "cancellation_rate": _pct(r["cancellation_rate"]),
                "avg_delivery_minutes": _f(r["avg_delivery_minutes"], 1),
                "late_rate": _pct(r["late_rate"]),
                "gmv": _f(r["gmv"]),
            }
            for r in run_sql(BY_CLASS_SQL)
        ]
    except Exception as exc:  # noqa: BLE001
        error = error or f"By class: {exc}"

    try:
        rows = run_sql(TOP_RESTAURANTS_SQL)
        top_gmv = sum(float(r["gmv"]) for r in rows) or 1.0
        top_restaurants = [
            {
                "restaurant": r["restaurant"],
                "orders": r["orders"],
                "gmv": _f(r["gmv"]),
                "avg_minutes": _f(r["avg_minutes"], 1),
                "late_rate": _pct(r["late_rate"]),
                "share": round(float(r["gmv"]) / top_gmv * 100),
            }
            for r in rows
        ]
    except Exception as exc:  # noqa: BLE001
        error = error or f"Top restaurants: {exc}"

    try:
        compensation = [
            {
                "ticket_id": r["ticket_id"],
                "fault": r["fault"],
                "delivered": (r["delivered"] or "").lower(),
                "is_late": (r["is_late"] or "").lower(),
                "compensation_type": r["compensation_type"] or "NONE",
                "amount": _f(r["amount"], 0),
                "reason_code": r["reason_code"],
            }
            for r in run_sql(COMPENSATION_SQL)
        ]
    except Exception as exc:  # noqa: BLE001
        error = error or f"Compensation: {exc}"

    return render_template_string(
        PAGE,
        error=error,
        kpis=kpis,
        by_class=by_class,
        top_restaurants=top_restaurants,
        compensation=compensation,
    )


# ---------------- Guardrail (R13 rules) ----------------
GUARDRAIL_MODEL = "system.ai.gpt-oss-120b"
BLOCK_REASONS = {
    "PII": (
        "I can't share private contact details of a specific person - phone numbers, "
        "emails or addresses are protected. That information doesn't exist in my data "
        "anyway. I CAN show aggregated analytics instead - try asking 'late orders by "
        "customer' or 'late rate by zone'."
    ),
    "PROMISE": (
        "I can't promise or hand out refunds, coupons or compensation - those are decided "
        "only after the order is verified against the SOP. Ask me 'what does the "
        "compensation policy say?' and I'll show you the exact rules and eligibility."
    ),
    "OFFTOPIC": (
        "I'm the HungryHop delivery assistant - I only answer questions about HungryHop "
        "orders, late deliveries, cancellation rates, GMV, customers, restaurants, zones "
        "and policy. Could you ask me about your delivery data instead?"
    ),
}
FALLBACK_BLOCK = (
    "I can't help with that request. Ask me about HungryHop orders, delivery "
    "performance, GMV, customers or policy."
)
GUARDRAIL_PROMPT = """You are the safety guardrail for the HungryHop food-delivery OWNER/ANALYTICS assistant chat. The user is the business owner. The analytics data contains only anonymized customer keys - NO names, phone numbers, emails or addresses exist in this system.

Classify the user message as exactly one of: allow, block:PII, block:PROMISE, block:OFFTOPIC.

block:PII = the message asks to reveal private contact data of a specific person (a phone number, email, or home address of a customer/rider/staff member).
block:PROMISE = the message demands or seeks promises of money, refunds, coupons or compensation without verification ("give me Rs 500", "refund me now", "promise me a coupon").
block:OFFTOPIC = the message is unrelated to food delivery, orders, delivery performance, HungryHop or this assistant (e.g. homework, poems, code).

ALLOW: any analytics or status question about orders, late orders, late deliveries, cancellation rates, GMV, customers (as a group, incl. "customer details", "late order customers"), restaurants, zones, riders, delivery performance, policy or SOP ("what does the policy say", "am I eligible"); updating the user's OWN data; questions about the assistant itself.

Respond with exactly one of: allow, block:PII, block:PROMISE, block:OFFTOPIC.

User message: """


def _sql_literal(text):
    return "'" + text.replace("'", "''") + "'"


def guardrail_sql(question):
    return (
        "SELECT ai_query('" + GUARDRAIL_MODEL + "', "
        + _sql_literal(GUARDRAIL_PROMPT + question) + ") AS decision"
    )


def rule_guardrail(question):
    """Deterministic fallback when the LLM guardrail is unavailable (R13 rules)."""
    q = question.lower()
    pii = bool(re.search(r"phone\s*number|e-?mail|home address|\baddress\b", q))
    own_update = ("my " in q or "mine" in q) and ("update" in q or "change" in q)
    promise = any(
        k in q
        for k in (
            "give me rs", "give me money", "give me a coupon", "give me coupon",
            "free coupon", "refund me", "compensate me", "promise me",
            "send me money", "wallet cash",
        )
    )
    policy_q = any(k in q for k in ("policy", "sop", "am i eligible", "compensation rules"))
    on_topic = any(
        k in q
        for k in (
            "order", "deliver", "late", "cancel", "gmv", "restaurant", "zone",
            "rider", "ticket", "policy", "hungryhop", "food", "menu", "customer",
            "assistant", "help", "yourself", "find", "anything", "chat", "support",
            "compensation", "coupon", "refund",
        )
    )
    if promise and not policy_q:
        return "block:PROMISE"
    if pii and not own_update:
        return "block:PII"
    if not on_topic:
        return "block:OFFTOPIC"
    return "allow"


def _genie_answer(question):
    """Fallback: ask the HungryHop Genie agent and return its answer."""
    res = w.api_client.do(
        "POST",
        f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
        body={"content": question},
    )
    conv_id, msg_id = res["conversation_id"], res["message_id"]
    state, msg = None, None
    deadline = time.time() + 90
    while time.time() < deadline:
        msg = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conv_id}/messages/{msg_id}",
        )
        state = msg.get("status")
        if state in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    if state != "COMPLETED":
        return f"Sorry, I could not find an answer (state: {state})."
    answer = "Sorry, I could not find an answer."
    for att in msg.get("attachments") or []:
        text = att.get("text") or {}
        if text.get("purpose") == "TEXT_ATTACHMENT_PURPOSE_ANSWER" and text.get("content"):
            answer = text["content"]
    return answer


def _run_agent(question, history):
    """Run the GenAI agent (LLM + STM + LTM + tools); Genie as fallback."""
    tools_desc = ai_agent.builtin_tools_desc()
    mcp = None
    mcp_names = set()
    if MCP_SERVER_URL:
        try:
            mcp = mcp_client.MCPClient(MCP_SERVER_URL)
            mcp.initialize()
            tools = mcp.list_tools()
            if tools:
                mcp_names = {t.get("name") for t in tools if t.get("name")}
                tools_desc += "\n" + ai_agent.mcp_tools_desc(tools)
        except Exception:  # noqa: BLE001
            mcp = None

    def call_tool(name, args):
        if name == "run_sql":
            return ai_agent.tool_run_sql(w.api_client, WAREHOUSE_ID, (args or {}).get("query", ""))
        uc_sql = ai_agent.uc_tool_sql(name, args)
        if uc_sql is not None:
            return ai_agent.tool_run_sql(w.api_client, WAREHOUSE_ID, uc_sql)
        if mcp is not None and name in mcp_names:
            return mcp.call_tool(name, args)
        return f"ERROR: unknown tool {name}"

    try:
        answer = ai_agent.run_agent(
            w.api_client, WAREHOUSE_ID, question, history, tools_desc, call_tool
        )
        if answer:
            return answer
    except Exception:  # noqa: BLE001
        app.logger.exception("agent run failed for question: %s", question)
    try:
        return _genie_answer(question)
    except Exception:  # noqa: BLE001
        app.logger.exception("genie fallback failed for question: %s", question)
        return "Sorry, I could not complete that request right now. Please try again or rephrase."


@app.route("/chat", methods=["POST"])
def chat():
    """Agent chat: guardrail -> GenAI agent (STM + LTM + tools) -> answer."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        question = (body.get("question") or "").strip()
        if not question:
            return jsonify({"error": "empty question"})
        history = [
            h
            for h in (body.get("history") or [])
            if isinstance(h, dict) and (h.get("text") or "").strip()
        ][-12:]
        decision = "allow"
        try:
            rows = run_sql(guardrail_sql(question))
            d = (rows[0].get("decision") or "").strip().lower() if rows else ""
            if d == "allow" or d.startswith("block"):
                decision = d
        except Exception:  # noqa: BLE001
            decision = rule_guardrail(question)
        if decision != "allow":
            category = decision.split(":", 1)[1].strip().upper() if ":" in decision else ""
            return jsonify(
                {"blocked": True, "blocked_msg": BLOCK_REASONS.get(category, FALLBACK_BLOCK)}
            )
        answer = _run_agent(question, history)
        try:
            ai_agent.store_memory(w.api_client, WAREHOUSE_ID, question, answer)
        except Exception:  # noqa: BLE001
            pass
        return jsonify({"answer": answer})
    except Exception as exc:  # noqa: BLE001
        app.logger.exception("chat endpoint failed")
        return jsonify({"error": str(exc)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
