"""HungryHop GenAI Agent — LLM + system prompt + STM + LTM + tool calling.

Components:
- LLM: gpt-oss-120b via ai_query() on the Statement Execution API.
- System prompt: business-owner assistant for HungryHop delivery analytics.
- STM (short-term memory): the caller passes the current conversation
  history; every turn it is injected into the prompt.
- LTM (long-term memory): hop.genai.agent_memory Unity Catalog table. The
  LLM itself decides which facts are worth remembering (store_memory) and
  past facts are injected into the system prompt (fetch_memories).
- Tools: built-in read-only run_sql plus any tools exposed by the HungryHop
  MCP server (see mcp_client.py).
"""

import json
import re

AGENT_MODEL = "system.ai.gpt-oss-120b"
MAX_STEPS = 8
MAX_MEMORIES = 8
MAX_HISTORY = 12
MEMORY_TABLE = "hop.genai.agent_memory"
RESULT_LIMIT = 25

SYSTEM_PROMPT = """You are the HungryHop AI assistant for the business owner of a food-delivery company. Answer questions about orders, late deliveries, cancellation rates, GMV, customers, restaurants, zones, riders and company policy.

AVAILABLE TOOLS — to use one, reply with ONLY a JSON object {{"tool": "<name>", "args": {{...}}}}:
{tools}

DATA SCHEMA (hop.analytics — you do NOT need to explore or list tables, this is the complete schema):
- fact_orders: order_id, customer_sk, restaurant_sk, zone_sk, placed_date_key (INT yyyyMMdd), delivery_class (instant|standard), status (delivered|cancelled), order_value, promised_minutes, actual_minutes, minutes_past_promise
- dim_restaurant: restaurant_sk, restaurant_id, name, cuisine, zone_id, rating, is_active, valid_from, valid_to, is_current
- dim_customer: customer_sk, customer_id, city, tier
- dim_zone: zone_sk, zone_id, city, zone_type (core|suburb|outskirts)
- dim_date: date_key, full_date, year, month, month_start, week_start, day_of_week
- v_metrics_by_class: delivery_class, order_count, delivered_orders, cancellation_rate, avg_delivery_minutes, late_rate, gmv (pre-aggregated per delivery_class)
- mv_orders: same columns as v_metrics_by_class (metric view)
- metric_definitions: metric_name, expression, description
Joins: fact_orders.restaurant_sk = dim_restaurant.restaurant_sk, fact_orders.customer_sk = dim_customer.customer_sk, fact_orders.zone_sk = dim_zone.zone_sk, fact_orders.placed_date_key = dim_date.date_key. Restaurant names are in dim_restaurant.name. There is NO table named hop.analytics.orders or hop.fact_orders.

LONG-TERM MEMORY (facts from past conversations):
{memories}

RULES:
- For data questions use a tool. A late order = status delivered AND minutes_past_promise > 5 (instant class) or > 20 (standard class).
- ALWAYS use fully qualified table names in SQL: hop.analytics.<table> or hop.genai.<table> — never write hop.<table> without the schema.
- Plan efficiently: you have a limited tool budget (8 calls). The full schema is already in your prompt — do NOT waste calls listing tables or inspecting columns; write the final query directly and answer.
- Do NOT use v_metrics_by_class or mv_orders for restaurant-level questions — those are only per delivery_class. For per-restaurant (or per-zone/city/customer) questions aggregate fact_orders joined with the needed dim.
- You have LIVE warehouse access through your tools — never say you cannot access or pull data, and never hand the user SQL to run; YOU run the query via run_sql and answer from the real results.
- Final answers must NEVER contain SQL code, markdown tables, headings or bold/italic markers. Answer in PLAIN TEXT: short natural-language sentences, and a simple list like "1. Lotus Garden — 156 orders" when ranking items.
- You may inspect table columns first: SELECT column_name, data_type FROM hop.information_schema.columns WHERE table_name = '<table>'. (Only if something is genuinely missing from the schema above.)
- When you can answer without data (policy, small talk, memory recall), reply ONLY {{"answer": "<plain-text reply>"}}.
- After a tool result is given, reply ONLY {{"answer": "<plain-text reply>"}} — no more tool calls unless truly needed.
- Never reveal private contact details (phone/email/address); never promise refunds or coupons; compensation is decided only after order verification per the SOP.
- Keep answers concise, plain-text and conversational (no markdown formatting at all)."""


def _sql_literal(s):
    return "'" + str(s).replace("'", "''") + "'"


def ask_llm(api, warehouse_id, prompt):
    """Call the agent LLM through ai_query() on the Statement Execution API."""
    sql = f"SELECT ai_query('{AGENT_MODEL}', {_sql_literal(prompt)}) AS out"
    res = api.do(
        "POST",
        "/api/2.0/sql/statements",
        body={"statement": sql, "warehouse_id": warehouse_id, "wait_timeout": "50s"},
    )
    rows = (res.get("result") or {}).get("data_array") or []
    if rows and rows[0] and rows[0][0]:
        return rows[0][0]
    raise RuntimeError("agent LLM returned no output")


def _extract_json(text):
    """Pull the first JSON object out of an LLM reply (handles code fences)."""
    if not text:
        return None
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip())
    start = t.find("{")
    if start == -1:
        return None
    depth, in_str, esc = 0, False, False
    for i in range(start, len(t)):
        ch = t[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(t[start : i + 1])
                    except ValueError:
                        return None
    return None


def builtin_tools_desc():
    """Description of built-in tools: read-only SQL + HungryHop UC functions."""
    return (
        "- run_sql: run a read-only SQL SELECT against the HungryHop warehouse "
        "(catalog hop; schemas: analytics, genai). args: {\"query\": \"<SELECT statement>\"} "
        f"(results limited to {RESULT_LIMIT} rows)\n"
        "- lookup_order: verify one order by its order id. Returns status, delivery_class, "
        "ticket_id, issue_code, order_value, promised_minutes, minutes_past_promise, "
        "customer_tier. args: {\"order_id\": \"<ORD-xxxxx>\"}\n"
        "- search_sop: search the HungryHop SOP documents (policy, compensation rules). "
        "args: {\"query\": \"<search text>\"}\n"
        "- decide_compensation: decide the compensation for a verified order per the SOP. "
        "Call only AFTER lookup_order (and optionally search_sop). "
        "args: {\"delivery_class\": \"instant|standard\", \"tier\": \"free|hop_plus|hop_pro\", "
        "\"issue_code\": \"<from lookup_order or null>\", \"delivered\": true|false, "
        "\"minutes_past_promise\": <number or null>, \"order_value\": <number>}"
    )


def _num(v):
    """Render a number or NULL for SQL."""
    if v is None or v == "":
        return "NULL"
    try:
        return str(float(v))
    except (TypeError, ValueError):
        return "NULL"


def _bool(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if str(v).lower() in ("true", "false"):
        return str(v).lower()
    return "false"


def uc_tool_sql(name, args):
    """Build the read-only SELECT for a HungryHop UC-function tool.

    Returns the SQL string, or None if the tool name is not a UC tool.
    """
    a = args or {}
    if name == "lookup_order":
        oid = str(a.get("order_id") or "").strip()
        if not oid:
            return "ERROR: order_id is required"
        return f"SELECT * FROM hop.genai.lookup_order({_sql_literal(oid)})"
    if name == "search_sop":
        q = str(a.get("query") or "").strip()
        if not q:
            return "ERROR: query is required"
        return (
            f"SELECT doc_name, heading, text FROM hop.genai.search_sop({_sql_literal(q)}) "
            f"LIMIT 3"
        )
    if name == "decide_compensation":
        return (
            "SELECT hop.genai.decide_compensation("
            f"{_sql_literal(a.get('delivery_class') or '')}, "
            f"{_sql_literal(a.get('tier') or '')}, "
            + (
                "NULL, " if not a.get("issue_code")
                else f"{_sql_literal(a.get('issue_code'))}, "
            )
            + f"{_bool(a.get('delivered'))}, {_num(a.get('minutes_past_promise'))}, "
            f"{_num(a.get('order_value'))}) AS decision"
        )
    return None


def mcp_tools_desc(tools):
    """Describe tools discovered from the HungryHop MCP server."""
    parts = []
    for t in tools or []:
        parts.append(f"- {t.get('name') or '?'}: {t.get('description') or ''}")
    return "\n".join(parts)


def tool_run_sql(api, warehouse_id, query):
    """Built-in tool: read-only SQL execution with safety checks."""
    q = " ".join((query or "").strip().split(";")[0].split())
    if not q:
        return "ERROR: empty query"
    if not q.lower().startswith("select"):
        return "ERROR: only SELECT queries are allowed"
    if re.search(
        r"\b(insert|update|delete|drop|merge|truncate|alter|create|grant|revoke)\b",
        q.lower(),
    ):
        return "ERROR: only read-only SELECT queries are allowed"
    wrapped = f"SELECT * FROM ({q}) tool_result LIMIT {RESULT_LIMIT}"
    res = api.do(
        "POST",
        "/api/2.0/sql/statements",
        body={"statement": wrapped, "warehouse_id": warehouse_id, "wait_timeout": "50s"},
    )
    if res.get("status", {}).get("state") != "SUCCEEDED":
        err = (res.get("status", {}).get("error") or {}).get("message") or "query failed"
        return f"ERROR: {err}"
    cols = (res.get("manifest", {}).get("schema", {}) or {}).get("columns") or []
    header = " | ".join(c.get("name", "?") for c in cols)
    rows = [
        " | ".join("" if v is None else str(v) for v in r)
        for r in (res.get("result") or {}).get("data_array") or []
    ]
    out = [header] + rows
    if len(rows) == RESULT_LIMIT:
        out.append(f"(showing first {RESULT_LIMIT} rows)")
    return "\n".join(out)


def fetch_memories(api, warehouse_id):
    """LTM read: recent facts remembered from past conversations."""
    try:
        res = api.do(
            "POST",
            "/api/2.0/sql/statements",
            body={
                "statement": f"SELECT fact FROM {MEMORY_TABLE} ORDER BY created_at DESC LIMIT {MAX_MEMORIES}",
                "warehouse_id": warehouse_id,
                "wait_timeout": "50s",
            },
        )
        rows = (res.get("result") or {}).get("data_array") or []
        return [r[0] for r in rows if r and r[0]]
    except Exception:  # noqa: BLE001
        return []


def store_memory(api, warehouse_id, question, answer):
    """LTM write: the LLM itself decides which facts are worth remembering."""
    prompt = (
        "You maintain the long-term memory of the HungryHop AI assistant. Decide whether this "
        "exchange contains any lasting fact worth remembering for future conversations (owner "
        "preferences, important numbers, decisions, corrections). Small talk and one-off data "
        "lookups are NOT worth remembering.\n"
        "Reply with ONLY a JSON object like {\"facts\": [\"...\", ...]} with at most 2 short "
        "facts, or {\"facts\": []} if nothing is worth remembering.\n\n"
        f"USER: {question}\nASSISTANT: {str(answer)[:800]}"
    )
    obj = _extract_json(ask_llm(api, warehouse_id, prompt)) or {}
    facts = [str(f).strip()[:300] for f in (obj.get("facts") or []) if str(f).strip()][:2]
    for fact in facts:
        api.do(
            "POST",
            "/api/2.0/sql/statements",
            body={
                "statement": (
                    f"INSERT INTO {MEMORY_TABLE} (fact, kind, created_at) VALUES "
                    f"({_sql_literal(fact)}, 'conversation', current_timestamp())"
                ),
                "warehouse_id": warehouse_id,
                "wait_timeout": "50s",
            },
        )
    return facts


def run_agent(api, warehouse_id, question, history, tools_desc, call_tool):
    """Run the agent loop: guardrail-checked question in, markdown answer out.

    history = STM (list of {"role": "user"|"assistant", "text": ...}).
    tools_desc = string describing available tools to the LLM.
    call_tool(name, args) -> str executes a tool and returns its result.
    """
    memories = fetch_memories(api, warehouse_id)
    base = SYSTEM_PROMPT.format(
        tools=tools_desc or "(none)",
        memories="\n".join(f"- {m}" for m in reversed(memories)) or "(nothing remembered yet)",
    )
    conv_lines = []
    for h in (history or [])[-MAX_HISTORY:]:
        role = "user" if (h or {}).get("role") == "user" else "assistant"
        conv_lines.append(f"{role}: {str((h or {}).get('text') or '')[:600]}")
    conv = "\n".join(conv_lines)
    transcript = ""
    for i in range(MAX_STEPS):
        prompt = base
        if conv:
            prompt += "\n\nCONVERSATION SO FAR (short-term memory):\n" + conv
        if transcript:
            prompt += "\n" + transcript
        prompt += f"\n\nUSER QUESTION: {question}\nReply with ONE JSON object only."
        if i >= MAX_STEPS - 3:
            prompt += (
                "\nIMPORTANT: your tool budget is almost exhausted — reply with "
                'a final {"answer": ...} NOW unless one more tool call is truly essential.'
            )
        out = ask_llm(api, warehouse_id, prompt)
        obj = _extract_json(out)
        if obj is None:
            # Invalid/non-JSON reply: retry once with a corrective instruction.
            retry = ask_llm(
                api,
                warehouse_id,
                prompt
                + "\n\nYour previous reply was NOT a JSON object and therefore cannot be processed. "
                + 'Reply again with exactly ONE JSON object: {"tool": ...} to call a tool, or '
                + '{"answer": "<markdown reply, no SQL code>"} to answer.',
            )
            obj = _extract_json(retry)
            if obj is None:
                return retry.strip()[:2000] or out.strip()[:2000] or "I could not generate an answer."
        if obj.get("tool") and call_tool:
            name = str(obj["tool"])
            try:
                result = str(call_tool(name, obj.get("args") or {}))[:4000]
            except Exception as exc:  # noqa: BLE001
                result = f"ERROR: {exc}"
            transcript += f"\nTOOL RESULT ({name}):\n{result}\n"
            continue
        ans = str(obj.get("answer") or "").strip()
        return ans or out.strip()[:2000]
    # Tool budget exhausted: force a final answer from what we have so far.
    try:
        prompt = (
            base
            + ("\n" + transcript if transcript else "")
            + f"\n\nUSER QUESTION: {question}\n"
            + "No more tool calls are allowed. Using the tool results above, reply with "
            + 'ONLY a JSON object {"answer": "<markdown reply>"} now.'
        )
        out = ask_llm(api, warehouse_id, prompt)
        obj = _extract_json(out)
        if obj and str(obj.get("answer") or "").strip():
            return str(obj["answer"]).strip()[:2000]
        if out and obj is None:
            return out.strip()[:2000]
    except Exception:  # noqa: BLE001
        pass
    return "I could not complete that request. Please rephrase and try again."
