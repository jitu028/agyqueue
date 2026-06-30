import asyncio
import json
import time
import os

from mcp import ClientSession
from mcp.client.sse import sse_client

SERVER_URL = os.environ.get("AGYQUEUE_SERVER_URL", "http://localhost:8000")
URL = f"{SERVER_URL.rstrip('/')}/sse"


def parse_text(result):
    if not result.content:
        return None
    text = result.content[0].text
    try:
        return json.loads(text)
    except Exception:
        return text


def summarize(value, max_len=220):
    text = json.dumps(value, ensure_ascii=True) if not isinstance(value, str) else value
    text = text.replace("\n", " ")
    return text if len(text) <= max_len else text[: max_len - 3] + "..."


async def call(session, name, arguments=None):
    arguments = arguments or {}
    started = time.time()
    try:
        result = await session.call_tool(name, arguments)
        parsed = parse_text(result)
        elapsed_ms = int((time.time() - started) * 1000)
        print(f"PASS {name} {elapsed_ms}ms {summarize(parsed)}")
        return parsed
    except Exception as exc:
        elapsed_ms = int((time.time() - started) * 1000)
        print(f"FAIL {name} {elapsed_ms}ms {type(exc).__name__}: {exc}")
        return None


async def wait_terminal(session, task_id):
    last = None
    for _ in range(20):
        last = await call(session, "get_task_status", {"task_id": task_id})
        if isinstance(last, dict) and last.get("status") in {"COMPLETED", "FAILED", "CANCELLED"}:
            return last
        await asyncio.sleep(1)
    return last


async def main():
    async with sse_client(URL) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools = await session.list_tools()
            print("TOOLS " + ", ".join(tool.name for tool in tools.tools))

            prompt = "MCP smoke test: respond with a short confirmation that AgyQueue is working."
            submitted = await call(session, "submit_task", {
                "prompt": prompt,
                "task_type": "generic",
                "namespace": "default",
            })
            task_id = submitted.get("task_id") if isinstance(submitted, dict) else None

            await call(session, "list_tasks", {"namespace": "default"})

            if task_id:
                await wait_terminal(session, task_id)
                await call(session, "get_task_result", {"task_id": task_id})
                artifacts = await call(session, "list_artifacts", {"task_id": task_id})
                if isinstance(artifacts, dict):
                    paths = artifacts.get("artifacts") or artifacts.get("files") or []
                elif isinstance(artifacts, list):
                    paths = artifacts
                else:
                    paths = []
                if paths:
                    first = paths[0] if isinstance(paths[0], str) else paths[0].get("path")
                    if first:
                        await call(session, "fetch_artifact", {"task_id": task_id, "relative_path": first})
                else:
                    print("SKIP fetch_artifact no artifacts returned")
                await call(session, "get_workflow_history", {"task_id": task_id})
                await call(session, "signal_workflow", {
                    "task_id": task_id,
                    "signal_name": "approve",
                    "payload": "smoke-test signal",
                })
                await call(session, "retry_task", {"task_id": task_id})

            cancellable = await call(session, "submit_task", {
                "prompt": "MCP smoke test: task submitted only to exercise cancellation.",
                "task_type": "generic",
                "namespace": "default",
            })
            cancel_id = cancellable.get("task_id") if isinstance(cancellable, dict) else None
            if cancel_id:
                await call(session, "cancel_task", {"task_id": cancel_id})

            await call(session, "list_active_workers")
            await call(session, "schedule_cron_workflow", {
                "cron_expression": "0 0 31 2 *",
                "workflow_type": "generic",
                "prompt": "MCP smoke test non-firing cron entry",
                "namespace": "default",
            })


if __name__ == "__main__":
    asyncio.run(main())
