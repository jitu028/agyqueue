import uuid
import json
import logging
import sys
import os
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse
from mcp.server.fastmcp import FastMCP
from agyqueue.models import Task, TaskStatus
from agyqueue.storage import TaskStore
from agyqueue.task_queue import TaskQueue

# Configure logging to stderr (since stdio is used for MCP messages)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger("agyqueue.mcp_server")

from agyqueue.config import settings
mcp = FastMCP(
    "AgyQueue",
    host=settings.host,
    port=settings.port
)

store = TaskStore()
queue = TaskQueue()

import asyncio
import threading
from datetime import datetime, timezone
import mcp.types as types
from mcp.server.session import ServerSession

# Session tracking via monkey-patching
active_sessions = set()
active_sessions_lock = threading.Lock()

async def send_catchup_notifications(session):
    # Give the session a brief moment to complete initialization
    await asyncio.sleep(1.0)
    try:
        tasks = store.list_tasks()
        logger.info(f"[SSE Push] Sending {len(tasks)} catch-up notification(s) to new session {id(session)}.")
        for task in tasks:
            notification = types.JSONRPCNotification(
                jsonrpc="2.0",
                method="notifications/task_updated",
                params={
                    "task_id": task.task_id,
                    "status": task.status.value,
                    "progress": task.progress,
                    "step": task.step
                }
            )
            try:
                await session._send(notification)
            except Exception as e:
                logger.debug(f"[SSE Push] Failed catchup for session {id(session)}: {e}")
    except Exception as e:
        logger.error(f"Error in send_catchup_notifications: {e}")

original_init = ServerSession.__init__
def new_init(self, *args, **kwargs):
    original_init(self, *args, **kwargs)
    with active_sessions_lock:
        active_sessions.add(self)
        logger.info(f"[MCP Session] Registered session {id(self)}. Total active: {len(active_sessions)}")
    
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(send_catchup_notifications(self))
    except RuntimeError:
        pass

original_aexit = ServerSession.__aexit__
async def new_aexit(self, exc_type, exc_val, exc_tb):
    try:
        await original_aexit(self, exc_type, exc_val, exc_tb)
    finally:
        with active_sessions_lock:
            active_sessions.discard(self)
            logger.info(f"[MCP Session] Discarded session {id(self)}. Total active: {len(active_sessions)}")

ServerSession.__init__ = new_init
ServerSession.__aexit__ = new_aexit

# Broadcast task updates to all connected clients
async def broadcast_task_notification(task_id: str, status: str, progress: int, step: str):
    notification = types.JSONRPCNotification(
        jsonrpc="2.0",
        method="notifications/task_updated",
        params={
            "task_id": task_id,
            "status": status,
            "progress": progress,
            "step": step
        }
    )
    with active_sessions_lock:
        sessions = list(active_sessions)
    if sessions:
        logger.info(f"[SSE Push] Broadcasting task {task_id} status change ({status}) to {len(sessions)} client(s).")
    for session in sessions:
        try:
            await session._send(notification)
        except Exception as e:
            logger.debug(f"[SSE Push] Failed to notify session {id(session)}: {e}")

# Dict to track last known state: { task_id: (status, progress, step) }
last_known_task_states = {}
db_monitor_task = None

def trigger_notifications_wrapper(task: Task):
    try:
        from agyqueue.notifications import notifications
        loop = asyncio.get_running_loop()
        loop.run_in_executor(
            None,
            notifications.trigger_notifications,
            task.task_id,
            task.status.value,
            task.progress,
            task.step,
            task.result,
            task.error
        )
    except Exception as e:
        logger.error(f"Error triggering notifications wrapper: {e}")

async def db_monitor_and_broadcast_loop(store: TaskStore, queue: TaskQueue):
    logger.info("Database monitor and push notifications loop started.")
    check_counter = 0
    while True:
        try:
            tasks = store.list_tasks()
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            
            # 1. Check for state updates and broadcast
            for task in tasks:
                state_key = task.task_id
                current_state = (task.status.value, task.progress, task.step)
                
                if state_key not in last_known_task_states:
                    last_known_task_states[state_key] = current_state
                    await broadcast_task_notification(task.task_id, task.status.value, task.progress, task.step)
                    if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                        trigger_notifications_wrapper(task)
                elif last_known_task_states[state_key] != current_state:
                    old_status = last_known_task_states[state_key][0]
                    last_known_task_states[state_key] = current_state
                    await broadcast_task_notification(task.task_id, task.status.value, task.progress, task.step)
                    if task.status != old_status and task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                        trigger_notifications_wrapper(task)
            
            # 2. Check for stale tasks (heartbeat timeout) every ~4.5 seconds
            check_counter += 1
            if check_counter >= 15:
                check_counter = 0
                for task in tasks:
                    if task.status == TaskStatus.RUNNING:
                        try:
                            updated_at = datetime.fromisoformat(task.updated_at).replace(tzinfo=None)
                            delta = (now - updated_at).total_seconds()
                            if delta > 15.0: # Heartbeat timeout threshold
                                logger.warning(f"[Timeout Recovery] Task {task.task_id} heartbeat expired (last updated {delta:.1f}s ago). Marking as FAILED.")
                                store.update_task(
                                    task_id=task.task_id,
                                    status=TaskStatus.FAILED,
                                    progress=100,
                                    step="Aborted due to worker heartbeat timeout.",
                                    error="Worker heartbeat timeout. The background worker executing this task may have crashed."
                                )
                                # If it has a parent in WAITING state, re-queue parent so it wakes up
                                if task.parent_id:
                                    parent = store.get_task(task.parent_id)
                                    if parent and parent.status == TaskStatus.WAITING:
                                        store.update_task(
                                            task_id=task.parent_id,
                                            status=TaskStatus.QUEUED,
                                            progress=60,
                                            step="Subtask timeout detected. Re-queueing parent for aggregation..."
                                        )
                                        queue.enqueue(task.parent_id)
                        except Exception as parse_err:
                            logger.error(f"Error parsing updated_at for task {task.task_id}: {parse_err}")
                            
        except Exception as e:
            logger.error(f"Error in DB monitor loop: {e}")
            
        await asyncio.sleep(0.3)

def start_db_monitor_if_needed():
    global db_monitor_task
    if db_monitor_task is None or db_monitor_task.done():
        try:
            loop = asyncio.get_running_loop()
            db_monitor_task = loop.create_task(db_monitor_and_broadcast_loop(store, queue))
            logger.info("Successfully scheduled background DB monitor loop on running event loop.")
        except RuntimeError:
            pass

@mcp.tool()
def submit_task(prompt: str, task_type: str = "generic", namespace: str = "default") -> str:
    """Submit a long-running task to the background queue.
    
    Args:
        prompt: The task instruction or compliance prompt (e.g. 'Validate this k8s manifest')
        task_type: The type of task (e.g., 'manifest_compliance', 'fastapi_gen', 'generic')
        namespace: The namespace to scope this task under (e.g. 'default', 'production')
        
    Returns:
        A JSON string containing the generated task_id and initial status.
    """
    start_db_monitor_if_needed()
    task_id = f"agy-{uuid.uuid4().hex[:8]}"
    task = Task(
        task_id=task_id,
        prompt=prompt,
        task_type=task_type,
        status=TaskStatus.QUEUED,
        progress=0,
        step="Queued in AgyQueue",
        namespace=namespace
    )
    store.save_task(task)
    queue.enqueue(task_id)
    
    logger.info(f"Submitted task {task_id} of type {task_type} in namespace {namespace}")
    return json.dumps({
        "task_id": task_id,
        "status": "QUEUED",
        "message": f"Task {task_id} successfully submitted. Use get_task_status to monitor progress."
    }, indent=2)

@mcp.tool()
def get_task_status(task_id: str) -> str:
    """Check the execution status, progress, and current step of a task.
    
    Args:
        task_id: The unique task ID returned by submit_task.
        
    Returns:
        A JSON string containing the status, progress, and current step details.
    """
    start_db_monitor_if_needed()
    task = store.get_task(task_id)
    if not task:
        return json.dumps({"error": f"Task {task_id} not found"}, indent=2)
        
    completed_at = task.updated_at if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED) else None
    return json.dumps({
        "task_id": task.task_id,
        "prompt": task.prompt,
        "task_type": task.task_type,
        "status": task.status.value,
        "progress": task.progress,
        "step": task.step,
        "parent_id": task.parent_id,
        "namespace": task.namespace,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
        "completed_at": completed_at,
        "result": task.result,
        "error": task.error
    }, indent=2)

@mcp.tool()
def get_task_result(task_id: str) -> str:
    """Retrieve the final execution result or error of a completed/failed task.
    
    Args:
        task_id: The unique task ID returned by submit_task.
        
    Returns:
        A JSON string containing the task result or failure reason.
    """
    start_db_monitor_if_needed()
    task = store.get_task(task_id)
    if not task:
        return json.dumps({"error": f"Task {task_id} not found"}, indent=2)
        
    if task.status in (TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.WAITING):
        return json.dumps({
            "task_id": task.task_id,
            "status": task.status.value,
            "progress": task.progress,
            "message": "Task is still running. Please wait for completion before requesting results."
        }, indent=2)

    return json.dumps({
        "task_id": task.task_id,
        "status": task.status.value,
        "result": task.result,
        "error": task.error
    }, indent=2)

@mcp.tool()
def list_tasks(namespace: Optional[str] = None) -> str:
    """List all submitted tasks and their current state summaries.
    
    Args:
        namespace: Optional namespace filter (e.g. 'default', 'production')
        
    Returns:
        A JSON string listing all tasks.
    """
    start_db_monitor_if_needed()
    tasks = store.list_tasks(namespace=namespace)
    if not tasks:
        return json.dumps([], indent=2)
        
    return json.dumps([
        {
            "task_id": t.task_id,
            "prompt": t.prompt[:60] + "..." if len(t.prompt) > 60 else t.prompt,
            "task_type": t.task_type,
            "status": t.status.value,
            "progress": t.progress,
            "step": t.step,
            "parent_id": t.parent_id,
            "namespace": t.namespace,
            "created_at": t.created_at,
            "updated_at": t.updated_at
        } for t in tasks
    ], indent=2)

@mcp.tool()
def cancel_task(task_id: str) -> str:
    """Request cancellation of a queued or running task.
    
    Args:
        task_id: The unique task ID to cancel.
        
    Returns:
        A JSON string confirming the cancellation status.
    """
    start_db_monitor_if_needed()
    task = store.get_task(task_id)
    if not task:
        return json.dumps({"error": f"Task {task_id} not found"}, indent=2)
        
    if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
        return json.dumps({
            "task_id": task.task_id,
            "status": task.status.value,
            "message": f"Task is already in a terminal state ({task.status.value})."
        }, indent=2)
        
    # Recursively cancel subtasks if this is an orchestrator/parent task
    subtasks = store.get_subtasks(task_id)
    for sub in subtasks:
        if sub.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            store.update_task(
                task_id=sub.task_id,
                status=TaskStatus.CANCELLED,
                progress=100,
                step="Cancelled by parent orchestrator cancellation request",
                error="Cancelled"
            )
            logger.info(f"Cancelled subtask {sub.task_id} of parent task {task_id}")
            
    # Cancel the main task
    store.update_task(
        task_id=task_id,
        status=TaskStatus.CANCELLED,
        progress=100,
        step="Cancelled by user request.",
        error="Cancelled"
    )
    logger.info(f"Cancelled task {task_id}")
    
    return json.dumps({
        "task_id": task_id,
        "status": "CANCELLED",
        "message": f"Task {task_id} cancellation requested. Active workloads will be aborted."
    }, indent=2)

# REST API Custom Routes
@mcp.custom_route("/api/tasks", methods=["POST"])
async def api_submit_task(request: Request) -> JSONResponse:
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON payload"}, status_code=400)
    
    prompt = data.get("prompt")
    if not prompt:
        return JSONResponse({"error": "Missing required field: prompt"}, status_code=400)
    
    task_type = data.get("task_type", "generic")
    namespace = data.get("namespace", "default")
    res_str = submit_task(prompt, task_type, namespace)
    return JSONResponse(json.loads(res_str))

@mcp.custom_route("/api/tasks/{task_id}", methods=["GET"])
async def api_get_task_status(request: Request) -> JSONResponse:
    task_id = request.path_params.get("task_id")
    task = store.get_task(task_id)
    if not task:
        return JSONResponse({"error": f"Task {task_id} not found"}, status_code=404)
    res_str = get_task_status(task_id)
    return JSONResponse(json.loads(res_str))

@mcp.custom_route("/api/tasks/{task_id}/result", methods=["GET"])
async def api_get_task_result(request: Request) -> JSONResponse:
    task_id = request.path_params.get("task_id")
    task = store.get_task(task_id)
    if not task:
        return JSONResponse({"error": f"Task {task_id} not found"}, status_code=404)
    res_str = get_task_result(task_id)
    return JSONResponse(json.loads(res_str))

@mcp.custom_route("/api/tasks", methods=["GET"])
async def api_list_tasks(request: Request) -> JSONResponse:
    namespace = request.query_params.get("namespace")
    res_str = list_tasks(namespace=namespace)
    return JSONResponse(json.loads(res_str))

@mcp.custom_route("/api/tasks/{task_id}/cancel", methods=["POST"])
async def api_cancel_task(request: Request) -> JSONResponse:
    task_id = request.path_params.get("task_id")
    task = store.get_task(task_id)
    if not task:
        return JSONResponse({"error": f"Task {task_id} not found"}, status_code=404)
    res_str = cancel_task(task_id)
    return JSONResponse(json.loads(res_str))
@mcp.custom_route("/dashboard", methods=["GET"])
async def serve_dashboard(request: Request) -> HTMLResponse:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(current_dir, "dashboard.html")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(html_content)
    except Exception as e:
        return HTMLResponse(f"<h3>Error loading dashboard: {str(e)}</h3>", status_code=500)

def main():
    transport = settings.transport
    
    if transport == "sse":
        logger.info("Starting AgyQueue MCP server via SSE transport...")
        mcp.run(transport="sse")
    else:
        logger.info("Starting AgyQueue MCP server via STDIO transport...")
        mcp.run(transport="stdio")

if __name__ == "__main__":
    main()

