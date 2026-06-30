import uuid
import json
import logging
import sys
import os
from typing import Optional
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse, FileResponse
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

def is_cron_due(cron_expr: str, last_run_iso: Optional[str]) -> bool:
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if last_run_iso:
            last_run = datetime.fromisoformat(last_run_iso)
            if last_run.minute == now.minute and last_run.hour == now.hour and last_run.day == now.day:
                return False
        
        parts = cron_expr.split()
        if len(parts) != 5:
            return False
            
        def match_part(val: int, expr: str) -> bool:
            if expr == "*":
                return True
            if expr.startswith("*/"):
                try:
                    divisor = int(expr[2:])
                    return val % divisor == 0
                except ValueError:
                    return False
            if "," in expr:
                return any(match_part(val, sub) for sub in expr.split(","))
            if "-" in expr:
                try:
                    start, end = map(int, expr.split("-"))
                    return start <= val <= end
                except ValueError:
                    return False
            try:
                return val == int(expr)
            except ValueError:
                return False

        cron_minute, cron_hour, cron_day, cron_month, cron_weekday = parts
        
        if not match_part(now.minute, cron_minute):
            return False
        if not match_part(now.hour, cron_hour):
            return False
        if not match_part(now.day, cron_day):
            return False
        if not match_part(now.month, cron_month):
            return False
        cron_wd = now.weekday() + 1
        if cron_wd == 7:
            if not (match_part(0, cron_weekday) or match_part(7, cron_weekday)):
                return False
        else:
            if not match_part(cron_wd, cron_weekday):
                return False
                
        return True
    except Exception as e:
        logger.error(f"Error evaluating cron: {e}")
        return False

async def cron_scheduler_loop(store: TaskStore, queue: TaskQueue):
    logger.info("Cron scheduler loop started.")
    while True:
        try:
            schedules = store.list_schedules(active_only=True)
            for sched in schedules:
                cron_expr = sched["cron_expression"]
                last_run = sched.get("last_run_at")
                if is_cron_due(cron_expr, last_run):
                    task_id = f"agy-cron-{uuid.uuid4().hex[:8]}"
                    task = Task(
                        task_id=task_id,
                        prompt=sched["prompt"],
                        task_type=sched["workflow_type"],
                        status=TaskStatus.QUEUED,
                        progress=0,
                        step="Triggered by cron scheduler",
                        namespace=sched["namespace"]
                    )
                    store.save_task(task)
                    queue.enqueue(task_id)
                    store.update_schedule_run(sched["schedule_id"])
                    store.log_event(task_id, "WorkflowStarted", f"Spawned by schedule {sched['schedule_id']}")
                    logger.info(f"[Scheduler] Triggered scheduled workflow {task_id} for cron '{cron_expr}'")
        except Exception as e:
            logger.error(f"Error in cron scheduler loop: {e}")
        await asyncio.sleep(10.0)

scheduler_task = None

def start_db_monitor_if_needed():
    global db_monitor_task, scheduler_task
    if db_monitor_task is None or db_monitor_task.done():
        try:
            loop = asyncio.get_running_loop()
            db_monitor_task = loop.create_task(db_monitor_and_broadcast_loop(store, queue))
            logger.info("Successfully scheduled background DB monitor loop on running event loop.")
        except RuntimeError:
            pass
            
    if scheduler_task is None or scheduler_task.done():
        try:
            loop = asyncio.get_running_loop()
            scheduler_task = loop.create_task(cron_scheduler_loop(store, queue))
            logger.info("Successfully scheduled cron scheduler loop on running event loop.")
        except RuntimeError:
            pass

@mcp.tool()
def submit_task(prompt: str, task_type: str = "generic", namespace: str = "default", approver_name: str = "", approver_email: str = "") -> str:
    """Submit a long-running task to the background queue.
    
    Args:
        prompt: The task instruction or compliance prompt (e.g. 'Validate this k8s manifest')
        task_type: The type of task (e.g., 'manifest_compliance', 'fastapi_gen', 'generic')
        namespace: The namespace to scope this task under (e.g. 'default', 'production')
        approver_name: Optional name of the release manager/approver
        approver_email: Optional email to notify when the task pauses for approval
        
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
        namespace=namespace,
        approver_name=approver_name if approver_name else None,
        approver_email=approver_email if approver_email else None
    )
    store.save_task(task)
    queue.enqueue(task_id)
    
    logger.info(f"Submitted task {task_id} of type {task_type} in namespace {namespace}")
    dashboard_url = f"{settings.server_url}/dashboard?run={task_id}"
    return json.dumps({
        "task_id": task_id,
        "status": "QUEUED",
        "message": f"Task {task_id} successfully submitted. Monitor progress in real-time at: {dashboard_url}"
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

@mcp.tool()
def retry_task(task_id: str) -> str:
    """Retry a failed, cancelled, or completed task by re-queueing it.
    
    Args:
        task_id: The unique task ID to retry.
        
    Returns:
        A JSON string confirming the retry status.
    """
    start_db_monitor_if_needed()
    task = store.get_task(task_id)
    if not task:
        return json.dumps({"error": f"Task {task_id} not found"}, indent=2)
        
    # Re-queue the task
    store.update_task(
        task_id=task_id,
        status=TaskStatus.QUEUED,
        progress=0,
        step="Queued for retry",
        result="",  # Clear previous result
        error=""    # Clear previous error
    )
    queue.enqueue(task_id)
    
    logger.info(f"Retried task {task_id}")
    return json.dumps({
        "task_id": task_id,
        "status": "QUEUED",
        "message": f"Task {task_id} successfully re-queued for execution."
    }, indent=2)

@mcp.tool()
def list_artifacts(task_id: str) -> str:
    """List all saved file artifacts for a completed task.
    
    Args:
        task_id: The unique task ID to list artifacts for.
        
    Returns:
        A JSON string listing all artifact file paths relative to the task's artifact directory.
    """
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    artifacts_dir = os.path.join(workspace_root, "data", "artifacts", task_id)
    
    local_files = []
    if os.path.exists(artifacts_dir):
        for root, _, files in os.walk(artifacts_dir):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, artifacts_dir)
                local_files.append(rel_path)
                
    try:
        from agyqueue.gcs_helper import list_gcs_artifacts
        gcs_files = list_gcs_artifacts(task_id)
    except Exception:
        gcs_files = []
        
    all_files = sorted(list(set(local_files + gcs_files)))
    return json.dumps({
        "task_id": task_id,
        "artifacts": all_files
    }, indent=2)

@mcp.tool()
def fetch_artifact(task_id: str, relative_path: str) -> str:
    """Retrieve the content of a specific task artifact.
    
    Args:
        task_id: The unique task ID the artifact belongs to.
        relative_path: The relative path of the artifact file as returned by list_artifacts.
        
    Returns:
        A string containing the artifact file content, or a JSON string with an error message.
    """
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    artifacts_dir = os.path.abspath(os.path.join(workspace_root, "data", "artifacts", task_id))
    target_path = os.path.abspath(os.path.join(artifacts_dir, relative_path))
    
    if not target_path.startswith(artifacts_dir):
        return json.dumps({"error": "Access denied: Invalid artifact path."}, indent=2)
        
    if not os.path.exists(target_path) or not os.path.isfile(target_path):
        try:
            from agyqueue.gcs_helper import download_from_gcs
            success = download_from_gcs(task_id, relative_path, target_path)
            if not success:
                return json.dumps({"error": f"Artifact '{relative_path}' not found for task {task_id}."}, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Failed to retrieve from GCS: {str(e)}"}, indent=2)
            
    try:
        with open(target_path, "r", encoding="utf-8") as f:
            content = f.read()
        return content
    except Exception as e:
        return json.dumps({"error": f"Failed to read artifact: {str(e)}"}, indent=2)

@mcp.tool()
def signal_workflow(task_id: str, signal_name: str, payload: Optional[str] = None) -> str:
    """Send a signal to an active workflow to resume execution or provide approval inputs.
    
    Args:
        task_id: The unique task ID of the target workflow.
        signal_name: The name of the signal (e.g. 'approve').
        payload: Optional string metadata/inputs accompanying the signal.
        
    Returns:
        A JSON string confirming the signal registration.
    """
    start_db_monitor_if_needed()
    task = store.get_task(task_id)
    if not task:
        return json.dumps({"error": f"Workflow {task_id} not found"}, indent=2)
        
    signal_id = store.create_signal(task_id, signal_name, payload)
    
    # If the workflow is in WAITING state, re-queue it so it wakes up and processes the signal!
    if task.status == TaskStatus.WAITING:
        store.update_task(
            task_id=task_id,
            status=TaskStatus.QUEUED,
            progress=task.progress,
            step=f"Signal '{signal_name}' received. Resuming workflow execution..."
        )
        queue.enqueue(task_id)
        
    return json.dumps({
        "signal_id": signal_id,
        "task_id": task_id,
        "status": "SIGNALED",
        "message": f"Signal '{signal_name}' successfully sent to workflow {task_id}."
    }, indent=2)

@mcp.tool()
def get_workflow_history(task_id: str) -> str:
    """Retrieve the durable chronological event history of a workflow.
    
    Args:
        task_id: The unique task ID of the workflow.
        
    Returns:
        A JSON string listing the workflow execution events.
    """
    start_db_monitor_if_needed()
    events = store.get_events(task_id)
    return json.dumps(events, indent=2)

@mcp.tool()
def list_active_workers() -> str:
    """List all registered background worker nodes currently active.
    
    Returns:
        A JSON string listing active workers.
    """
    start_db_monitor_if_needed()
    workers = store.list_active_workers()
    return json.dumps(workers, indent=2)

@mcp.tool()
def schedule_cron_workflow(cron_expression: str, workflow_type: str, prompt: str, namespace: str = "default") -> str:
    """Schedule a workflow task to run periodically using a cron expression.
    
    Args:
        cron_expression: A 5-field cron expression (e.g. '*/5 * * * *' for every 5 minutes).
        workflow_type: The task type to execute (e.g. 'manifest_compliance').
        prompt: The task instruction prompt.
        namespace: The namespace scope.
        
    Returns:
        A JSON string confirming the schedule registration.
    """
    start_db_monitor_if_needed()
    import uuid
    schedule_id = f"sched-{uuid.uuid4().hex[:8]}"
    store.save_schedule(schedule_id, cron_expression, workflow_type, prompt, namespace)
    return json.dumps({
        "schedule_id": schedule_id,
        "cron_expression": cron_expression,
        "workflow_type": workflow_type,
        "message": f"Schedule '{schedule_id}' registered successfully."
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
    approver_name = data.get("approver_name", "")
    approver_email = data.get("approver_email", "")
    res_str = submit_task(prompt, task_type, namespace, approver_name, approver_email)
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

@mcp.custom_route("/api/tasks/{task_id}/retry", methods=["POST"])
async def api_retry_task(request: Request) -> JSONResponse:
    task_id = request.path_params.get("task_id")
    task = store.get_task(task_id)
    if not task:
        return JSONResponse({"error": f"Task {task_id} not found"}, status_code=404)
    res_str = retry_task(task_id)
    return JSONResponse(json.loads(res_str))

@mcp.custom_route("/api/tasks/{task_id}/artifacts", methods=["GET"])
async def api_list_artifacts(request: Request) -> JSONResponse:
    task_id = request.path_params.get("task_id")
    
    # Get local files
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    artifacts_dir = os.path.join(workspace_root, "data", "artifacts", task_id)
    local_files = []
    if os.path.exists(artifacts_dir):
        for root, _, files in os.walk(artifacts_dir):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, artifacts_dir)
                local_files.append(rel_path)
                
    # Get GCS files
    try:
        from agyqueue.gcs_helper import list_gcs_artifacts
        gcs_files = list_gcs_artifacts(task_id)
    except Exception:
        gcs_files = []
        
    all_files = sorted(list(set(local_files + gcs_files)))
    return JSONResponse({
        "task_id": task_id,
        "artifacts": all_files
    })

@mcp.custom_route("/api/tasks/{task_id}/artifacts/{relative_path:path}", methods=["GET"])
async def api_fetch_artifact(request: Request):
    task_id = request.path_params.get("task_id")
    relative_path = request.path_params.get("relative_path")
    
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    artifacts_dir = os.path.abspath(os.path.join(workspace_root, "data", "artifacts", task_id))
    target_path = os.path.abspath(os.path.join(artifacts_dir, relative_path))
    
    if not target_path.startswith(artifacts_dir):
        return JSONResponse({"error": "Access denied: Invalid artifact path."}, status_code=403)
        
    if not os.path.exists(target_path) or not os.path.isfile(target_path):
        # Download from GCS to local cache
        try:
            from agyqueue.gcs_helper import download_from_gcs
            success = download_from_gcs(task_id, relative_path, target_path)
            if not success:
                return JSONResponse({"error": f"Artifact '{relative_path}' not found for task {task_id}."}, status_code=404)
        except Exception as e:
            return JSONResponse({"error": f"Failed to retrieve from GCS: {str(e)}"}, status_code=500)
            
    return FileResponse(target_path)

@mcp.custom_route("/api/tasks/{task_id}/signal", methods=["POST"])
async def api_signal_workflow(request: Request) -> JSONResponse:
    task_id = request.path_params.get("task_id")
    try:
        data = await request.json()
    except Exception:
        data = {}
    signal_name = data.get("signal_name", "approve")
    payload = data.get("payload")
    res_str = signal_workflow(task_id, signal_name, payload)
    return JSONResponse(json.loads(res_str))

@mcp.custom_route("/api/tasks/{task_id}/history", methods=["GET"])
async def api_get_workflow_history(request: Request) -> JSONResponse:
    task_id = request.path_params.get("task_id")
    res_str = get_workflow_history(task_id)
    return JSONResponse(json.loads(res_str))

@mcp.custom_route("/api/workers", methods=["GET"])
async def api_list_active_workers(request: Request) -> JSONResponse:
    res_str = list_active_workers()
    return JSONResponse(json.loads(res_str))

@mcp.custom_route("/api/schedules", methods=["POST"])
async def api_schedule_cron_workflow(request: Request) -> JSONResponse:
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON payload"}, status_code=400)
    cron = data.get("cron_expression")
    wtype = data.get("workflow_type")
    prompt = data.get("prompt")
    ns = data.get("namespace", "default")
    if not (cron and wtype and prompt):
        return JSONResponse({"error": "Missing required fields"}, status_code=400)
    res_str = schedule_cron_workflow(cron, wtype, prompt, ns)
    return JSONResponse(json.loads(res_str))
@mcp.custom_route("/a2a/app/.well-known/agent-card.json", methods=["GET"])
@mcp.custom_route("/.well-known/agent-card.json", methods=["GET"])
@mcp.custom_route("/.well-known/agent.json", methods=["GET"])
async def serve_agent_card(request: Request) -> JSONResponse:
    card = {
        "schema_version": "v1",
        "name": "AgyQueue",
        "description": "A pluggable, non-blocking asynchronous task queue and MCP server for AI Agents",
        "url": settings.server_url,
        "version": "0.1.2",
        "protocolVersion": "1.0",
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "capabilities": {
            "streaming": True
        },
        "skills": [
            {
                "id": "agyqueue",
                "name": "AgyQueue Task Management",
                "description": "Enables creation, cancellation, status checks, and retrieval of background tasks.",
                "tags": ["task-queue", "mcp-server"]
            }
        ]
    }
    return JSONResponse(card)

@mcp.custom_route("/review/{task_id}", methods=["GET"])
async def serve_review_page(request: Request) -> HTMLResponse:
    task_id = request.path_params.get("task_id")
    task = store.get_task(task_id)
    if not task:
        return HTMLResponse("<h3>Task not found</h3>", status_code=404)
        
    events = store.get_events(task_id)
    linter_log = "Linter scan logs not available yet."
    for ev in events:
        if ev.get("event_type") == "LinterOutput":
            linter_log = ev.get("payload", "")
            break
            
    current_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(current_dir, "review.html")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        
        # Simple placeholder replacement
        html_content = html_content.replace("{{task_id}}", task.task_id)
        html_content = html_content.replace("{{status}}", task.status.value)
        html_content = html_content.replace("{{namespace}}", task.namespace)
        html_content = html_content.replace("{{prompt}}", task.prompt)
        html_content = html_content.replace("{{linter_log}}", linter_log)
        
        return HTMLResponse(html_content)
    except Exception as e:
        return HTMLResponse(f"<h3>Error loading review page: {str(e)}</h3>", status_code=500)

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

