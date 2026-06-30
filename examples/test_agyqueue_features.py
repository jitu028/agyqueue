import os
import sys
import time
import json
import threading
from agyqueue.mcp_server import (
    submit_task, get_task_status, get_task_result,
    signal_workflow, get_workflow_history, list_active_workers, schedule_cron_workflow
)
from agyqueue.worker import process_task
from agyqueue.task_queue import TaskQueue
from agyqueue.storage import TaskStore

stop_worker = threading.Event()
worker_id_ref = None

def run_worker_in_background(workspace_root: str):
    global worker_id_ref
    print("[Worker] Background worker thread started.")
    queue = TaskQueue()
    store = TaskStore()
    
    import uuid
    worker_id = f"worker-test-{uuid.uuid4().hex[:4]}"
    worker_id_ref = worker_id
    store.register_worker(worker_id, "generic,manifest_compliance,fastapi_gen")
    
    last_heartbeat = time.time()
    while not stop_worker.is_set():
        if time.time() - last_heartbeat > 2.0:
            store.worker_heartbeat(worker_id)
            last_heartbeat = time.time()
            
        task_id = queue.dequeue(timeout=1)
        if task_id:
            print(f"\n[Worker] Claimed task {task_id}")
            process_task(task_id, store, workspace_root, worker_id)
            
    print("[Worker] Worker thread stopped.")

if __name__ == "__main__":
    print("=========================================================")
    print("       AgyQueue-Advanced E2E Integration Test            ")
    print("=========================================================")
    
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # 1. Start worker thread in background
    worker_thread = threading.Thread(target=run_worker_in_background, args=(workspace_root,), daemon=True)
    worker_thread.start()
    time.sleep(1.5) # Give worker time to register
    
    # 2. Test active worker listing
    print("\n[Test] Querying active workers...")
    workers_response = json.loads(list_active_workers())
    print(f" -> Active workers: {json.dumps(workers_response, indent=2)}")
    
    # 3. Test cron workflow scheduling (triggering every minute)
    print("\n[Test] Registering a recurring schedule (every minute)...")
    cron_expr = "*/1 * * * *"
    sched_response = json.loads(schedule_cron_workflow(
        cron_expression=cron_expr,
        workflow_type="generic",
        prompt="Periodic healthcheck via schedule"
    ))
    print(f" -> Schedule response: {json.dumps(sched_response, indent=2)}")
    
    # 4. Submit SRE task (which has a signal approval wait gate)
    prompt = "Orchestrate SRE manifest validation with security rules"
    print("\n[Test] Submitting SRE workflow...")
    submit_res = json.loads(submit_task(prompt=prompt, task_type="manifest_compliance"))
    task_id = submit_res["task_id"]
    print(f" -> Task submitted: {task_id}")
    
    # 5. Monitor and Signal when WAITING
    print("\n[Test] Monitoring workflow state for approval gate...")
    approval_sent = False
    
    while True:
        status_res = json.loads(get_task_status(task_id=task_id))
        status = status_res["status"]
        progress = status_res["progress"]
        step = status_res["step"]
        
        print(f" -> Task {task_id}: status={status}, progress={progress}%, step='{step}'")
        
        if status == "WAITING" and not approval_sent:
            print(f"\n[Test] Workflow is WAITING for approval. Sending 'approve' signal...")
            sig_res = json.loads(signal_workflow(
                task_id=task_id,
                signal_name="approve",
                payload="Approved by Lead SRE"
            ))
            print(f" -> Signal response: {json.dumps(sig_res, indent=2)}")
            approval_sent = True
            
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            print(f"\n[Test] Task reached terminal state: {status}")
            break
            
        time.sleep(1.5)
        
    # 6. Fetch Event History Log
    print("\n[Test] Fetching workflow event history...")
    history_res = json.loads(get_workflow_history(task_id=task_id))
    print("===================== EVENT TIMELINE =====================")
    for event in history_res:
        print(f"[{event['created_at']}] {event['event_type']:<20} | {event['payload']}")
    print("==========================================================")
    
    # Stop background worker
    stop_worker.set()
    worker_thread.join(timeout=2)
    print("\n[Test] Done.")
