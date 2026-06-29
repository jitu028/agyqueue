import sys
import os
import time
import json
import threading
from agyqueue.mcp_server import submit_task, get_task_status, get_task_result, cancel_task
from agyqueue.worker import process_task
from agyqueue.task_queue import TaskQueue
from agyqueue.storage import TaskStore

# Flag to stop the worker thread later
stop_worker = threading.Event()

def run_worker_in_background(workspace_root: str):
    """Simulates a background worker thread that polls the queue."""
    print("[Worker] Background worker thread started. Monitoring tasks...")
    queue = TaskQueue()
    store = TaskStore()
    
    while not stop_worker.is_set():
        task_id = queue.dequeue(timeout=1)
        if task_id:
            print(f"\n[Worker] Picked up task: {task_id}")
            process_task(task_id, store, workspace_root)
    print("[Worker] Worker thread stopped.")

if __name__ == "__main__":
    print("=========================================================")
    print("        AgyQueue Task Cancellation Demo Script           ")
    print("=========================================================")
    
    workspace_root = os.path.dirname(os.path.abspath(__file__))
    
    # 1. Start worker thread in background
    worker_thread = threading.Thread(target=run_worker_in_background, args=(workspace_root,), daemon=True)
    worker_thread.start()
    time.sleep(1) # Give worker time to spin up
    
    # 2. Submit a long-running FastAPI Generation Task
    prompt = "Create a large FastAPI math API with complex tests"
    print(f"\n[Agent] Submitting FastAPI Gen task...")
    submit_response_str = submit_task(prompt=prompt, task_type="fastapi_gen")
    submit_response = json.loads(submit_response_str)
    
    task_id = submit_response["task_id"]
    print(f"[Agent] Task submitted successfully! Task ID: {task_id}")
    
    # 3. Wait a brief moment to let the worker pick it up and run it
    print("\n[Agent] Waiting 3 seconds to let execution begin...")
    time.sleep(3)
    
    # Check current status (should be RUNNING)
    status_response_str = get_task_status(task_id=task_id)
    status_response = json.loads(status_response_str)
    print(f"[Agent] Current status: {status_response['status']} | Progress: {status_response['progress']}% | Step: {status_response['step']}")
    
    # 4. Trigger Cancellation!
    print(f"\n[Agent] Triggering cancellation for task: {task_id}...")
    cancel_response_str = cancel_task(task_id=task_id)
    cancel_response = json.loads(cancel_response_str)
    print(f"[Agent] Cancel Response: {cancel_response['message']}")
    
    # 5. Poll status to watch it transition to CANCELLED
    print("\n[Agent] Monitoring task to check if worker aborts correctly...")
    for _ in range(5):
        status_response_str = get_task_status(task_id=task_id)
        status_response = json.loads(status_response_str)
        
        status = status_response["status"]
        progress = status_response["progress"]
        step = status_response["step"]
        
        print(f" -> Status: {status:<10} | Progress: {progress:3d}% | Step: {step}")
        
        if status == "CANCELLED":
            break
            
        time.sleep(1)
        
    # 6. Fetch final results
    print("\n[Agent] Fetching final results to inspect cancellation report...")
    result_response_str = get_task_result(task_id=task_id)
    result_response = json.loads(result_response_str)
    print("\n=================== FINAL DATA STORE RECORD ===================")
    print(json.dumps(result_response, indent=2))
    print("===============================================================")
        
    # Stop background worker
    stop_worker.set()
    worker_thread.join(timeout=2)
    print("\n[Demo] Done.")
