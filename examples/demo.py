import sys
import os
import time
import json
import threading
from agyqueue.mcp_server import submit_task, get_task_status, get_task_result
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
    print("           AgyQueue End-to-End Demo Script               ")
    print("=========================================================")
    
    workspace_root = os.path.dirname(os.path.abspath(__file__))
    
    # 1. Start worker thread in background
    worker_thread = threading.Thread(target=run_worker_in_background, args=(workspace_root,), daemon=True)
    worker_thread.start()
    time.sleep(1) # Give worker time to spin up
    
    # 2. Submit a Multi-Agent Orchestration Task
    prompt = "Orchestrate deployment check and API service monitor validation"
    print(f"\n[Agent] Submitting Multi-Agent Orchestration task...")
    submit_response_str = submit_task(prompt=prompt, task_type="multi_agent_deploy")
    submit_response = json.loads(submit_response_str)
    
    task_id = submit_response["task_id"]
    print(f"[Agent] Task submitted successfully! Task ID: {task_id}")
    print(f"[Agent] Client conversation remains responsive! Let's check status...")
    
    # 3. Poll status
    print("\n[Agent] Monitoring background task progress...")
    while True:
        status_response_str = get_task_status(task_id=task_id)
        status_response = json.loads(status_response_str)
        
        status = status_response["status"]
        progress = status_response["progress"]
        step = status_response["step"]
        
        print(f" -> Status: {status:<10} | Progress: {progress:3d}% | Step: {step}")
        
        if status in ("COMPLETED", "FAILED"):
            break
            
        time.sleep(2)
        
    # 4. Fetch and display final results
    print("\n[Agent] Task finished! Fetching results...")
    result_response_str = get_task_result(task_id=task_id)
    result_response = json.loads(result_response_str)
    
    if result_response["status"] == "COMPLETED":
        print("\n=================== FINAL AGGREGATED REPORT ===================")
        print(result_response["result"])
        print("===============================================================")
    else:
        print(f"\n[Agent] Task failed with error: {result_response['error']}")
        
    # Stop background worker
    stop_worker.set()
    worker_thread.join(timeout=2)
    print("\n[Demo] Done.")
