import os
import sys
import time
import json
import threading
from agyqueue.mcp_server import submit_task, get_task_status, get_task_result, list_artifacts, fetch_artifact, retry_task
from agyqueue.worker import process_task
from agyqueue.task_queue import TaskQueue
from agyqueue.storage import TaskStore

stop_worker = threading.Event()

def run_worker_in_background(workspace_root: str):
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
    print("      Testing AgyQueue Artifacts & Retry Logics          ")
    print("=========================================================")
    
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # 1. Start worker thread in background
    worker_thread = threading.Thread(target=run_worker_in_background, args=(workspace_root,), daemon=True)
    worker_thread.start()
    time.sleep(1)
    
    # 2. Submit an SRE manifest compliance task
    prompt = "Validate Kubernetes deployment security rules"
    print("\n[Agent] Submitting SRE Validation task...")
    submit_response_str = submit_task(prompt=prompt, task_type="manifest_compliance")
    submit_response = json.loads(submit_response_str)
    task_id = submit_response["task_id"]
    print(f"[Agent] Task submitted successfully! Task ID: {task_id}")
    
    # 3. Wait for the task to complete
    print("\n[Agent] Waiting for task completion...")
    while True:
        status_response = json.loads(get_task_status(task_id=task_id))
        status = status_response["status"]
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            print(f"[Agent] Task reached terminal state: {status}")
            break
        time.sleep(1)
        
    # 4. List artifacts
    print("\n[Agent] Querying task artifacts...")
    artifacts_response_str = list_artifacts(task_id=task_id)
    artifacts_response = json.loads(artifacts_response_str)
    print(f"[Agent] list_artifacts response:\n{json.dumps(artifacts_response, indent=2)}")
    
    # 5. Fetch each artifact content
    if "artifacts" in artifacts_response and artifacts_response["artifacts"]:
        for artifact in artifacts_response["artifacts"]:
            print(f"\n[Agent] Fetching content of artifact: {artifact}")
            content = fetch_artifact(task_id=task_id, relative_path=artifact)
            print("---------------------------------------------------------")
            print(content)
            print("---------------------------------------------------------")
            
    # 6. Test Retry logic
    print(f"\n[Agent] Retrying task: {task_id}")
    retry_response_str = retry_task(task_id=task_id)
    retry_response = json.loads(retry_response_str)
    print(f"[Agent] retry_task response:\n{json.dumps(retry_response, indent=2)}")
    
    # 7. Wait for retried task to finish
    print("\n[Agent] Waiting for retried task completion...")
    while True:
        status_response = json.loads(get_task_status(task_id=task_id))
        status = status_response["status"]
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            print(f"[Agent] Retried task reached terminal state: {status}")
            break
        time.sleep(1)
        
    # Stop background worker
    stop_worker.set()
    worker_thread.join(timeout=2)
    print("\n[Test] Done.")
