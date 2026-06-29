import time
from agyqueue.client import AgyQueueClient

def main():
    print("=========================================================")
    print("Initializing AgyQueue Client SDK...")
    
    # Initialize the zero-dependency REST Client SDK
    # Connects to http://127.0.0.1:8000 by default (overridable via environment)
    client = AgyQueueClient()
    
    # 1. Submit task
    print("\nSubmitting task using Client SDK...")
    submit_res = client.submit_task(
        prompt="Validate Kubernetes resource deployment limits",
        task_type="manifest_compliance"
    )
    
    if "error" in submit_res:
        print(f"Error submitting task: {submit_res['error']}")
        print("Ensure the AgyQueue SSE server is running locally.")
        return
        
    task_id = submit_res.get("task_id")
    print(f"Task successfully enqueued! Task ID: {task_id}")
    
    # 2. Monitor task progress
    print(f"\nMonitoring progress for task {task_id}...")
    while True:
        status_res = client.get_task_status(task_id)
        if "error" in status_res:
            print(f"Error fetching status: {status_res['error']}")
            break
            
        status = status_res.get("status")
        progress = status_res.get("progress")
        step = status_res.get("step")
        
        print(f" -> Status: {status:<10} | Progress: {progress:>3}% | Step: {step}")
        
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(1.5)
        
    # 3. Retrieve final outputs
    if status == "COMPLETED":
        print("\nRetrieving final task execution report:")
        result_res = client.get_task_result(task_id)
        print("---------------------------------------------------------")
        print(result_res.get("result"))
        print("---------------------------------------------------------")

if __name__ == "__main__":
    main()
