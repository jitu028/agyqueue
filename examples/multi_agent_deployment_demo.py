import os
import sys
import time
from agyqueue.client import AgyQueueClient

def main():
    print("=========================================================")
    print("      AgyQueue Multi-Agent Orchestration Demo            ")
    print("=========================================================")
    
    server_url = os.environ.get("AGYQUEUE_SERVER_URL", "http://localhost:8000")
    print(f"Connecting to AgyQueue Server: {server_url}\n")
    client = AgyQueueClient(base_url=server_url)
    
    # 1. Submit parent orchestrator task
    parent_prompt = "Deploy billing microservice pipeline with SRE guardrails and FastAPI generators"
    print("Submitting Multi-Agent Orchestrator task...")
    submit_res = client.submit_task(
        prompt=parent_prompt,
        task_type="multi_agent_deploy"
    )
    
    if "error" in submit_res and submit_res["error"] is not None:
        print(f"Error submitting task: {submit_res['error']}")
        return
        
    parent_id = submit_res.get("task_id")
    print(f"Parent Orchestrator successfully enqueued! Task ID: {parent_id}")
    
    # 2. Track parent and subtasks in real-time
    print("\nTracking multi-agent execution pipeline...")
    print("-" * 80)
    print(f"{'Task / Subtask':<35} | {'Status':<12} | {'Progress':<8} | {'Step/Message':<20}")
    print("-" * 80)
    
    subtask_sre_id = f"{parent_id}-sre"
    subtask_code_id = f"{parent_id}-code"
    
    while True:
        # Fetch parent status
        parent_res = client.get_task_status(parent_id)
        parent_status = parent_res.get("status", "UNKNOWN")
        parent_progress = parent_res.get("progress", 0)
        parent_step = parent_res.get("step", "")
        
        # Fetch subtask 1 (SRE) status
        sre_res = client.get_task_status(subtask_sre_id)
        sre_status = sre_res.get("status", "QUEUED") if "error" not in sre_res or sre_res["error"] is None else "PENDING_SPAWN"
        sre_progress = sre_res.get("progress", 0) if "error" not in sre_res or sre_res["error"] is None else 0
        sre_step = sre_res.get("step", "Waiting for spawn") if "error" not in sre_res or sre_res["error"] is None else "Waiting for spawn"
        
        # Fetch subtask 2 (Code) status
        code_res = client.get_task_status(subtask_code_id)
        code_status = code_res.get("status", "QUEUED") if "error" not in code_res or code_res["error"] is None else "PENDING_SPAWN"
        code_progress = code_res.get("progress", 0) if "error" not in code_res or code_res["error"] is None else 0
        code_step = code_res.get("step", "Waiting for spawn") if "error" not in code_res or code_res["error"] is None else "Waiting for spawn"
        
        # Print status updates
        print(f"Parent: {parent_id:<27} | {parent_status:<12} | {parent_progress:>7}% | {parent_step}")
        print(f" -> Subagent SRE: {subtask_sre_id:<20} | {sre_status:<12} | {sre_progress:>7}% | {sre_step}")
        print(f" -> Subagent Code: {subtask_code_id:<19} | {code_status:<12} | {code_progress:>7}% | {code_step}")
        print("-" * 80)
        
        if parent_status in ("COMPLETED", "FAILED", "CANCELLED"):
            break
            
        time.sleep(3)
        
    # 3. Retrieve final aggregated report
    if parent_status == "COMPLETED":
        print("\nRetrieving Aggregated Multi-Agent Deployment Report:")
        result_res = client.get_task_result(parent_id)
        print("=" * 80)
        print(result_res.get("result"))
        print("=" * 80)

if __name__ == "__main__":
    main()
