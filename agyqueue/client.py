import os
import urllib.request
import urllib.error
import urllib.parse
import json
import time
from typing import Optional, Any, List, Dict

class AgyQueueClient:
    """Client SDK for interacting with the AgyQueue microservice REST API."""
    
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or os.environ.get("AGYQUEUE_SERVER_URL", "http://127.0.0.1:8000")).rstrip("/")

    def _request(self, path: str, method: str = "GET", data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        req_data = None
        headers = {"Content-Type": "application/json"}
        
        if data is not None:
            req_data = json.dumps(data).encode("utf-8")
            
        req = urllib.request.Request(url, data=req_data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8")
                return json.loads(err_body)
            except Exception:
                return {"error": f"HTTP Error {e.code}: {e.reason}"}
        except Exception as e:
            return {"error": f"Connection failed: {str(e)}"}

    def submit_task(self, prompt: str, task_type: str = "generic") -> Dict[str, Any]:
        """Submit a new task to the AgyQueue service."""
        return self._request("/api/tasks", method="POST", data={"prompt": prompt, "task_type": task_type})

    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Fetch the execution status and progress of a task."""
        return self._request(f"/api/tasks/{task_id}", method="GET")

    def get_task_result(self, task_id: str) -> Dict[str, Any]:
        """Retrieve the result or error of a completed task."""
        return self._request(f"/api/tasks/{task_id}/result", method="GET")

    def list_tasks(self) -> List[Dict[str, Any]]:
        """List all tasks in the queue."""
        res = self._request("/api/tasks", method="GET")
        if isinstance(res, list):
            return res
        return []

    def cancel_task(self, task_id: str) -> Dict[str, Any]:
        """Cancel a running or queued task."""
        return self._request(f"/api/tasks/{task_id}/cancel", method="POST")

    def wait_for_task(self, task_id: str, poll_interval: float = 2.0, timeout: float = 300.0) -> Dict[str, Any]:
        """Wait for a task to reach a terminal state (COMPLETED, FAILED, CANCELLED)."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            status_res = self.get_task_status(task_id)
            if "error" in status_res:
                return status_res
                
            status = status_res.get("status")
            if status in ("COMPLETED", "FAILED", "CANCELLED"):
                return status_res
                
            time.sleep(poll_interval)
            
        return {"error": "Timeout waiting for task execution completion"}


# AI Agent Tool Wrappers
# These helper functions match the standard function signature/docstring pattern expected by AI Agent frameworks (like Google ADK, LangChain, etc.).

def get_agyqueue_client() -> AgyQueueClient:
    return AgyQueueClient()

def submit_async_task(prompt: str, task_type: str = "generic") -> str:
    """Submit a long-running asynchronous task to the background queue.
    
    Args:
        prompt: Detailed instructions or code validation prompt.
        task_type: Type of task executor (e.g. 'sre_k8s_analysis', 'fastapi_gen', 'generic').
        
    Returns:
        A JSON string containing the submitted task's ID and initial status.
    """
    client = get_agyqueue_client()
    return json.dumps(client.submit_task(prompt, task_type))

def check_task_progress(task_id: str) -> str:
    """Check the current status, progress percentage, and step of an active task.
    
    Args:
        task_id: The unique task ID returned when the task was submitted.
        
    Returns:
        A JSON string detailing current state, progress percentage, and active step.
    """
    client = get_agyqueue_client()
    return json.dumps(client.get_task_status(task_id))

def get_task_output(task_id: str) -> str:
    """Retrieve the final completed markdown report or error stack for a task.
    
    Args:
        task_id: The unique task ID returned when the task was submitted.
        
    Returns:
        A JSON string containing either the completed markdown result or the error payload.
    """
    client = get_agyqueue_client()
    return json.dumps(client.get_task_result(task_id))

def cancel_running_task(task_id: str) -> str:
    """Request immediate cancellation and cleanup of a running background task.
    
    Args:
        task_id: The unique task ID to cancel.
        
    Returns:
        A JSON string confirming cancellation status.
    """
    client = get_agyqueue_client()
    return json.dumps(client.cancel_task(task_id))
