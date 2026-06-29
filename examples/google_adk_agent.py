import asyncio
import json
import sys
from agyqueue.client import submit_async_task, check_task_progress, get_task_output, cancel_running_task

try:
    from google.adk.agents import Agent
except ImportError:
    print("Error: The 'google-adk' library is not installed in this environment.")
    print("Please install it before running this script.")
    sys.exit(1)

def run_adk_orchestration_flow():
    print("=========================================================")
    print("Initializing Google ADK Agent with AgyQueue tool bindings...")
    
    # 1. Define the ADK Agent
    agent = Agent(
        name="adk_compliance_coordinator",
        model="gemini-2.5-flash",
        instruction=(
            "You are a compliance agent that validates configuration manifests. "
            "Always submit manifest checks as background tasks using submit_async_task. "
            "Return the task ID immediately to the user. Do not block the chat thread."
        ),
        tools=[
            submit_async_task,
            check_task_progress,
            get_task_output,
            cancel_running_task
        ]
    )
    
    # 2. Simulate User Request
    user_prompt = "Validate my Kubernetes deployment.yaml configuration for security compliance."
    print(f"\n[User]: {user_prompt}")
    
    # 3. Get response from ADK Agent
    response = agent.generate_response(user_prompt)
    
    print("\n[ADK Agent Response]:")
    print(response.text)
    
if __name__ == "__main__":
    run_adk_orchestration_flow()
