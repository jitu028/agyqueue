import os
import sys
import time
import asyncio
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from agyqueue.client import AgyQueueClient
from pipeline_agent import get_coordinator_agent

# Ensure we route calls to the live Cloud Run AgyQueue Server
SERVER_URL = os.environ.get("AGYQUEUE_SERVER_URL", "http://localhost:8000")
os.environ["AGYQUEUE_SERVER_URL"] = SERVER_URL

async def run_success_flow(runner, user_prompt):
    print("\n" + "="*80)
    print("STAGE 1: RUNNING RELEASE PIPELINE SUCCESS WORKFLOW")
    print("="*80)
    
    print(f"[User]: {user_prompt}")
    print("Orchestrating Release Coordinator Agent...")
    
    # Run the coordinator agent
    task_id = None
    async for event in runner.run_async(
        user_id="devops_user",
        session_id="session_success",
        new_message=types.Content(role="user", parts=[types.Part.from_text(text=user_prompt)])
    ):
        if event.is_final_response():
            response_text = event.content.parts[0].text
            print(f"\n[Release Coordinator Agent]: {response_text}")
            
            # Extract the task ID from the agent's text response
            # Format usually contains: agy-xxxxxxxx
            for word in response_text.replace("`", " ").split():
                if word.startswith("agy-"):
                    task_id = word.strip(".,!?:;")
                    break

    if not task_id:
        print("[Error] Failed to retrieve task ID from agent response.")
        return
        
    print(f"\n[Monitor] Tracking Task {task_id} in AgyQueue...")
    client = AgyQueueClient(base_url=SERVER_URL)
    
    # Poll status until complete
    while True:
        status_res = client.get_task_status(task_id)
        status = status_res.get("status")
        progress = status_res.get("progress")
        step = status_res.get("step")
        
        print(f" -> Parent Task: {task_id} | Status: {status:<10} | Progress: {progress:>3}% | Step: {step}")
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        await asyncio.sleep(3)
        
    if status == "COMPLETED":
        print("\n[Report] Retrieving Final Release Report:")
        result_res = client.get_task_result(task_id)
        print("-" * 80)
        print(result_res.get("result"))
        print("-" * 80)

async def run_cancel_retry_flow(runner, user_prompt):
    print("\n" + "="*80)
    print("STAGE 2: TESTING AUTOMATED CANCELLATION AND RETRY WORKFLOW")
    print("="*80)
    
    print(f"[User]: {user_prompt}")
    
    # 1. Start the orchestration pipeline
    task_id = None
    async for event in runner.run_async(
        user_id="devops_user",
        session_id="session_cancel",
        new_message=types.Content(role="user", parts=[types.Part.from_text(text=user_prompt)])
    ):
        if event.is_final_response():
            response_text = event.content.parts[0].text
            print(f"\n[Release Coordinator Agent]: {response_text}")
            for word in response_text.replace("`", " ").split():
                if word.startswith("agy-"):
                    task_id = word.strip(".,!?:;")
                    break

    if not task_id:
        print("[Error] Failed to retrieve task ID.")
        return
        
    client = AgyQueueClient(base_url=SERVER_URL)
    
    # 2. Wait 4 seconds (simulating active running state)
    print(f"\n[Monitor] Allowing task {task_id} to initialize and run for 4 seconds...")
    await asyncio.sleep(4)
    
    # Fetch status before cancellation
    status_res = client.get_task_status(task_id)
    print(f" -> Current Status: {status_res.get('status')} | Progress: {status_res.get('progress')}%")
    
    # 3. Trigger cancellation command
    print(f"\n[Action] Triggering task cancellation for {task_id}...")
    cancel_res = client.cancel_task(task_id)
    print(f" -> Cancellation Response: {cancel_res.get('message', cancel_res.get('error'))}")
    
    # Verify status moves to CANCELLED
    print("\n[Monitor] Verifying task cancellation state...")
    for _ in range(5):
        status_res = client.get_task_status(task_id)
        status = status_res.get("status")
        print(f" -> Task Status: {status}")
        if status == "CANCELLED":
            print(f"[Success] Task {task_id} successfully cancelled in database and broker!")
            break
        await asyncio.sleep(1.5)
        
    # 4. Trigger Retry
    print("\n" + "-"*80)
    print(f"STAGE 3: RETRYING THE CANCELLED WORKFLOW")
    print("-"*80)
    
    print(f"Submitting retry request to AgyQueue...")
    retry_prompt = f"Retry deployment pipeline with SRE checks for task {task_id}"
    
    # Re-submit the task
    retry_submit = client.submit_task(prompt=retry_prompt, task_type="multi_agent_deploy")
    retry_task_id = retry_submit.get("task_id")
    print(f"Retry Task successfully enqueued! New Task ID: {retry_task_id}")
    
    # Poll retry to completion
    print(f"\n[Monitor] Tracking retry task {retry_task_id} to completion...")
    while True:
        status_res = client.get_task_status(retry_task_id)
        status = status_res.get("status")
        progress = status_res.get("progress")
        step = status_res.get("step")
        
        print(f" -> Retry Task: {retry_task_id} | Status: {status:<10} | Progress: {progress:>3}% | Step: {step}")
        if status in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        await asyncio.sleep(3)
        
    if status == "COMPLETED":
        print(f"\n[Success] Retry task {retry_task_id} completed successfully!")
        result_res = client.get_task_result(retry_task_id)
        print("-" * 80)
        print(result_res.get("result"))
        print("-" * 80)

async def main():
    print("=========================================================")
    print("    DevOps Pipeline Multi-Agent End-to-End Orchestrator  ")
    print("=========================================================")
    print(f"Targeting Cloud Run Server: {SERVER_URL}\n")
    
    # Initialize the ADK Runner with the Coordinator Agent
    agent = get_coordinator_agent()
    session_service = InMemorySessionService()
    await session_service.create_session(app_name="devops_app", user_id="devops_user", session_id="session_success")
    await session_service.create_session(app_name="devops_app", user_id="devops_user", session_id="session_cancel")
    runner = Runner(agent=agent, app_name="devops_app", session_service=session_service)
    
    # Flow A: Run normal release success flow
    user_prompt_success = "Validate compliance and run release pipeline check for project billing-v2"
    await run_success_flow(runner, user_prompt_success)
    
    # Flow B: Run cancellation and retry flow
    user_prompt_cancel = "Deploy release pipeline checks for transaction-validator-v1"
    await run_cancel_retry_flow(runner, user_prompt_cancel)

if __name__ == "__main__":
    asyncio.run(main())
