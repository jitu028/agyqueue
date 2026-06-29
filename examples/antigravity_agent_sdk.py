import asyncio
import sys
import json
from agyqueue.client import submit_async_task, check_task_progress, get_task_output, cancel_running_task

try:
    from google.antigravity import Agent, LocalAgentConfig, CapabilitiesConfig
except ImportError:
    print("Error: The 'google-antigravity' SDK is not installed in this environment.")
    print("Please install it by running: pip install google-antigravity")
    sys.exit(1)

async def main():
    print("=========================================================")
    # Configure the agent. We pass CapabilitiesConfig to allow tool executions
    config = LocalAgentConfig(
        system_instructions=(
            "You are an asynchronous pipeline coordinator. You offload long-running "
            "tasks to AgyQueue using 'submit_async_task'. "
            "When you submit a task, return the task_id to the user and explain "
            "that they can check progress asynchronously. Do not wait in a blocking loop "
            "unless the user explicitly requests you to wait."
        ),
        capabilities=CapabilitiesConfig(),
    )

    # Bind the AgyQueue Python SDK tools to the Antigravity agent
    tools_list = [
        submit_async_task,
        check_task_progress,
        get_task_output,
        cancel_running_task
    ]

    print("Initializing Google Antigravity Agent with AgyQueue tools...")
    
    # Spawn the agent using the async context manager from the SDK
    async with Agent(config) as agent:
        # Register the tools with the agent runtime
        for tool in tools_list:
            agent.register_tool(tool)
            
        print("Agent ready. Submitting task prompt...")
        
        # Simulate chat interaction: the user asks the agent to run a compliance check
        prompt = (
            "Please check the compliance of my deployment manifest file and let me "
            "know the results. Run it as a background task."
        )
        print(f"\n[User]: {prompt}")
        
        response = await agent.chat(prompt)
        
        print("\n[Agent Thinking & Executing]:")
        # Stream thoughts and tool calls in real time from the SDK response
        async for thought in response.thoughts:
            print(f" -> {thought}")
            
        async for call in response.tool_calls:
            print(f"\n[Executing SDK Tool Call]: {call.name} with args: {call.args}")
            
        print("\n[Agent Response]:")
        async for token in response:
            sys.stdout.write(token)
            sys.stdout.flush()
        print("\n")

if __name__ == "__main__":
    asyncio.run(main())
