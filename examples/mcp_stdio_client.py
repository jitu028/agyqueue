import asyncio
import os
import sys
import json

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:
    print("Error: The 'mcp' library is not installed in this environment.")
    print("Please install it by running: pip install mcp")
    sys.exit(1)

async def main():
    print("=========================================================")
    print("Launching AgyQueue MCP Server via StdIO Subprocess...")
    
    # Configure the server parameters to start AgyQueue over StdIO
    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "agyqueue.mcp_server"],
        env={
            "PYTHONPATH": os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "AGYQUEUE_TRANSPORT": "stdio",
            "AGYQUEUE_DB_PATH": "agyqueue.db"
        }
    )
    
    # Establish standard input/output bridge and initialize the MCP session
    async with stdio_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            print("Performing MCP Client Handshake...")
            await session.initialize()
            
            # 1. List available tools
            print("\nListing available tools:")
            tools_list = await session.list_tools()
            for tool in tools_list.tools:
                print(f" - {tool.name}: {tool.description}")
                
            # 2. Submit a background compliance task via MCP tool call
            print("\nSubmitting compliance task via 'submit_task' tool...")
            submit_result = await session.call_tool(
                name="submit_task",
                arguments={
                    "prompt": "Orchestrate compliance check on standard deployment manifests",
                    "task_type": "manifest_compliance"
                }
            )
            
            # The result content is returned as text/json
            response_data = json.loads(submit_result.content[0].text)
            task_id = response_data.get("task_id")
            print(f"Task successfully submitted! Generated Task ID: {task_id}")
            
            # 3. Poll status until complete
            print(f"\nMonitoring task {task_id} progress...")
            while True:
                status_result = await session.call_tool(
                    name="get_task_status",
                    arguments={"task_id": task_id}
                )
                status_data = json.loads(status_result.content[0].text)
                status = status_data.get("status")
                progress = status_data.get("progress")
                step = status_data.get("step")
                
                print(f" -> Status: {status:<10} | Progress: {progress:>3}% | Step: {step}")
                
                if status in ("COMPLETED", "FAILED", "CANCELLED"):
                    break
                await asyncio.sleep(1.5)
                
            # 4. Fetch final execution logs
            if status == "COMPLETED":
                print("\nFetching task output results:")
                result_output = await session.call_tool(
                    name="get_task_result",
                    arguments={"task_id": task_id}
                )
                result_data = json.loads(result_output.content[0].text)
                print("---------------------------------------------------------")
                print(result_data.get("result"))
                print("---------------------------------------------------------")

if __name__ == "__main__":
    # Start the event loop
    asyncio.run(main())
