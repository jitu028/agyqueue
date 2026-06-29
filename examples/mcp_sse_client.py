import asyncio
import json
import sys

try:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
except ImportError:
    print("Error: The 'mcp' library is not installed in this environment.")
    print("Please install it by running: pip install mcp")
    sys.exit(1)

async def main():
    print("=========================================================")
    print("Connecting to AgyQueue SSE Endpoint...")
    print("Note: Ensure the server is running locally (e.g. at http://localhost:8000)")
    
    # Establish connection with the Server-Sent Events transport stream
    try:
        async with sse_client("http://127.0.0.1:8000/sse") as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                print("Performing MCP Handshake over SSE...")
                await session.initialize()
                
                # 1. List tools
                print("\nListing available tools:")
                tools_list = await session.list_tools()
                for tool in tools_list.tools:
                    print(f" - {tool.name}")
                    
                # 2. Submit task
                print("\nSubmitting FastAPI task...")
                submit_res = await session.call_tool(
                    name="submit_task",
                    arguments={
                        "prompt": "Create a simple calculator API and execute unit tests",
                        "task_type": "fastapi_gen"
                    }
                )
                
                response_data = json.loads(submit_res.content[0].text)
                task_id = response_data.get("task_id")
                print(f"Task submitted! ID: {task_id}")
                
                # 3. Poll status
                print(f"\nMonitoring progress for task {task_id}...")
                while True:
                    status_res = await session.call_tool(
                        name="get_task_status",
                        arguments={"task_id": task_id}
                    )
                    status_data = json.loads(status_res.content[0].text)
                    status = status_data.get("status")
                    progress = status_data.get("progress")
                    step = status_data.get("step")
                    
                    print(f" -> Status: {status:<10} | Progress: {progress:>3}% | Step: {step}")
                    
                    if status in ("COMPLETED", "FAILED", "CANCELLED"):
                        break
                    await asyncio.sleep(2.0)
                    
    except Exception as e:
        print(f"\nConnection Error: {e}")
        print("Please start the AgyQueue server first:")
        print("  export AGYQUEUE_TRANSPORT=sse")
        print("  python -m agyqueue.mcp_server")

if __name__ == "__main__":
    asyncio.run(main())
