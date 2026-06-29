# Integration Guide: Connecting AgyQueue to Agentic Automation Workflows

This guide explains how to integrate **AgyQueue** into professional agentic workflows, including **Google Agent Development Kit (ADK) / Agent Engine**, **Gemini Enterprise Multi-Agent Workflows**, and local IDEs (such as **Claude Desktop** and **Cursor**).

---

## 1. Overview: The Asynchronous Pattern in LLM Orchestration

In standard agent implementations, tool calls are executed synchronously: the LLM calls a tool and blocks, waiting for the tool to complete. For long-running tasks (e.g. running code tests, SRE audits, compiling, or executing multi-step generations), this causes connection timeouts, model stalling, and context-window saturation.

AgyQueue solves this by introducing a **non-blocking asynchronous queue pattern**:

```
┌─────────────┐       1. Submit Task (Tool Call)        ┌──────────────┐
│  AI Agent   ├────────────────────────────────────────►│  AgyQueue    │
│             │◄────────────────────────────────────────┤  MCP Server  │
└──────┬──────┘          2. Returns task_id             └──────────────┘
       │                                                       ▲
       │ 3. Yields / Polls (Status check)                      │
       ▼                                                       │ 4. Heartbeat &
┌─────────────┐                                                │    Workloads
│   Client /  │                                                ▼
│ Orchestrator│◄────────────────────────────────────────┌──────────────┐
│   Runtime   │           5. Retrieve Result            │  Background  │
└─────────────┘                                         │   Workers    │
                                                        └──────────────┘
```

---

## 2. Integrating with Google Agent Development Kit (ADK) & Agent Engine

The Google **ADK** (Agent Development Kit) allows Python-defined agents to be deployed to **Agent Engine** (Vertex AI Reasoning Engine). Since ADK extracts tool schemas directly from standard Python function signatures, you can import and register AgyQueue's functional tools directly.

### Step 1: Install the Package
First, install `agyqueue` into your agent environment:
```bash
pip install -e .
```

### Step 2: Define and Equip the Agent
Import the tools from the `agyqueue.client` module and attach them to your root agent:

```python
from google.adk.agents import Agent
from agyqueue.client import submit_async_task, check_task_progress, get_task_output, cancel_running_task

# Define the coordinator agent
orchestrator_agent = Agent(
    name="deployment_coordinator",
    model="gemini-2.5-flash",
    instruction=(
        "You coordinate SRE manifest compliance checks and API code generation workloads. "
        "Use submit_async_task to spawn background jobs. Do not wait for jobs in a loop. "
        "Return the task_id to the user/orchestrator so that progress can be tracked asynchronously."
    ),
    tools=[
        submit_async_task,
        check_task_progress,
        get_task_output,
        cancel_running_task
    ]
)
```

---

## 3. Registering with Gemini Enterprise & Agent Registry

To expose AgyQueue as a reusable tool across your enterprise fleet, deploy the MCP server as a microservice (e.g., on Cloud Run using our Terraform templates) and register it in the Google **Agent Registry**:

### Step 1: Deploy to Cloud Run
Build the container image and deploy the SSE server:
```bash
docker build -t gcr.io/YOUR_PROJECT_ID/agyqueue-server:latest --target server .
docker push gcr.io/YOUR_PROJECT_ID/agyqueue-server:latest
```
Using the Terraform configuration under `deployment/terraform/`, run `terraform apply` to deploy.

### Step 2: Register via Agent CLI
Expose the deployed URL to Gemini Enterprise using the `agents-cli` tool:
```bash
agents-cli publish gemini-enterprise \
  --name "agyqueue-service" \
  --description "Exposes asynchronous background task execution, cancellation, and progress tracking tools." \
  --url "https://agyqueue-server-dev-xxxx-uc.a.run.app/sse" \
  --type "mcp"
```

Any registered enterprise agent in your organization can now invoke AgyQueue to delegate heavy workloads asynchronously.

---

## 4. Local IDE Integration (Claude Desktop & Cursor)

For local development or testing with desktop co-workers, AgyQueue can be mounted directly as a local stdio MCP tool.

### Claude Desktop Configuration
Add the following to your `claude_desktop_config.json` (typically located in `~/Library/Application Support/Claude/` on macOS):

```json
{
  "mcpServers": {
    "agyqueue": {
      "command": "python",
      "args": [
        "-m",
        "agyqueue.mcp_server"
      ],
      "env": {
        "PYTHONPATH": "/absolute/path/to/your/agyqueue/repo",
        "AGYQUEUE_TRANSPORT": "stdio",
        "AGYQUEUE_DB_PATH": "/absolute/path/to/your/agyqueue/repo/agyqueue.db"
      }
    }
  }
}
```

### Cursor Configuration
1. Open Cursor Settings -> Features -> MCP.
2. Click **+ Add New MCP Server**.
3. Configure:
   * **Name**: `AgyQueue`
   * **Type**: `command`
   * **Command**: `PYTHONPATH=. .venv/bin/python -m agyqueue.mcp_server` (relative to your repo path).

### VS Code Configuration (Cline / Roo Code Extensions)
1. Open VS Code and navigate to standard MCP settings for Cline/Roo Code.
2. Edit the config file `cline_mcp_settings.json` (located in the global storage folder of the extension).
3. Append the AgyQueue stdio server configuration:
```json
{
  "mcpServers": {
    "agyqueue": {
      "command": "python",
      "args": [
        "-m",
        "agyqueue.mcp_server"
      ],
      "env": {
        "PYTHONPATH": "/absolute/path/to/your/agyqueue/repo",
        "AGYQUEUE_TRANSPORT": "stdio",
        "AGYQUEUE_DB_PATH": "/absolute/path/to/your/agyqueue/repo/agyqueue.db"
      }
    }
  }
}
```

### Claude Code CLI Integration
Claude Code CLI (terminal agent CLI tool) reads MCP server registrations from `~/.config/claude-code/mcp.json`. 
1. Create or edit `~/.config/claude-code/mcp.json`.
2. Add AgyQueue as a local tool under the `"mcpServers"` dictionary using the exact same stdio JSON block as shown above.

### GitHub Copilot CLI Integration
Copilot CLI can interface with AgyQueue either via raw HTTP REST commands (making curl requests directly to `http://localhost:8000/api/tasks`) or by installing the `agyqueue` command-line executable globally. Once installed as a python package (`pip install agyqueue`), developers can invoke it directly from any shell or scripting alias:
```bash
# Start local MCP StdIO server in shell
agyqueue --transport stdio
```

---

## 5. Multi-Agent Tree-Based Orchestration Pattern

AgyQueue natively supports **parent-child task aggregation**. When an orchestrator agent splits a large request into parallel child workloads, it submits the subtasks with a `parent_id` parameter:

1. **Task Submission**:
   * The orchestrator spawns subagents by submitting child tasks referencing the parent ID.
   * The parent task status transitions to `WAITING`.
2. **Worker Isolation**:
   * Background workers pick up and execute child tasks in isolated workspaces (`git_worktree` or copy-on-write temp folders) in parallel.
3. **Automatic Resumption**:
   * As soon as the final sibling completes, the worker recognizes that all siblings are done and automatically re-queues the parent task.
   * The orchestrator wakes up, reads the logs of all completed subtasks, aggregates the results, and marks the parent task as `COMPLETED`.

This prevents prompt-context saturation and allows agents to stay responsive and perform parallel processing.

---

## 6. Configurable Slack & Email Notifications

AgyQueue contains a pluggable `NotificationManager` that triggers alerts on terminal task states (`COMPLETED`, `FAILED`, `CANCELLED`). This is executed in a background executor thread on the MCP server, guaranteeing that network delays in SMTP or Slack APIs do not block the core task loops.

To configure one or both channels, set the following environment variables:

```bash
# Enable channels (comma-separated list)
export AGYQUEUE_NOTIFICATIONS="slack,email"

# 1. Slack Webhook Configuration
export SLACK_WEBHOOK_URL="https://example.com/slack-webhook-url"

# 2. Email SMTP Configuration
export SMTP_HOST="smtp.gmail.com"
export SMTP_PORT="587"
export SMTP_USER="your-email@gmail.com"
export SMTP_PASSWORD="your-app-password"
export SMTP_FROM="noreply@agyqueue.internal"
export SMTP_TO="recipient-alert-inbox@domain.com"
```

---

## 7. Publishing to PyPI (Public Access)

AgyQueue is fully packaged to be built and uploaded directly to PyPI. 

### Step 1: Install Build Tools
Make sure you have standard build and packaging tools installed in your virtual environment:
```bash
pip install --upgrade build twine
```

### Step 2: Build the Distribution Package
Run the following build command from the root of the workspace:
```bash
python -m build
```
This generates source distributions (`.tar.gz`) and compiled wheels (`.whl`) inside the `dist/` directory.

### Step 3: Upload to PyPI
Upload the compiled distribution artifacts to PyPI:
```bash
python -m twine upload dist/*
```
Once uploaded, anyone can install and run the AgyQueue CLI and python SDK client wrappers globally:
```bash
pip install agyqueue
```


