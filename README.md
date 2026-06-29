# AgyQueue: Non-Blocking Asynchronous Task Execution for AI Agents

![Run on Google Cloud](https://deploy.cloud.run/button.svg)

AgyQueue is a lightweight, non-blocking asynchronous task execution framework built for AI agents. It allows agents to offload long-running operations—such as code generation, compilation, testing, and validation—to background workers while keeping client conversation threads responsive.

---

## 🚀 Core Capabilities

1. **Asynchronous Task Offloading**: Agents submit tasks via MCP tool calls or REST API requests and immediately receive a `task_id`, returning execution control back to the user chat session.
2. **Resilience & Reconnection**: Task status is saved in a persistent database store, allowing clients to query progress or fetch final outputs even after network drops or worker crashes.
3. **Interactive Console UI**: Built-in dark mode dashboard that visualizes enqueued runs, progress bars, vertical event history timelines (e.g. `WorkflowExecutionStarted`, `WorkflowTaskScheduled`), and aggregated markdown reports.
4. **Well-Architected Architecture**:
   * **Threaded Connection Pooling**: Uses `ThreadedConnectionPool` for PostgreSQL backend tasks, preventing socket exhaust under high concurrent query volumes.
   * **Graceful Shutdown**: Catches `SIGTERM` / `SIGINT` signals, allowing workers to finish their active task run cleanly before eviction.
   * **Workspace Isolation**: Spawns copy-on-write scratch worktree folders for subprocess executions, ensuring zero file-system cross-interference.
5. **Configurable Multi-Channel Notifications**: Real-time push notifications over Slack webhook and SMTP Email when tasks hit terminal states.
6. **Model Context Protocol (MCP) Support**: Serves as a standard stdio/sse MCP server compatible with Cursor, Claude Desktop, Claude Code, and Copilot CLI.

---

## 🛠️ Architecture

AgyQueue supports both hybrid cloud-agnostic execution flows and fully-managed Google Cloud configurations:

### 1. Cloud-Agnostic Core Flow
![AgyQueue Cloud-Agnostic Architecture](agyqueue_architecture.jpg)

### 2. Google Cloud Native Production Deployment
![AgyQueue GCP System Architecture](agyqueue_gcp_architecture.jpg)

### Google Cloud & Google AI Managed Services Mapping

When migrating from local development/fallback to production cloud scaling, AgyQueue integrates natively with Google Cloud managed services:

| Component | Dev Fallback | Production GCP Service |
| :--- | :--- | :--- |
| **Agent Reasoning Core** | Local LLM | **Vertex AI Gemini API** (Gemini 2.5 Flash / Pro) |
| **API Server Compute** | Local Python Process | **Google Cloud Run** (Event-driven serverless container) |
| **Worker Node Compute** | Local Daemon Process | **Google Kubernetes Engine (GKE)** or **Cloud Run Jobs** |
| **Task Message Broker** | SQLite (FIFO Table) | **Google Cloud Pub/Sub** or **Cloud Memorystore for Redis** |
| **Persistent Task Store** | SQLite Database | **Google Cloud SQL for PostgreSQL** or **Cloud Spanner** |
| **Workspace Repos** | Local Directory | **Google Cloud Storage (GCS)** Buckets |
| **Traffic Router** | Direct Port Binding | **Google Cloud Load Balancing** (HTTPS Layer 7 Router) |


---

## 📁 Examples & Supported Integrations

The [examples/](file:///Users/jitendragupta/Documents/github-repo/agyqueue/examples/) folder contains detailed, ready-to-run client scripts for every connection method:

* **Antigravity 2.0 SDK**: [examples/antigravity_agent_sdk.py](file:///Users/jitendragupta/Documents/github-repo/agyqueue/examples/antigravity_agent_sdk.py) shows how to bind tools to a `google.antigravity` agent.
* **Google ADK Agents**: [examples/google_adk_agent.py](file:///Users/jitendragupta/Documents/github-repo/agyqueue/examples/google_adk_agent.py) demonstrates tool binding in the `google.adk.agents` framework.
* **StdIO MCP Client**: [examples/mcp_stdio_client.py](file:///Users/jitendragupta/Documents/github-repo/agyqueue/examples/mcp_stdio_client.py) connects programmatically via stdin/stdout subprocesses.
* **SSE MCP Client**: [examples/mcp_sse_client.py](file:///Users/jitendragupta/Documents/github-repo/agyqueue/examples/mcp_sse_client.py) connects over network Server-Sent Events.
* **Direct REST SDK**: [examples/rest_client_sdk_demo.py](file:///Users/jitendragupta/Documents/github-repo/agyqueue/examples/rest_client_sdk_demo.py) performs calls using the lightweight `AgyQueueClient` Python client.

---

## 🏁 Quick Start (Local Run)

1. **Setup Environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   cp .env.example .env
   ```

2. **Run Standalone Dev Demo**:
   Spins up a local server, enqueues an SRE manifest compliance and FastAPI code generator workflow, executes it in isolated directories, and outputs the final report:
   ```bash
   python examples/demo.py
   ```

3. **Open the Web Console Dashboard**:
   Start the standalone SSE server and background worker:
   ```bash
   # Terminal 1: Start Server
   export AGYQUEUE_TRANSPORT=sse
   python -m agyqueue.mcp_server

   # Terminal 2: Start Worker
   python -m agyqueue.worker
   ```
   Navigate to [http://localhost:8000/dashboard](http://localhost:8000/dashboard) to submit runs, inspect progress, and view timeline event history logs.

---

## 🐳 Docker Compose Deployment (Redis + SSE Mode)

To run the complete production-mimicking system with Redis queues and PostgreSQL stores:

1. **Build and Run**:
   ```bash
   docker compose up --build
   ```

2. **Verify over SSE**:
   ```bash
   python examples/mcp_sse_client.py
   ```

---

## ☁️ Cloud Deployment (Cloud Run & GKE)

For Google Cloud production environments, configuration templates are located in [deployment/](file:///Users/jitendragupta/Documents/github-repo/agyqueue/deployment/):
* **Cloud Run**: Serverless container setups with Memorystore Redis and Cloud SQL PostgreSQL.
* **GKE**: Scalable Kubernetes microservices with Horizontal Pod Autoscalers (HPA).

For complete deployment details, see [INTEGRATION_GUIDE.md](file:///Users/jitendragupta/Documents/github-repo/agyqueue/INTEGRATION_GUIDE.md).

