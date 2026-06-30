# 🎭 Multi-Agent Workflow with Human-Approval Signal Gate (Live Demo)

This document shows a recorded live execution walkthrough of a multi-agent orchestrated task within AgyQueue. It demonstrates **automatic workspace isolation (Git worktrees)**, **background worker activity logging**, and a **human-in-the-loop signal gate** where a workflow pauses until an external `approve` signal is consumed.

---

## 🛠️ The Scenario: DevOps SRE Compliance & Patching

1. **Submitting Workflow:** A user submits an SRE manifest compliance request to validate a Kubernetes `deployment.yaml`.
2. **Worker Claim & Isolation:** A background worker claims the task, checks out a clean isolated Git worktree, and runs a base linter.
3. **Validation Failure:** The linter fails because container limits are missing.
4. **Approval Gate (Paused):** The worker transitions the task status to `WAITING` and enters a pause loop, writing a timeline event and awaiting an `approve` signal.
5. **Human Approval:** An SRE manager reviews the linter logs and sends an `approve` signal via the `signal_workflow` endpoint.
6. **Resume & Patch:** The worker consumes the signal, resumes, applies the security patches, re-runs validation (passing successfully), cleans up the Git worktree, and marks the task as `COMPLETED`.

---

## 🖥️ Live Terminal Execution Output

```text
=========================================================
       AgyQueue-Advanced E2E Integration Test            
=========================================================
[Worker] Background worker thread started.

[Test] Querying active workers...
 -> Active workers: [
  {
    "worker_id": "worker-test-3e8e",
    "supported_types": "generic,manifest_compliance,fastapi_gen",
    "status": "ACTIVE",
    "last_heartbeat": "2026-06-30T11:50:03.946681"
  }
]

[Test] Registering a recurring schedule (every minute)...
 -> Schedule response: {
  "schedule_id": "sched-606b71ea",
  "cron_expression": "*/1 * * * *",
  "workflow_type": "generic",
  "message": "Schedule 'sched-606b71ea' registered successfully."
}

[Test] Submitting SRE workflow...
 -> Task submitted: agy-8f5bd30d

[Test] Monitoring workflow state for approval gate...
 -> Task agy-8f5bd30d: status=QUEUED, progress=0%, step='Queued in AgyQueue'

[Worker] Claimed task agy-8f5bd30d
 -> Task agy-8f5bd30d: status=RUNNING, progress=10%, step='Initializing isolated workspace...'
 -> Task agy-8f5bd30d: status=RUNNING, progress=25%, step='Writing Kubernetes manifest and SRE validation suite...'
 -> Task agy-8f5bd30d: status=RUNNING, progress=25%, step='Writing Kubernetes manifest and SRE validation suite...'
 -> Task agy-8f5bd30d: status=RUNNING, progress=50%, step='Running SRE linter on base manifests...'
 -> Task agy-8f5bd30d: status=RUNNING, progress=50%, step='Running SRE linter on base manifests...'
 -> Task agy-8f5bd30d: status=WAITING, progress=60%, step='Waiting for SRE manager approval signal...'

[Test] Workflow is WAITING for approval. Sending 'approve' signal...
 -> Signal response: {
  "signal_id": "sig-c85737bc",
  "task_id": "agy-8f5bd30d",
  "status": "SIGNALED",
  "message": "Signal 'approve' successfully sent to workflow agy-8f5bd30d."
}
 -> Task agy-8f5bd30d: status=RUNNING, progress=75%, step='Approval received. Applying security & reliability patches...'
 -> Task agy-8f5bd30d: status=RUNNING, progress=90%, step='Re-running SRE linter to validate fixes...'
 -> Task agy-8f5bd30d: status=RUNNING, progress=90%, step='Re-running SRE linter to validate fixes...'
 -> Task agy-8f5bd30d: status=RUNNING, progress=90%, step='Re-running SRE linter to validate fixes...'
 -> Task agy-8f5bd30d: status=COMPLETED, progress=100%, step='Analysis and validation complete.'

[Test] Task reached terminal state: COMPLETED

[Test] Fetching workflow event history...
===================== EVENT TIMELINE =====================
[2026-06-30T11:50:05.959937] WorkflowStarted      | Started on worker worker-test-3e8e
[2026-06-30T11:50:10.300066] LinterOutput         | SRE VALIDATION FAILED: Container resources limits (CPU and Memory) must be explicitly set.

[2026-06-30T11:50:13.910037] WorkflowPaused       | Awaiting 'approve' signal on task_signals
[2026-06-30T11:50:14.927286] WorkflowResumed      | Received approval: Approved by Lead SRE
[2026-06-30T11:50:21.051103] WorkflowCompleted    | Completed successfully
==========================================================
[Worker] Worker thread stopped.

[Test] Done.
```

---

## 📈 Real-Time Event Timeline Walkthrough

Looking closely at the generated **Event Timeline**, we can see the exact, millisecond-precise coordination:

1. **`11:50:05` (WorkflowStarted):** The worker claims the job and starts execution.
2. **`11:50:10` (LinterOutput):** The isolated linter process fails because container limits (CPU and Memory) were missing.
3. **`11:50:13` (WorkflowPaused):** The workflow successfully pauses, updates its progress to **`60%`**, and registers a `PENDING` state.
4. **`11:50:14` (WorkflowResumed):** The client program fires the `approve` signal. The worker immediately consumes the signal, transitions back to `RUNNING` with **`75%`** progress, and applies the security patches.
5. **`11:50:21` (WorkflowCompleted):** Re-validation passes. Workspace isolation is safely dismantled and cleaned, and task finalization reports are committed to storage.

---

## 🚀 Try It Yourself!

To execute this exact workflow locally and inspect the code tree, simply run:
```bash
uv run python examples/test_agyqueue_features.py
```
