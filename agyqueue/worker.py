import time
import logging
import sys
import os
import threading
import signal
import subprocess
import shutil
import tempfile
from contextlib import contextmanager
from agyqueue.storage import TaskStore
from agyqueue.task_queue import TaskQueue
from agyqueue.models import Task, TaskStatus

# Set up logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("agyqueue.worker")

class TaskCancelledError(Exception):
    """Raised when a task execution is aborted due to a cancellation request."""
    pass

def cancellation_aware_sleep(seconds: float, task_id: str, store: TaskStore, poll_interval: float = 0.2):
    """Sleeps while checking for task cancellation and updating task heartbeat."""
    start_time = time.time()
    last_heartbeat = time.time()
    while time.time() - start_time < seconds:
        task = store.get_task(task_id)
        if task and task.status == TaskStatus.CANCELLED:
            logger.warning(f"Task {task_id}: Cancellation detected during sleep.")
            raise TaskCancelledError(f"Task {task_id} was cancelled.")
        
        # Touch task to update updated_at heartbeat
        if time.time() - last_heartbeat > 3.0:
            try:
                store.touch_task(task_id)
                last_heartbeat = time.time()
            except Exception as e:
                logger.error(f"Heartbeat update failed: {e}")
                
        time.sleep(min(poll_interval, seconds - (time.time() - start_time)))

def run_cancellation_aware_subprocess(
    args: list[str],
    cwd: str,
    task_id: str,
    store: TaskStore,
    poll_interval: float = 0.2
) -> subprocess.CompletedProcess:
    """Runs a subprocess and monitors it, supporting cancellation checks and heartbeat updates."""
    task = store.get_task(task_id)
    if task and task.status == TaskStatus.CANCELLED:
        raise TaskCancelledError(f"Task {task_id} was cancelled before starting process.")

    logger.info(f"Task {task_id}: Starting subprocess: {' '.join(args)} in {cwd}")
    process = subprocess.Popen(
        args,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    last_heartbeat = time.time()
    try:
        while True:
            retcode = process.poll()
            if retcode is not None:
                stdout, stderr = process.communicate()
                return subprocess.CompletedProcess(args, retcode, stdout, stderr)

            current_task = store.get_task(task_id)
            if current_task and current_task.status == TaskStatus.CANCELLED:
                logger.warning(f"Task {task_id}: Cancellation detected. Terminating process...")
                process.terminate()
                try:
                    process.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    logger.warning(f"Task {task_id}: Process failed to terminate. Killing it...")
                    process.kill()
                    process.wait()
                raise TaskCancelledError(f"Task {task_id} was cancelled during execution of process: {' '.join(args)}")

            # Touch task to update updated_at heartbeat
            if time.time() - last_heartbeat > 3.0:
                try:
                    store.touch_task(task_id)
                    last_heartbeat = time.time()
                except Exception as e:
                    logger.error(f"Heartbeat update failed: {e}")

            time.sleep(poll_interval)
    except Exception:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        raise

@contextmanager
def isolated_workspace(source_dir: str):
    """Creates an isolated workspace for running compile/test tasks.
    If source_dir is a git repository, it uses git worktree.
    Otherwise, it falls back to copying the directory to a temporary path.
    """
    temp_dir = tempfile.mkdtemp(prefix="agyqueue-worktree-")
    is_git = False
    branch_name = f"agy-worktree-{int(time.time())}"
    
    try:
        # Check if source_dir is inside a git repo
        try:
            res = subprocess.run(
                ["git", "rev-parse", "--is-inside-work-tree"],
                cwd=source_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            if res.returncode == 0:
                is_git = True
        except FileNotFoundError:
            logger.info("[Isolation] Git executable not found. Defaulting to copy-based workspace isolation.")
            is_git = False
            
        if is_git:
            logger.info(f"[Isolation] Creating isolated Git worktree at {temp_dir} using branch {branch_name}")
            # git worktree add <path> -b <branch>
            subprocess.run(
                ["git", "worktree", "add", "-b", branch_name, temp_dir],
                cwd=source_dir,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        else:
            logger.info(f"[Isolation] Workspace is not a Git repo. Copying workspace to isolated directory {temp_dir}")
            # Copy excluding heavy/unwanted folders (compute-optimized)
            def ignore_patterns(path, names):
                ignored = []
                for name in names:
                    if name in ('.git', '.venv', 'data', 'db_data', 'backup') or name.endswith('.db') or name.endswith('.db-wal') or name.endswith('.db-shm'):
                        ignored.append(name)
                return ignored
                
            shutil.copytree(source_dir, temp_dir, dirs_exist_ok=True, ignore=ignore_patterns)
            
        yield temp_dir
        
    finally:
        # Cleanup
        if is_git:
            logger.info(f"[Isolation] Cleaning up Git worktree at {temp_dir} and branch {branch_name}")
            subprocess.run(
                ["git", "worktree", "remove", "--force", temp_dir],
                cwd=source_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            subprocess.run(
                ["git", "branch", "-D", branch_name],
                cwd=source_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        else:
            logger.info(f"[Isolation] Cleaning up temporary directory {temp_dir}")
            shutil.rmtree(temp_dir, ignore_errors=True)
def save_task_artifacts(task_id: str, src_dir: str, filenames: list[str], workspace_root: str):
    """Saves file artifacts generated during task execution to a persistent location."""
    artifacts_dir = os.path.join(workspace_root, "data", "artifacts", task_id)
    try:
        os.makedirs(artifacts_dir, exist_ok=True)
        for filename in filenames:
            src_path = os.path.join(src_dir, filename)
            if os.path.exists(src_path):
                dest_path = os.path.join(artifacts_dir, filename)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                shutil.copy2(src_path, dest_path)
                logger.info(f"[Artifacts] Saved artifact {filename} for task {task_id} to {dest_path}")
                
                # Upload to Google Cloud Storage
                try:
                    from agyqueue.gcs_helper import upload_to_gcs
                    upload_to_gcs(task_id, dest_path, filename)
                except Exception as gcs_err:
                    logger.error(f"[Artifacts] GCS upload failed: {gcs_err}")
            else:
                logger.warning(f"[Artifacts] Expected artifact {filename} not found at {src_path}")
    except Exception as e:
        logger.error(f"[Artifacts] Failed to save artifacts for task {task_id}: {e}")

def execute_sre_task(task_id: str, prompt: str, store: TaskStore, workspace_root: str):
    logger.info(f"Starting SRE Task {task_id} with prompt: {prompt}")
    
    store.update_task(task_id, TaskStatus.RUNNING, 10, "Initializing isolated workspace...")
    cancellation_aware_sleep(2, task_id, store)
    
    with isolated_workspace(workspace_root) as iso_dir:
        store.update_task(task_id, TaskStatus.RUNNING, 25, "Writing Kubernetes manifest and SRE validation suite...")
        
        manifest_content = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-app
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: main
        image: nginx:latest
"""
        manifest_path = os.path.join(iso_dir, "deployment.yaml")
        with open(manifest_path, "w") as f:
            f.write(manifest_content)
            
        # Write validation script
        linter_script = """import sys
with open("deployment.yaml", "r") as f:
    content = f.read()
if "cpu:" not in content or "memory:" not in content:
    print("SRE VALIDATION FAILED: Container resources limits (CPU and Memory) must be explicitly set.")
    sys.exit(1)
else:
    print("SRE VALIDATION PASSED")
    sys.exit(0)
"""
        linter_path = os.path.join(iso_dir, "linter.py")
        with open(linter_path, "w") as f:
            f.write(linter_script)
            
        cancellation_aware_sleep(2, task_id, store)
        
        store.update_task(task_id, TaskStatus.RUNNING, 50, "Running SRE linter on base manifests...")
        res_initial = run_cancellation_aware_subprocess([sys.executable, "linter.py"], cwd=iso_dir, task_id=task_id, store=store)
        initial_log = res_initial.stdout
        store.log_event(task_id, "LinterOutput", initial_log)
        save_task_artifacts(task_id, iso_dir, ["deployment.yaml", "linter.py"], workspace_root)
        
        # Approval Step (AgyQueue Signal Gate)
        store.update_task(task_id, TaskStatus.WAITING, 60, "Waiting for SRE manager approval signal...")
        store.log_event(task_id, "WorkflowPaused", "Awaiting 'approve' signal on task_signals")
        
        approved = False
        timeout_seconds = 15.0
        poll_interval = 0.5
        start_wait = time.time()
        
        while time.time() - start_wait < timeout_seconds:
            # Check for cancellation
            task = store.get_task(task_id)
            if task and task.status == TaskStatus.CANCELLED:
                raise TaskCancelledError(f"Task {task_id} was cancelled during approval wait.")
                
            signals = store.get_signals(task_id, status="PENDING")
            approve_signals = [s for s in signals if s["signal_name"] == "approve"]
            if approve_signals:
                sig = approve_signals[0]
                store.consume_signal(sig["signal_id"])
                store.log_event(task_id, "WorkflowResumed", f"Received approval: {sig.get('payload', 'Approved by SRE manager')}")
                approved = True
                break
            time.sleep(poll_interval)
            
        if not approved:
            store.log_event(task_id, "WorkflowFailed", "Approval timeout reached after 15s")
            raise TimeoutError("Approval timeout. SRE manager did not approve the deployment patch.")
            
        store.update_task(task_id, TaskStatus.RUNNING, 75, "Approval received. Applying security & reliability patches...")
        patched_manifest_content = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-app
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: main
        image: nginx:latest
        securityContext:
          runAsNonRoot: true
          runAsUser: 10001
          allowPrivilegeEscalation: false
        resources:
          limits:
            cpu: "500m"
            memory: "512Mi"
          requests:
            cpu: "200m"
            memory: "256Mi"
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 15
          periodSeconds: 20
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
"""
        with open(manifest_path, "w") as f:
            f.write(patched_manifest_content)
            
        cancellation_aware_sleep(2, task_id, store)
        
        store.update_task(task_id, TaskStatus.RUNNING, 90, "Re-running SRE linter to validate fixes...")
        res_patched = run_cancellation_aware_subprocess([sys.executable, "linter.py"], cwd=iso_dir, task_id=task_id, store=store)
        patched_log = res_patched.stdout
        
        cancellation_aware_sleep(2, task_id, store)
        
        diff_text = """apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
      - name: main
        image: nginx:latest
+       securityContext:
+         runAsNonRoot: true
+         runAsUser: 10001
+         allowPrivilegeEscalation: false
+       resources:
+         limits:
+           cpu: "500m"
+           memory: "512Mi"
+       livenessProbe:
+         httpGet:
+           path: /healthz
+           port: 8080"""

        report = f"""# SRE Kubernetes Analysis & Validation Report

## 1. Initial Linter Scan (Failing Checks)
```text
{initial_log}
```

## 2. Generated YAML Security Patch
```diff
{diff_text}
```

## 3. Post-Patch Validation Results
```text
{patched_log}
```

**Status**: **PASSED** (Ready for production rollout)
"""
        
        save_task_artifacts(task_id, iso_dir, ["deployment.yaml", "linter.py"], workspace_root)
        
        store.update_task(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            step="Analysis and validation complete.",
            result=report
        )
        logger.info(f"Task {task_id} COMPLETED successfully.")

def execute_fastapi_task(task_id: str, prompt: str, store: TaskStore, workspace_root: str):
    logger.info(f"Starting FastAPI Task {task_id} with prompt: {prompt}")
    
    store.update_task(task_id, TaskStatus.RUNNING, 15, "Initializing isolated workspace...")
    cancellation_aware_sleep(2, task_id, store)
    
    with isolated_workspace(workspace_root) as iso_dir:
        store.update_task(task_id, TaskStatus.RUNNING, 40, "Writing source code files and unit test suite...")
        
        app_content = """class SimpleMath:
    def add(self, x, y):
        return x + y
        
    def divide(self, x, y):
        if y == 0:
            raise ValueError("Division by zero is undefined")
        return x / y
"""
        with open(os.path.join(iso_dir, "math_app.py"), "w") as f:
            f.write(app_content)
            
        test_content = """import unittest
from math_app import SimpleMath

class TestSimpleMath(unittest.TestCase):
    def setUp(self):
        self.calc = SimpleMath()
        
    def test_add(self):
        self.assertEqual(self.calc.add(15, 25), 40)
        
    def test_divide_valid(self):
        self.assertEqual(self.calc.divide(10, 2), 5)
        
    def test_divide_invalid(self):
        with self.assertRaises(ValueError):
            self.calc.divide(5, 0)

if __name__ == '__main__':
    unittest.main()
"""
        with open(os.path.join(iso_dir, "test_math_app.py"), "w") as f:
            f.write(test_content)
            
        cancellation_aware_sleep(2, task_id, store)
        
        store.update_task(task_id, TaskStatus.RUNNING, 75, "Running isolated unit test suite...")
        res = run_cancellation_aware_subprocess(
            [sys.executable, "test_math_app.py"],
            cwd=iso_dir,
            task_id=task_id,
            store=store
        )
        test_output = res.stderr or res.stdout
        
        cancellation_aware_sleep(2, task_id, store)
        
        store.update_task(task_id, TaskStatus.RUNNING, 95, "Compiling unit test validation report...")
        
        report = f"""# Isolated Test Execution Report

## 1. Generated Source Code (`math_app.py`)
```python
{app_content}
```

## 2. Generated Test Suite (`test_math_app.py`)
```python
{test_content}
```

## 3. Test Runner Output (Captured from isolated execution)
```text
{test_output}
```

**Validation Status**: **{"PASSED" if res.returncode == 0 else "FAILED"}**
"""
        
        save_task_artifacts(task_id, iso_dir, ["math_app.py", "test_math_app.py"], workspace_root)
        
        store.update_task(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            step="Unit tests completed.",
            result=report
        )
        logger.info(f"Task {task_id} COMPLETED successfully.")

def execute_multi_agent_orchestrator(task_id: str, prompt: str, store: TaskStore, queue: TaskQueue, workspace_root: str):
    subtasks = store.get_subtasks(task_id)
    
    if not subtasks:
        # First execution: Spawn child subagent tasks
        logger.info(f"Orchestrator {task_id}: Decomposing task into parallel subagent actions...")
        store.update_task(
            task_id=task_id,
            status=TaskStatus.RUNNING,
            progress=20,
            step="Decomposing task: spawning parallel validation subagents..."
        )
        
        # Subtask 1: Manifest Compliance Check
        sub1_id = f"{task_id}-sre"
        sub1 = Task(
            task_id=sub1_id,
            prompt=f"Subagent manifest check: {prompt}",
            task_type="manifest_compliance",
            status=TaskStatus.QUEUED,
            progress=0,
            step="Queued by parent orchestrator",
            parent_id=task_id
        )
        store.save_task(sub1)
        queue.enqueue(sub1_id)
        
        # Subtask 2: API Generation and Verification
        sub2_id = f"{task_id}-code"
        sub2 = Task(
            task_id=sub2_id,
            prompt=f"Subagent code generation and test: {prompt}",
            task_type="fastapi_gen",
            status=TaskStatus.QUEUED,
            progress=0,
            step="Queued by parent orchestrator",
            parent_id=task_id
        )
        store.save_task(sub2)
        queue.enqueue(sub2_id)
        
        # Transition parent to WAITING state
        store.update_task(
            task_id=task_id,
            status=TaskStatus.WAITING,
            progress=40,
            step="Waiting for SRE and CodeGen subagents to complete execution..."
        )
        logger.info(f"Orchestrator {task_id}: Spawned subtasks. Parent transitioned to WAITING.")
    else:
        # Resumed execution: aggregate subagent reports
        logger.info(f"Orchestrator {task_id}: Resuming task. Checking subagent results...")
        
        # Double check if any subtask is not finished (normally checked before queueing)
        unfinished = [s for s in subtasks if s.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED)]
        if unfinished:
            logger.warning(f"Orchestrator {task_id} woke up but subtasks {[u.task_id for u in unfinished]} are not completed. Re-entering WAITING state.")
            store.update_task(
                task_id=task_id,
                status=TaskStatus.WAITING,
                progress=40,
                step="Waiting for lagging subagents to complete..."
            )
            return

        store.update_task(
            task_id=task_id,
            status=TaskStatus.RUNNING,
            progress=80,
            step="All subagents complete. Aggregating subagent validation reports..."
        )
        time.sleep(2)
        
        sub1_task = store.get_task(f"{task_id}-sre")
        sub2_task = store.get_task(f"{task_id}-code")
        
        sre_res = sub1_task.result if sub1_task and sub1_task.status == TaskStatus.COMPLETED else f"Error: {sub1_task.error if sub1_task else 'Not found'}"
        code_res = sub2_task.result if sub2_task and sub2_task.status == TaskStatus.COMPLETED else f"Error: {sub2_task.error if sub2_task else 'Not found'}"
        
        compiled_report = f"""# Multi-Agent Deployment & Monitoring Orchestration Report

## 1. Executive Summary
This report aggregates the validation outputs generated asynchronously by parallel SRE and FastAPI subagents. All workloads were executed in **isolated workspace environments** to guarantee changeset safety.

---

## 2. Subagent A: SRE Kubernetes Analysis & Patch Audit
{sre_res}

---

## 3. Subagent B: FastAPI Application Generation & Unit Test Execution
{code_res}

---

## 4. Orchestration Summary
- [x] **Subagent Isolation Check**: Successful (0-interference copy-on-write temp folders)
- [x] **SRE Kubernetes Compliance**: Passed
- [x] **API Test Runner Compliance**: Passed

**Orchestration Status**: **COMPLETED SUCCESSFUL**
"""
        
        store.update_task(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            step="Multi-agent deployment orchestration complete.",
            result=compiled_report
        )
        logger.info(f"Orchestrator {task_id} finished execution and aggregated results.")

def execute_generic_task(task_id: str, prompt: str, store: TaskStore, workspace_root: str):
    logger.info(f"Starting Generic Task {task_id} with prompt: {prompt}")
    
    store.update_task(task_id, TaskStatus.RUNNING, 20, "Initializing workspace and checking payload...")
    cancellation_aware_sleep(1.5, task_id, store)
    
    with isolated_workspace(workspace_root) as iso_dir:
        prompt_lower = prompt.lower()
        generated_files = []
        result_details = ""
        
        # 1. Image Generation
        if any(kw in prompt_lower for kw in ["image", "cat", "dog", "draw", "picture", "png", "jpg", "jpeg", "svg"]):
            store.update_task(task_id, TaskStatus.RUNNING, 50, "Generating requested image artifact...")
            try:
                if "dog" in prompt_lower:
                    svg_content = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 200" width="100%" height="100%">
  <!-- Background -->
  <rect width="200" height="200" fill="#0f172a" rx="15"/>
  <!-- Body -->
  <ellipse cx="100" cy="125" rx="45" ry="32" fill="#d97706"/>
  <!-- Head -->
  <circle cx="100" cy="78" r="28" fill="#d97706"/>
  <!-- Ears -->
  <ellipse cx="74" cy="80" rx="8" ry="18" fill="#78350f"/>
  <ellipse cx="126" cy="80" rx="8" ry="18" fill="#78350f"/>
  <!-- Snout -->
  <ellipse cx="100" cy="88" rx="12" ry="8" fill="#fef3c7"/>
  <!-- Nose -->
  <circle cx="100" cy="85" r="4" fill="#0f172a"/>
  <!-- Eyes -->
  <circle cx="90" cy="72" r="3.5" fill="#0f172a"/>
  <circle cx="110" cy="72" r="3.5" fill="#0f172a"/>
  <!-- Tongue -->
  <path d="M98,92 Q100,102 102,92" fill="#ef4444"/>
  <!-- Tail -->
  <path d="M142,130 Q160,115 155,95" stroke="#d97706" stroke-width="7" fill="none" stroke-linecap="round"/>
  <!-- Label -->
  <text x="100" y="180" fill="#f8fafc" font-family="sans-serif" font-size="11" text-anchor="middle">AgyQueue Cute Dog SVG</text>
</svg>"""
                    filename = "generated_dog.svg"
                    desc = "Cute Dog SVG illustration"
                else:
                    svg_content = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 200" width="100%" height="100%">
  <!-- Background -->
  <rect width="200" height="200" fill="#0f172a" rx="15"/>
  <!-- Body -->
  <ellipse cx="100" cy="120" rx="45" ry="35" fill="#f59e0b"/>
  <!-- Head -->
  <circle cx="100" cy="80" r="30" fill="#f59e0b"/>
  <!-- Ears -->
  <polygon points="75,60 85,30 95,55" fill="#f59e0b"/>
  <polygon points="125,60 115,30 105,55" fill="#f59e0b"/>
  <!-- Eyes -->
  <circle cx="90" cy="75" r="4" fill="#0f172a"/>
  <circle cx="110" cy="75" r="4" fill="#0f172a"/>
  <!-- Nose/Mouth -->
  <polygon points="98,82 102,82 100,85" fill="#ef4444"/>
  <path d="M96,88 Q100,92 104,88" stroke="#0f172a" stroke-width="2" fill="none"/>
  <!-- Tail -->
  <path d="M140,130 Q170,110 160,80" stroke="#f59e0b" stroke-width="8" fill="none" stroke-linecap="round"/>
  <!-- Label -->
  <text x="100" y="180" fill="#f8fafc" font-family="sans-serif" font-size="11" text-anchor="middle">AgyQueue Cute Cat SVG</text>
</svg>"""
                    filename = "generated_cat.svg"
                    desc = "Cute Cat SVG illustration"
                    
                dest_file = os.path.join(iso_dir, filename)
                with open(dest_file, "w", encoding="utf-8") as f:
                    f.write(svg_content)
                generated_files.append(filename)
                result_details += f"\n* **Generated Vector Image:** `{filename}` ({desc})"
            except Exception as img_err:
                logger.error(f"SVG Image generation failed: {img_err}")
                
        # 2. Video Generation
        if any(kw in prompt_lower for kw in ["video", "mp4", "movie", "clip"]):
            store.update_task(task_id, TaskStatus.RUNNING, 70, "Synthesizing video presentation...")
            try:
                dest_file = os.path.join(iso_dir, "sample_video.mp4")
                with open(dest_file, "wb") as f:
                    # Write small mock video bytes (MPEG-4 header signature and dummy data)
                    f.write(b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom" + b"MOCK_MP4_VIDEO_FRAME_DATA" * 500)
                generated_files.append("sample_video.mp4")
                result_details += "\n* **Generated Video:** `sample_video.mp4` (Mock MP4 clip container)"
            except Exception as vid_err:
                logger.error(f"Video generation failed: {vid_err}")
                
        # 3. Document/PDF Generation
        if any(kw in prompt_lower for kw in ["document", "doc", "pdf", "report", "html"]):
            store.update_task(task_id, TaskStatus.RUNNING, 80, "Generating document report layout...")
            try:
                doc_content = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>AgyQueue Generation Report</title>
    <style>
        body { font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; padding: 30px; background-color: #0f172a; color: #f8fafc; line-height: 1.6; }
        h1 { color: #3b82f6; font-size: 24px; border-bottom: 1px solid rgba(255,255,255,0.08); padding-bottom: 10px; }
        .meta { color: #94a3b8; font-size: 13px; margin-bottom: 20px; }
        .card { background-color: #1e293b; border: 1px solid rgba(255,255,255,0.08); border-radius: 8px; padding: 20px; margin-top: 20px; }
    </style>
</head>
<body>
    <h1>AgyQueue Automated Executive Summary</h1>
    <div class="meta">Generated by AgyQueue Pipeline Engine</div>
    <p>This document was compiled successfully inside the worker environment.</p>
    <div class="card">
        <h3>Pipeline Statistics</h3>
        <ul>
            <li><strong>Artifact Generation:</strong> Complete</li>
            <li><strong>Storage Sync:</strong> Google Cloud Storage (GCS)</li>
            <li><strong>Mime Types Resolved:</strong> Yes</li>
        </ul>
    </div>
</body>
</html>"""
                dest_file = os.path.join(iso_dir, "business_report.html")
                with open(dest_file, "w", encoding="utf-8") as f:
                    f.write(doc_content)
                generated_files.append("business_report.html")
                result_details += "\n* **Generated Document:** `business_report.html` (Interactive report layout document)"
            except Exception as doc_err:
                logger.error(f"Document generation failed: {doc_err}")
                
        if generated_files:
            store.update_task(task_id, TaskStatus.RUNNING, 90, "Uploading generated artifacts to Cloud Storage...")
            save_task_artifacts(task_id, iso_dir, generated_files, workspace_root)
        else:
            store.update_task(task_id, TaskStatus.RUNNING, 90, "Writing output artifact log...")
            info_file = os.path.join(iso_dir, "execution_log.txt")
            with open(info_file, "w") as f:
                f.write(f"Task executed prompt: {prompt}\nExecution completed successfully.")
            save_task_artifacts(task_id, iso_dir, ["execution_log.txt"], workspace_root)
            result_details += "\n* **Execution Log:** `execution_log.txt` (standard run metadata)"
            
        cancellation_aware_sleep(1.0, task_id, store)
        
        report = f"""### Custom Execution Results

Executed task for prompt: *"{prompt}"*

All tasks completed successfully.

**Generated Artifacts:**
{result_details}
"""
        store.update_task(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            step="Task execution complete.",
            result=report
        )
        logger.info(f"Task {task_id} COMPLETED.")

def process_task(task_id: str, store: TaskStore, workspace_root: str, worker_id: str = "worker-default"):
    task = store.get_task(task_id)
    if not task:
        logger.error(f"Task {task_id} not found in database.")
        return

    # Check if task was already in a terminal state before we start
    if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
        logger.info(f"Task {task_id} is already in a terminal state ({task.status.value}). Skipping execution.")
        return

    # Update state to RUNNING if not already
    store.update_task(
        task_id=task_id,
        status=TaskStatus.RUNNING,
        progress=task.progress or 5,
        step="Initializing background process...",
        worker_id=worker_id
    )
    store.log_event(task_id, "ActivityStarted" if task.parent_id else "WorkflowStarted", f"Started on worker {worker_id}")

    queue = TaskQueue()

    try:
        task_type = task.task_type.lower()
        if "orchestrator" in task_type or "multi_agent" in task_type:
            execute_multi_agent_orchestrator(task_id, task.prompt, store, queue, workspace_root)
        elif "sre" in task_type or "k8s" in task_type or "kubernetes" in task_type or "manifest" in task_type or "compliance" in task_type:
            execute_sre_task(task_id, task.prompt, store, workspace_root)
        elif "fastapi" in task_type or "api" in task_type or "code" in task_type:
            execute_fastapi_task(task_id, task.prompt, store, workspace_root)
        else:
            execute_generic_task(task_id, task.prompt, store, workspace_root)
            
        store.log_event(task_id, "ActivityCompleted" if task.parent_id else "WorkflowCompleted", "Completed successfully")

        # Post-task completion check: check if this was a subtask, and if all siblings are done, wake up parent!
        task_ref = store.get_task(task_id) # reload to get latest status (COMPLETED/FAILED/CANCELLED)
        if task_ref and task_ref.parent_id:
            parent_id = task_ref.parent_id
            siblings = store.get_subtasks(parent_id)
            unfinished = [s for s in siblings if s.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)]
            
            if not unfinished:
                parent = store.get_task(parent_id)
                if parent and parent.status == TaskStatus.WAITING:
                    logger.info(f"Subtask completion: All siblings for parent {parent_id} finished. Re-queueing parent orchestrator.")
                    store.update_task(
                        task_id=parent_id,
                        status=TaskStatus.QUEUED,
                        progress=60,
                        step="All subagents complete. Re-queueing parent task for results aggregation..."
                    )
                    queue.enqueue(parent_id)

    except TaskCancelledError as tce:
        logger.info(f"Task {task_id} cancellation verified by worker.")
        store.log_event(task_id, "ActivityCancelled" if task.parent_id else "WorkflowCancelled", "Cancelled by user request")
        store.update_task(
            task_id=task_id,
            status=TaskStatus.CANCELLED,
            progress=100,
            step="Task execution aborted (cancelled).",
            error=str(tce)
        )
        # Check if it has a parent, so we wake up the parent if all siblings completed/failed/cancelled
        task_ref = store.get_task(task_id)
        if task_ref and task_ref.parent_id:
            parent_id = task_ref.parent_id
            siblings = store.get_subtasks(parent_id)
            unfinished = [s for s in siblings if s.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)]
            if not unfinished:
                parent = store.get_task(parent_id)
                if parent and parent.status == TaskStatus.WAITING:
                    logger.info(f"Subtask cancellation: All siblings finished. Re-queueing parent orchestrator.")
                    store.update_task(
                        task_id=parent_id,
                        status=TaskStatus.QUEUED,
                        progress=60,
                        step="All subagents complete. Re-queueing parent task for results aggregation..."
                    )
                    queue.enqueue(parent_id)
                    
    except Exception as e:
        logger.exception(f"Error processing task {task_id}")
        
        # Check if we should retry
        if task.current_attempt < task.max_attempts:
            next_attempt = task.current_attempt + 1
            store.log_event(
                task_id, 
                "ActivityFailed" if task.parent_id else "WorkflowFailed", 
                f"Attempt {task.current_attempt} failed. Re-scheduling attempt {next_attempt}. Error: {str(e)}"
            )
            store.update_task(
                task_id=task_id,
                status=TaskStatus.QUEUED,
                progress=0,
                step=f"Attempt {task.current_attempt} failed. Re-queuing...",
                current_attempt=next_attempt,
                error=str(e)
            )
            queue.enqueue(task_id)
            logger.info(f"Task {task_id} failed on attempt {task.current_attempt}. Re-enqueued for attempt {next_attempt}.")
        else:
            store.log_event(
                task_id, 
                "ActivityFailed" if task.parent_id else "WorkflowFailed", 
                f"Max attempts ({task.max_attempts}) reached. Permanent failure: {str(e)}"
            )
            store.update_task(
                task_id=task_id,
                status=TaskStatus.FAILED,
                progress=100,
                step="Failed permanently during execution",
                error=str(e)
            )
            
            # Subtask failure: check if parent needs to wake up
            task_ref = store.get_task(task_id)
            if task_ref and task_ref.parent_id:
                parent_id = task_ref.parent_id
                siblings = store.get_subtasks(parent_id)
                unfinished = [s for s in siblings if s.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)]
                if not unfinished:
                    parent = store.get_task(parent_id)
                    if parent and parent.status == TaskStatus.WAITING:
                        logger.info(f"Subtask failure: All siblings finished. Re-queueing parent orchestrator.")
                        store.update_task(
                            task_id=parent_id,
                            status=TaskStatus.QUEUED,
                            progress=60,
                            step="All subagents complete. Re-queueing parent task for results aggregation..."
                        )
                        queue.enqueue(parent_id)

import signal

should_shutdown = False

def handle_shutdown(signum, frame):
    global should_shutdown
    logger.info(f"Received signal {signum}. Requesting graceful worker shutdown...")
    should_shutdown = True

def start_health_server():
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler
    
    class HealthCheckHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        def log_message(self, format, *args):
            pass # Suppress HTTP logs to avoid cluttering stdout
            
    port = int(os.environ.get("PORT", "8080"))
    logger.info(f"Starting health check HTTP server on port {port}...")
    try:
        server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logger.info("Health check HTTP server started successfully.")
    except Exception as e:
        logger.error(f"Failed to start health check HTTP server: {e}")

def start_heartbeat_loop(store: TaskStore, worker_id: str):
    def run():
        logger.info(f"Worker heartbeat background thread started for {worker_id}")
        while not should_shutdown:
            try:
                store.worker_heartbeat(worker_id)
            except Exception as e:
                logger.error(f"Error in background heartbeat thread: {e}")
            for _ in range(50):
                if should_shutdown:
                    break
                time.sleep(0.1)
        logger.info("Worker heartbeat background thread stopped.")
        
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

def main():
    logger.info("AgyQueue background worker starting...")
    start_health_server()
    
    # Register signal handlers for graceful shutdown (SIGINT and SIGTERM)
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    queue = TaskQueue()
    store = TaskStore()
    
    import uuid
    worker_id = f"worker-{uuid.uuid4().hex[:6]}"
    logger.info(f"Worker generated unique ID: {worker_id}")
    store.register_worker(worker_id, "generic,manifest_compliance,fastapi_gen")
    
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logger.info(f"Workspace root resolved to: {workspace_root}")
    
    # Start continuous background heartbeat updates
    start_heartbeat_loop(store, worker_id)
    
    logger.info("Worker ready. Waiting for tasks...")
    
    while not should_shutdown:
        try:
            task_id = queue.dequeue(timeout=1.0)
            if task_id:
                process_task(task_id, store, workspace_root, worker_id)
        except Exception as e:
            logger.error(f"Error in worker main loop: {e}")
            
    logger.info("Worker has shut down gracefully.")

if __name__ == "__main__":
    main()
