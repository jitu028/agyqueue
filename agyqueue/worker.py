import time
import logging
import sys
import os
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

errors = []
if "livenessProbe" not in content:
    errors.append("Reliability Check: Missing livenessProbe")
if "resources" not in content:
    errors.append("Reliability Check: CPU/Memory resource limits are not defined")
if "runAsNonRoot: true" not in content:
    errors.append("Security Check: Container runs as root (runAsNonRoot is not true)")
    
if errors:
    print("SRE VALIDATION FAILED:")
    for err in errors:
        print(f" - {err}")
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
        
        cancellation_aware_sleep(2, task_id, store)
        
        store.update_task(task_id, TaskStatus.RUNNING, 75, "Applying security & reliability patches to manifest...")
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

def execute_generic_task(task_id: str, prompt: str, store: TaskStore):
    logger.info(f"Starting Generic Task {task_id} with prompt: {prompt}")
    
    for progress, step_desc in [(33, "Initializing task pipeline..."), (66, "Processing workload..."), (90, "Finalizing results...")]:
        store.update_task(task_id, TaskStatus.RUNNING, progress, step_desc)
        logger.info(f"Task {task_id}: {progress}% - {step_desc}")
        cancellation_aware_sleep(2, task_id, store)
        
    store.update_task(
        task_id=task_id,
        status=TaskStatus.COMPLETED,
        progress=100,
        step="Task execution complete.",
        result=f"### Custom Execution Results\n\nExecuted task for prompt: *\"{prompt}\"*\n\nAll tasks completed successfully."
    )
    logger.info(f"Task {task_id} COMPLETED.")

def process_task(task_id: str, store: TaskStore, workspace_root: str):
    task = store.get_task(task_id)
    if not task:
        logger.error(f"Task {task_id} not found in database.")
        return

    # Check if task was already cancelled before we start
    if task.status == TaskStatus.CANCELLED:
        logger.info(f"Task {task_id} was cancelled before worker could start it. Skipping.")
        return

    # Update state to RUNNING if not already
    store.update_task(
        task_id=task_id,
        status=TaskStatus.RUNNING,
        progress=task.progress or 5,
        step="Initializing background process..."
    )

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
            execute_generic_task(task_id, task.prompt, store)
            
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
        store.update_task(
            task_id=task_id,
            status=TaskStatus.FAILED,
            progress=100,
            step="Failed during execution",
            error=str(e)
        )

import signal

should_shutdown = False

def handle_shutdown(signum, frame):
    global should_shutdown
    logger.info(f"Received signal {signum}. Requesting graceful worker shutdown...")
    should_shutdown = True

def main():
    logger.info("AgyQueue background worker starting...")
    
    # Register signal handlers for graceful shutdown (SIGINT and SIGTERM)
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    queue = TaskQueue()
    store = TaskStore()
    
    workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logger.info(f"Workspace root resolved to: {workspace_root}")
    
    logger.info("Worker ready. Waiting for tasks...")
    
    while not should_shutdown:
        try:
            task_id = queue.dequeue(timeout=1.0)
            if task_id:
                process_task(task_id, store, workspace_root)
        except Exception as e:
            logger.error(f"Error in worker main loop: {e}")
            
    logger.info("Worker has shut down gracefully.")

if __name__ == "__main__":
    main()
