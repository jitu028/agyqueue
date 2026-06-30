import os
import sys
from google.adk.agents import Agent
from agyqueue.client import submit_async_task, check_task_progress, get_task_output, cancel_running_task

def get_infra_linter_agent():
    """Infra linter subagent that audits configurations via AgyQueue."""
    return Agent(
        name="infra_linter",
        model="gemini-2.5-flash",
        instruction=(
            "You are a specialized infrastructure linter. "
            "Always delegate Kubernetes configuration validation to AgyQueue "
            "using `submit_async_task` with task_type='manifest_compliance'. "
            "You must return the task ID immediately."
        ),
        tools=[submit_async_task, check_task_progress, get_task_output]
    )

def get_security_auditor_agent():
    """Security auditor subagent that checks container credentials and policies."""
    return Agent(
        name="security_auditor",
        model="gemini-2.5-flash",
        instruction=(
            "You are a container security auditor. "
            "Always delegate container vulnerability scans to AgyQueue "
            "using `submit_async_task` with task_type='generic'. "
            "You must return the task ID immediately."
        ),
        tools=[submit_async_task, check_task_progress, get_task_output]
    )

def get_coordinator_agent():
    """Release coordinator agent that manages the release checks."""
    return Agent(
        name="release_coordinator",
        model="gemini-2.5-flash",
        instruction=(
            "You are the DevOps Release Pipeline Coordinator. "
            "When asked to validate a release pipeline, coordinate SRE compliance and "
            "app validation by delegating a multi-agent orchestration task directly to AgyQueue "
            "using `submit_async_task` with task_type='multi_agent_deploy'. "
            "If the user requests cancellation, use the `cancel_running_task` tool."
        ),
        tools=[submit_async_task, check_task_progress, get_task_output, cancel_running_task],
        sub_agents=[get_infra_linter_agent(), get_security_auditor_agent()]
    )
