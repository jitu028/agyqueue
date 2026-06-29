import os
import logging
import json
import urllib.request
import smtplib
from email.mime.text import MIMEText
from abc import ABC, abstractmethod
from typing import List, Optional

logger = logging.getLogger(__name__)

class BaseNotificationBackend(ABC):
    """Abstract base class for notification channels."""
    
    @abstractmethod
    def send_notification(self, task_id: str, status: str, progress: int, step: str, result: Optional[str] = None, error: Optional[str] = None) -> None:
        """Sends a notification about a task's status change."""
        pass


class SlackWebhookBackend(BaseNotificationBackend):
    """Sends task updates to a configured Slack Webhook channel."""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_notification(self, task_id: str, status: str, progress: int, step: str, result: Optional[str] = None, error: Optional[str] = None) -> None:
        if not self.webhook_url:
            return
            
        emoji = "ℹ️"
        color = "#36a64f"
        
        if status == "COMPLETED":
            emoji = "✅"
            color = "#2eb886"
        elif status == "FAILED":
            emoji = "❌"
            color = "#a30200"
        elif status == "CANCELLED":
            emoji = "⚠️"
            color = "#e0a115"
        elif status == "RUNNING":
            emoji = "🔄"
            color = "#1d9bd1"

        title = f"{emoji} AgyQueue Task {status}: {task_id}"
        
        fields = [
            {"title": "Task ID", "value": task_id, "short": True},
            {"title": "Status", "value": status, "short": True},
            {"title": "Progress", "value": f"{progress}%", "short": True},
            {"title": "Current Step", "value": step, "short": False}
        ]
        
        if error:
            fields.append({"title": "Error Message", "value": error, "short": False})

        payload = {
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "fields": fields,
                    "fallback": f"AgyQueue Task {task_id} updated: {status} ({progress}%) - {step}"
                }
            ]
        }
        
        try:
            req_data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                self.webhook_url,
                data=req_data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                resp.read()
            logger.info(f"[Slack Notification] Successfully sent update for task {task_id}")
        except Exception as e:
            logger.error(f"[Slack Notification] Failed to send update for task {task_id}: {e}")


class EmailSMTPBackend(BaseNotificationBackend):
    """Sends task updates via email using SMTP."""
    
    def __init__(self, smtp_host: str, smtp_port: int, smtp_user: Optional[str], smtp_pass: Optional[str], email_from: str, email_to: str):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_pass = smtp_pass
        self.email_from = email_from
        self.email_to = email_to

    def send_notification(self, task_id: str, status: str, progress: int, step: str, result: Optional[str] = None, error: Optional[str] = None) -> None:
        if not self.smtp_host or not self.email_to:
            return
            
        subject = f"[AgyQueue] Task {status}: {task_id}"
        
        body_parts = [
            f"AgyQueue Task Update",
            f"----------------------------------------",
            f"Task ID:      {task_id}",
            f"Status:       {status}",
            f"Progress:     {progress}%",
            f"Current Step: {step}",
            f"----------------------------------------",
        ]
        
        if error:
            body_parts.append(f"Error details:\n{error}\n")
        elif result and status == "COMPLETED":
            body_parts.append(f"Execution Output:\n{result}\n")
            
        msg = MIMEText("\n".join(body_parts))
        msg["Subject"] = subject
        msg["From"] = self.email_from
        msg["To"] = self.email_to
        
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=5.0) as server:
                if self.smtp_user and self.smtp_pass:
                    server.starttls()
                    server.login(self.smtp_user, self.smtp_pass)
                server.sendmail(self.email_from, [self.email_to], msg.as_string())
            logger.info(f"[Email Notification] Successfully sent update for task {task_id}")
        except Exception as e:
            logger.error(f"[Email Notification] Failed to send email for task {task_id}: {e}")


class NotificationManager:
    """Manages routing of task notifications across multiple active channels."""
    
    def __init__(self):
        self.backends: List[BaseNotificationBackend] = []
        self._load_backends()

    def _load_backends(self) -> None:
        # Load from config / environment
        from agyqueue.config import settings
        
        # Comma-separated active backends (e.g. "slack,email")
        active_channels = os.environ.get("AGYQUEUE_NOTIFICATIONS", "").lower().split(",")
        active_channels = [c.strip() for c in active_channels if c.strip()]
        
        if "slack" in active_channels:
            webhook = os.environ.get("SLACK_WEBHOOK_URL")
            if webhook:
                self.backends.append(SlackWebhookBackend(webhook))
                logger.info("[Notification Manager] Slack channel enabled.")
            else:
                logger.warning("[Notification Manager] Slack enabled but SLACK_WEBHOOK_URL is missing.")
                
        if "email" in active_channels:
            smtp_host = os.environ.get("SMTP_HOST")
            email_to = os.environ.get("SMTP_TO")
            if smtp_host and email_to:
                try:
                    port = int(os.environ.get("SMTP_PORT", "587"))
                except ValueError:
                    port = 587
                self.backends.append(
                    EmailSMTPBackend(
                        smtp_host=smtp_host,
                        smtp_port=port,
                        smtp_user=os.environ.get("SMTP_USER"),
                        smtp_pass=os.environ.get("SMTP_PASSWORD"),
                        email_from=os.environ.get("SMTP_FROM", "noreply@agyqueue.internal"),
                        email_to=email_to
                    )
                )
                logger.info("[Notification Manager] Email SMTP channel enabled.")
            else:
                logger.warning("[Notification Manager] Email enabled but SMTP_HOST or SMTP_TO is missing.")

    def trigger_notifications(self, task_id: str, status: str, progress: int, step: str, result: Optional[str] = None, error: Optional[str] = None) -> None:
        """Broadcasts updates to all active notification backends."""
        for backend in self.backends:
            try:
                backend.send_notification(task_id, status, progress, step, result, error)
            except Exception as e:
                logger.error(f"Error triggering notification on backend {backend.__class__.__name__}: {e}")

# Global notification manager instance
notifications = NotificationManager()
