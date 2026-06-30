import os
import logging
from google.cloud import storage

logger = logging.getLogger("agyqueue.gcs")

BUCKET_NAME = os.environ.get("ARTIFACTS_BUCKET", "ai-architect-sprint-028-agyqueue-artifacts")

def upload_to_gcs(task_id: str, local_path: str, relative_path: str) -> bool:
    """Uploads a local file to GCS under the task_id prefix."""
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob_name = f"{task_id}/{relative_path}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        logger.info(f"[GCS] Uploaded {local_path} to gs://{BUCKET_NAME}/{blob_name}")
        return True
    except Exception as e:
        logger.error(f"[GCS] Failed to upload {local_path} to GCS: {e}")
        return False

def download_from_gcs(task_id: str, relative_path: str, dest_path: str) -> bool:
    """Downloads a file from GCS under the task_id prefix to a local destination."""
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob_name = f"{task_id}/{relative_path}"
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return False
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        blob.download_to_filename(dest_path)
        logger.info(f"[GCS] Downloaded gs://{BUCKET_NAME}/{blob_name} to {dest_path}")
        return True
    except Exception as e:
        logger.error(f"[GCS] Failed to download {blob_name} from GCS: {e}")
        return False

def list_gcs_artifacts(task_id: str) -> list[str]:
    """Lists all artifacts in GCS under the task_id prefix."""
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        prefix = f"{task_id}/"
        blobs = bucket.list_blobs(prefix=prefix)
        artifacts = []
        for blob in blobs:
            rel_path = blob.name[len(prefix):]
            if rel_path:
                artifacts.append(rel_path)
        return sorted(artifacts)
    except Exception as e:
        logger.error(f"[GCS] Failed to list artifacts in GCS for task {task_id}: {e}")
        return []
