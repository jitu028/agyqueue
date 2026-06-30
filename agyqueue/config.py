import os
from typing import Optional

class Settings:
    """Consolidated configuration settings for the AgyQueue application."""
    
    @property
    def store_type(self) -> str:
        return os.environ.get("AGYQUEUE_STORE_TYPE", "sqlite").lower()
        
    @property
    def db_path(self) -> str:
        return os.environ.get("AGYQUEUE_DB_PATH", "agyqueue.db")

    @property
    def db_host(self) -> Optional[str]:
        return os.environ.get("DB_HOST")

    @property
    def db_port(self) -> str:
        return os.environ.get("DB_PORT", "5432")

    @property
    def db_name(self) -> str:
        return os.environ.get("DB_NAME", "agyqueue")

    @property
    def db_user(self) -> str:
        return os.environ.get("DB_USER", "postgres")

    @property
    def db_password(self) -> Optional[str]:
        return os.environ.get("DB_PASSWORD")

    @property
    def database_url(self) -> Optional[str]:
        # Direct URL connection if provided
        url = os.environ.get("DATABASE_URL")
        if url:
            return url
        
        # Build URL from components if postgres host is configured
        host = self.db_host
        if host:
            pw = self.db_password
            pw_str = f":{pw}" if pw else ""
            return f"postgresql://{self.db_user}{pw_str}@{host}:{self.db_port}/{self.db_name}"
        return None

    @property
    def redis_url(self) -> Optional[str]:
        return os.environ.get("REDIS_URL")

    @property
    def transport(self) -> str:
        return os.environ.get("AGYQUEUE_TRANSPORT", "stdio").lower()

    @property
    def host(self) -> str:
        return os.environ.get("AGYQUEUE_HOST", "127.0.0.1")

    @property
    def port(self) -> int:
        return int(os.environ.get("AGYQUEUE_PORT", "8000"))

    @property
    def heartbeat_timeout(self) -> float:
        return float(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "15.0"))

    @property
    def server_url(self) -> str:
        return os.environ.get("AGYQUEUE_SERVER_URL", "http://localhost:8000")

# Global settings instance
settings = Settings()
