"""
Configuration management using pydantic-settings.
Loads configuration from environment variables and .env file.
"""

from typing import Any, List, Union
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application Settings for the Real-Time Pricing Engine.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # External Market Data Feed
    external_ws_url: str = Field(
        default="wss://v3-prod.livefxhub.com/ws/algo",
        alias="EXTERNAL_WS_URL",
        description="WebSocket URL of external market data provider",
    )
    api_key: str = Field(
        default="",
        alias="API_KEY",
        description="Public API key for external provider",
    )
    api_secret: str = Field(
        default="",
        alias="API_SECRET",
        description="Secret key for external provider authentication",
    )
    subscribe_symbols: Union[List[str], str] = Field(
        default=["EURUSD", "GBPUSD", "USDJPY", "BTCUSD", "ETHUSD"],
        alias="SUBSCRIBE_SYMBOLS",
        description="List of symbols to subscribe to",
    )
    external_ws_reconnect_initial_delay: float = Field(
        default=1.0,
        alias="EXTERNAL_WS_RECONNECT_INITIAL_DELAY",
        description="Initial delay in seconds for exponential backoff",
    )
    external_ws_reconnect_max_delay: float = Field(
        default=60.0,
        alias="EXTERNAL_WS_RECONNECT_MAX_DELAY",
        description="Maximum backoff delay in seconds",
    )
    external_ws_timeout_seconds: float = Field(
        default=10.0,
        alias="EXTERNAL_WS_TIMEOUT_SECONDS",
        description="Connection/Auth timeout in seconds",
    )

    # Outbound WebSocket Server
    outbound_ws_host: str = Field(
        default="0.0.0.0",
        alias="OUTBOUND_WS_HOST",
        description="Host interface to bind outbound WebSocket server",
    )
    outbound_ws_port: int = Field(
        default=8765,
        alias="OUTBOUND_WS_PORT",
        description="Port for outbound WebSocket server",
    )
    client_queue_maxsize: int = Field(
        default=1000,
        alias="CLIENT_QUEUE_MAXSIZE",
        description="Max buffer queue per connected client before backpressure protection",
    )

    # Market Processing & Windowing
    window_seconds: int = Field(
        default=86400,
        alias="WINDOW_SECONDS",
        description="Rolling window size in seconds (86400s = 24h)",
    )
    price_spike_threshold_pct: float = Field(
        default=50.0,
        alias="PRICE_SPIKE_THRESHOLD_PCT",
        description="Price percentage jump threshold to trigger anomaly alert",
    )

    # State Persistence & Redis
    redis_host: str = Field(
        default="localhost",
        alias="REDIS_HOST",
    )
    redis_port: int = Field(
        default=6379,
        alias="REDIS_PORT",
    )
    redis_password: str = Field(
        default="",
        alias="REDIS_PASSWORD",
    )
    redis_db: int = Field(
        default=0,
        alias="REDIS_DB",
    )
    redis_enabled: bool = Field(
        default=False,
        alias="REDIS_ENABLED",
    )
    redis_retry_interval_seconds: float = Field(
        default=5.0,
        alias="REDIS_RETRY_INTERVAL_SECONDS",
    )
    redis_retention_seconds: int = Field(
        default=90000,
        alias="REDIS_RETENTION_SECONDS",
        description="Redis sorted set retention (90000s = 25h, covers 24h + buffer)",
    )

    # Observability & Monitoring
    log_level: str = Field(
        default="INFO",
        alias="LOG_LEVEL",
    )
    metrics_report_interval_seconds: float = Field(
        default=10.0,
        alias="METRICS_REPORT_INTERVAL_SECONDS",
    )

    @field_validator("subscribe_symbols", mode="before")
    @classmethod
    def parse_symbols(cls, value):
        if isinstance(value, str):
            return [s.strip().upper() for s in value.split(",") if s.strip()]
        if isinstance(value, list):
            return [str(s).strip().upper() for s in value if str(s).strip()]
        return value

    @property
    def redis_url(self) -> str:
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


# Singleton instance
settings = Settings()
