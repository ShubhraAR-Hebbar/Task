"""
Logging configuration for Real-Time Pricing Engine.
Includes safe secret masking to ensure credentials are never leaked in logs.
"""

import logging
import re
import sys
from typing import Optional

SECRET_PATTERNS = [
    re.compile(r'(["\']?(?:api_key|secret|password|token)["\']?\s*[:=]\s*["\'])([^"\']+)(["\'])', re.IGNORECASE),
    re.compile(r'(Bearer\s+)([A-Za-z0-9_\-\.]+)', re.IGNORECASE),
]


class SecretMaskingFilter(logging.Filter):
    """
    Log filter that redacts sensitive keys and credentials from log messages.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = self.mask_secrets(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {k: self._mask_value(k, v) for k, v in record.args.items()}
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    self.mask_secrets(str(arg)) if isinstance(arg, str) else arg
                    for arg in record.args
                )
        return True

    @staticmethod
    def _mask_value(key: str, val: any) -> any:
        if any(s in key.lower() for s in ("secret", "key", "password", "token")):
            return "******"
        return val

    @classmethod
    def mask_secrets(cls, text: str) -> str:
        for pattern in SECRET_PATTERNS:
            text = pattern.sub(r"\g<1>******\g<3>", text) if "\\g<3>" in pattern.pattern else pattern.sub(r"\g<1>******", text)
        return text


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Configure root application logging with standard formatting and secret masking.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.addFilter(SecretMaskingFilter())

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Avoid duplicate handlers on reload
    if not root_logger.handlers:
        root_logger.addHandler(handler)
    else:
        root_logger.handlers = [handler]

    # Suppress verbose websockets debug noise unless DEBUG is set
    if level > logging.DEBUG:
        logging.getLogger("websockets").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)

    return logging.getLogger("pricing_engine")
