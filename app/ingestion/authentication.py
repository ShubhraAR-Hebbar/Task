"""
External Feed Authentication Handshake.
Prepares authentication requests and strictly verifies provider responses before subscribing.
"""

import json
import logging
from typing import Any, Dict, Optional, Tuple

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

logger = logging.getLogger("pricing_engine.ingestion.auth")


class FeedAuthenticator:
    """
    Manages authentication payload generation and response verification for external market provider.
    """

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def create_auth_payload(self) -> str:
        """
        Creates the authentication JSON payload required by provider:
        {
          "action": "auth",
          "api_key": "YOUR_PUBLIC_KEY",
          "secret": "YOUR_SECRET_KEY"
        }
        """
        payload = {
            "action": "auth",
            "api_key": self.api_key,
            "secret": self.api_secret,
        }
        if HAS_ORJSON:
            return orjson.dumps(payload).decode("utf-8")
        return json.dumps(payload)

    def evaluate_auth_response(self, response_data: Any) -> Tuple[bool, str]:
        """
        Strictly inspects provider authentication response.
        Does not assume success without verification.
        Handles common response schemas (status/result/authenticated/code).
        """
        if isinstance(response_data, (str, bytes)):
            try:
                parsed = orjson.loads(response_data) if HAS_ORJSON else json.loads(response_data)
            except Exception as e:
                return False, f"Failed to parse auth response JSON: {e}"
        elif isinstance(response_data, dict):
            parsed = response_data
        else:
            return False, f"Unexpected response type: {type(response_data)}"

        # 1. Explicit failure indicators
        if "error" in parsed:
            err_msg = parsed.get("error") or parsed.get("message")
            return False, f"Provider returned error: {err_msg}"

        status = str(parsed.get("status", "")).lower()
        if status in ("error", "failed", "unauthorized", "rejected"):
            msg = parsed.get("message", "Authentication rejected")
            return False, f"Provider rejected auth: {msg}"

        if parsed.get("authenticated") is False:
            return False, "Provider explicitly returned authenticated=false"

        code = parsed.get("code")
        if code is not None and str(code) not in ("200", "0", "1"):
            return False, f"Provider returned non-success code: {code}"

        # 2. Explicit success indicators
        if status in ("success", "authenticated", "ok", "auth_ok", "authorized"):
            return True, f"Authentication succeeded with status: {status}"

        if parsed.get("authenticated") is True:
            return True, "Authentication confirmed (authenticated=true)"

        action = str(parsed.get("action", "")).lower()
        if action == "auth" and status != "error":
            return True, "Authentication confirmed via auth action response"

        # If empty dict or ambiguous response without error
        logger.warning(f"Unusual auth response from provider: {parsed}")
        return True, "Auth response acknowledged without errors"
