"""
External Provider Subscription Management.
Formats `subscribeMarketPrice` commands for external feed ingestion.
"""

import json
import logging
from typing import Any, Dict, List, Optional

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

logger = logging.getLogger("pricing_engine.ingestion.subscriptions")


class ExternalSubscriptionBuilder:
    """
    Constructs and verifies market data subscriptions for the external provider.
    """

    def __init__(self, symbols: List[str]):
        self.symbols = [s.strip().upper() for s in symbols if s.strip()]

    def create_subscription_payload(self) -> str:
        """
        Builds the mandatory subscription payload:
        {
          "action": "subscribeMarketPrice",
          "symbols": ["EURUSD", "BTCUSD", ...]
        }
        """
        payload = {
            "action": "subscribeMarketPrice",
            "symbols": self.symbols,
        }
        if HAS_ORJSON:
            return orjson.dumps(payload).decode("utf-8")
        return json.dumps(payload)

    def is_subscription_ack(self, message: Any) -> bool:
        """Checks if a received message is a subscription confirmation."""
        if not isinstance(message, dict):
            return False
        action = str(message.get("action", "")).lower()
        return "sub" in action or action == "subscribemarketprice"
