"""
Client Subscription Management.
Maintains bidirectional mapping between connected clients and subscribed symbols.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

logger = logging.getLogger("pricing_engine.distribution.subscriptions")


@dataclass
class ClientSession:
    """Represents an active client WebSocket session."""
    client_id: str
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    subscribed_symbols: Set[str] = field(default_factory=set)
    dropped_count: int = 0


class SubscriptionManager:
    """
    Thread-safe and async-compatible subscription directory.
    Provides fast lookup from symbol to active subscribers for minimal broadcast latency.
    """

    def __init__(self, default_queue_size: int = 1000):
        self.default_queue_size = default_queue_size
        self._clients: Dict[str, ClientSession] = {}
        # Mapping from symbol (uppercase) to set of client_ids
        self._symbol_subscribers: Dict[str, Set[str]] = {}
        self._lock = asyncio.Lock()

    def register_client(self, client_id: str, queue_size: Optional[int] = None) -> asyncio.Queue:
        q_size = queue_size or self.default_queue_size
        session = ClientSession(client_id=client_id, queue=asyncio.Queue(maxsize=q_size))
        self._clients[client_id] = session
        logger.info(f"Client {client_id} registered. Total clients: {len(self._clients)}")
        return session.queue

    def unregister_client(self, client_id: str) -> None:
        session = self._clients.pop(client_id, None)
        if session:
            # Remove from all symbol subscriber sets
            for sym in session.subscribed_symbols:
                subs = self._symbol_subscribers.get(sym)
                if subs and client_id in subs:
                    subs.remove(client_id)
                    if not subs:
                        self._symbol_subscribers.pop(sym, None)
            logger.info(f"Client {client_id} unregistered. Remaining clients: {len(self._clients)}")

    def subscribe(self, client_id: str, symbols: List[str]) -> Set[str]:
        """Subscribes client to symbols. Returns newly added symbols."""
        session = self._clients.get(client_id)
        if not session:
            return set()

        newly_added = set()
        for s in symbols:
            upper = s.strip().upper()
            if not upper:
                continue
            if upper not in session.subscribed_symbols:
                session.subscribed_symbols.add(upper)
                newly_added.add(upper)
                if upper not in self._symbol_subscribers:
                    self._symbol_subscribers[upper] = set()
                self._symbol_subscribers[upper].add(client_id)

        logger.debug(f"Client {client_id} subscribed to {newly_added}. Total subs: {len(session.subscribed_symbols)}")
        return newly_added

    def unsubscribe(self, client_id: str, symbols: List[str]) -> Set[str]:
        """Unsubscribes client from symbols."""
        session = self._clients.get(client_id)
        if not session:
            return set()

        removed = set()
        for s in symbols:
            upper = s.strip().upper()
            if upper in session.subscribed_symbols:
                session.subscribed_symbols.remove(upper)
                removed.add(upper)
                subs = self._symbol_subscribers.get(upper)
                if subs and client_id in subs:
                    subs.remove(client_id)
                    if not subs:
                        self._symbol_subscribers.pop(upper, None)

        logger.debug(f"Client {client_id} unsubscribed from {removed}")
        return removed

    def get_subscribers_for_symbol(self, symbol: str) -> List[str]:
        """Returns list of client_ids subscribed to a specific symbol."""
        subs = self._symbol_subscribers.get(symbol.upper())
        return list(subs) if subs else []

    def get_client_session(self, client_id: str) -> Optional[ClientSession]:
        return self._clients.get(client_id)

    @property
    def client_count(self) -> int:
        return len(self._clients)
