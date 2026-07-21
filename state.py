# state.py

import time

import db


class SeenTokens:
    """
    TTL-based dedup store.
    When `scope` is given, entries are persisted to SQLite and reloaded
    on startup, so restarts don't cause a wave of duplicate alerts.
    """

    def __init__(self, ttl_seconds: int, scope: str | None = None):
        self.ttl = ttl_seconds
        self.scope = scope
        self._store: dict[str, float] = {}
        if scope:
            self._store = db.load_seen(scope, ttl_seconds)
            if self._store:
                print(f"[state] Restored {len(self._store)} '{scope}' entries from DB")

    def has(self, address: str) -> bool:
        ts = self._store.get(address)
        if ts is None:
            return False
        if time.time() - ts > self.ttl:
            del self._store[address]
            return False
        return True

    def add(self, address: str):
        self._store[address] = time.time()
        if self.scope:
            db.save_seen(self.scope, address)

    def cleanup(self):
        now = time.time()
        expired = [k for k, v in self._store.items() if now - v > self.ttl]
        for k in expired:
            del self._store[k]
        if self.scope:
            db.cleanup_seen(self.scope, self.ttl)
        return len(expired)


class VolumeHistory:
    """
    Stores previous volume.m5 snapshots per token address.
    Used to detect sudden volume spikes between scans.
    """

    def __init__(self, ttl_seconds: int = 3600):
        self.ttl = ttl_seconds
        # address -> {"vol_m5": float, "vol_h1": float, "ts": float}
        self._store: dict[str, dict] = {}

    def get(self, address: str) -> dict | None:
        entry = self._store.get(address)
        if entry is None:
            return None
        if time.time() - entry["ts"] > self.ttl:
            del self._store[address]
            return None
        return entry

    def set(self, address: str, vol_m5: float, vol_h1: float):
        self._store[address] = {
            "vol_m5": vol_m5,
            "vol_h1": vol_h1,
            "ts": time.time(),
        }

    def cleanup(self):
        now = time.time()
        expired = [k for k, v in self._store.items() if now - v["ts"] > self.ttl]
        for k in expired:
            del self._store[k]
        return len(expired)