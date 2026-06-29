"""
Redis hot-cache layer.

Key layout:
  esi:body:{cache_key}   → raw ESI response bytes (with TTL)
  esi:etag:{cache_key}   → ETag string (same TTL + 60s buffer)
  esi:name:{datasource}:{entity_id} → JSON {"name": str, "category": str} (24h TTL)
"""
import json
from typing import Optional

from redis.asyncio import Redis

from app.config import Settings


class CacheClient:
    def __init__(self, redis: Redis) -> None:
        self._r = redis

    async def get(self, key: str) -> Optional[tuple[bytes, Optional[str]]]:
        """
        Return (body_bytes, etag_or_None) if cached, else None.
        Uses a pipeline to fetch body and etag atomically.
        """
        pipe = self._r.pipeline()
        pipe.get(f"esi:body:{key}")
        pipe.get(f"esi:etag:{key}")
        body, etag = await pipe.execute()
        if body is None:
            return None
        return body, etag.decode() if etag else None

    async def set(
        self,
        key: str,
        body: bytes,
        ttl: int,
        etag: Optional[str] = None,
    ) -> None:
        """Store body with TTL. ETag stored with a small buffer beyond body TTL."""
        pipe = self._r.pipeline()
        pipe.set(f"esi:body:{key}", body, ex=ttl)
        if etag:
            pipe.set(f"esi:etag:{key}", etag, ex=ttl + 60)
        await pipe.execute()

    async def refresh_ttl(self, key: str, ttl: int) -> None:
        """Extend TTL on body (and etag if present) without changing the value."""
        pipe = self._r.pipeline()
        pipe.expire(f"esi:body:{key}", ttl)
        pipe.expire(f"esi:etag:{key}", ttl + 60)
        await pipe.execute()

    async def get_name(self, datasource: str, entity_id: int) -> Optional[dict]:
        """Return {"name": str, "category": str} for a known entity ID, or None."""
        raw = await self._r.get(f"esi:name:{datasource}:{entity_id}")
        if raw is None:
            return None
        return json.loads(raw)

    async def set_name(
        self,
        datasource: str,
        entity_id: int,
        name: str,
        category: str,
        ttl: int = 86400,
    ) -> None:
        """Cache an individual ID→name mapping."""
        value = json.dumps({"name": name, "category": category})
        await self._r.set(f"esi:name:{datasource}:{entity_id}", value, ex=ttl)


async def create_cache_client(settings: Settings) -> CacheClient:
    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    return CacheClient(redis)
