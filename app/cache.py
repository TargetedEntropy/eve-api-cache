"""
Redis hot-cache layer.

Key layout:
  esi:body:{cache_key}        → ESI response bytes (with TTL); zstd/zlib-compressed above a size threshold
  esi:stale:{cache_key}       → stale response bytes (TTL + stale window), same encoding as body
  esi:etag:{cache_key}        → ETag string (same TTL + 60s buffer)
  esi:ttl:{cache_key}         → last body TTL, so a 304 without Cache-Control can preserve it
  esi:neg:body:{cache_key}    → cached 4xx body (short TTL — negative cache)
  esi:neg:status:{cache_key}  → cached 4xx status code (same TTL)
  esi:name:{datasource}:{entity_id} → JSON {"name": str, "category": str} (24h TTL)
"""
import json
from typing import Optional

from redis.asyncio import Redis

from app.config import Settings

# Marks a compressed cache value: _CACHE_MAGIC + 1 codec byte (z=zstd, l=zlib) + data.
# ESI bodies are JSON (start with '[', '{', or whitespace), so this binary prefix
# never collides with an uncompressed value; legacy unprefixed values read as raw.
_CACHE_MAGIC = b"\x00zc\x00"


def _compress_value(body: bytes) -> bytes:
    try:
        import zstandard as zstd

        return _CACHE_MAGIC + b"z" + zstd.ZstdCompressor(level=6).compress(body)
    except ImportError:
        import zlib

        return _CACHE_MAGIC + b"l" + zlib.compress(body, level=6)


def _decode_value(value: bytes) -> bytes:
    if not value.startswith(_CACHE_MAGIC):
        return value
    codec = value[len(_CACHE_MAGIC):len(_CACHE_MAGIC) + 1]
    data = value[len(_CACHE_MAGIC) + 1:]
    if codec == b"z":
        import zstandard as zstd

        return zstd.ZstdDecompressor().decompress(data)
    if codec == b"l":
        import zlib

        return zlib.decompress(data)
    return value


class CacheClient:
    def __init__(self, redis: Redis, compress_min_bytes: int = 65536) -> None:
        self._r = redis
        self._compress_min_bytes = compress_min_bytes

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
        return _decode_value(body), etag.decode() if etag else None

    async def set(
        self,
        key: str,
        body: bytes,
        ttl: int,
        etag: Optional[str] = None,
        stale_ttl: int = 86400,
    ) -> None:
        """Store body with TTL, plus a bounded stale copy for degraded-mode fallback."""
        stored = _compress_value(body) if len(body) >= self._compress_min_bytes else body
        pipe = self._r.pipeline()
        pipe.set(f"esi:body:{key}", stored, ex=ttl)
        if stale_ttl > 0:
            pipe.set(f"esi:stale:{key}", stored, ex=ttl + stale_ttl)
        else:
            pipe.delete(f"esi:stale:{key}")
        if etag:
            pipe.set(f"esi:etag:{key}", etag, ex=ttl + 60)
            # Remember the TTL so a later 304 without Cache-Control can preserve it.
            pipe.set(f"esi:ttl:{key}", str(ttl), ex=ttl + 60)
        else:
            pipe.delete(f"esi:etag:{key}")
            pipe.delete(f"esi:ttl:{key}")
        # A fresh success supersedes any negative-cache entry for this key.
        pipe.delete(f"esi:neg:body:{key}")
        pipe.delete(f"esi:neg:status:{key}")
        await pipe.execute()

    async def get_ttl(self, key: str) -> Optional[int]:
        """Return the last stored body TTL for `key`, or None (see 304 handling)."""
        raw = await self._r.get(f"esi:ttl:{key}")
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    async def set_negative(self, key: str, body: bytes, status: int, ttl: int) -> None:
        """
        Briefly cache a 4xx response so a client looping on a known-bad request
        (nonexistent ID, malformed params) doesn't burn one ESI error per call.
        Never stores a stale copy or ETag — errors are not revalidated.
        """
        pipe = self._r.pipeline()
        pipe.set(f"esi:neg:body:{key}", body, ex=ttl)
        pipe.set(f"esi:neg:status:{key}", str(status), ex=ttl)
        await pipe.execute()

    async def get_negative(self, key: str) -> Optional[tuple[bytes, int]]:
        """Return (body, status) for a cached 4xx, or None if not negatively cached."""
        pipe = self._r.pipeline()
        pipe.get(f"esi:neg:body:{key}")
        pipe.get(f"esi:neg:status:{key}")
        body, status = await pipe.execute()
        if body is None or status is None:
            return None
        try:
            return body, int(status)
        except (TypeError, ValueError):
            return None

    async def get_stale(self, key: str) -> Optional[bytes]:
        """Return a recently expired cached body for degraded-mode fallback, if present."""
        raw = await self._r.get(f"esi:stale:{key}")
        return _decode_value(raw) if raw is not None else None

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

    async def set_names(self, datasource: str, items, ttl: int = 86400) -> None:
        """Bulk-cache ID→name mappings in a single Redis pipeline.

        `items` is an iterable of (entity_id, name, category) tuples.
        """
        items = list(items)
        if not items:
            return
        pipe = self._r.pipeline()
        for entity_id, name, category in items:
            value = json.dumps({"name": name, "category": category})
            pipe.set(f"esi:name:{datasource}:{entity_id}", value, ex=ttl)
        await pipe.execute()


async def create_cache_client(settings: Settings) -> CacheClient:
    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    return CacheClient(redis, compress_min_bytes=settings.cache_compress_min_bytes)
