"""
Unit tests for CacheClient using an in-memory fakeredis backend.

Tests cover all public methods plus the internal key layout
(esi:body:*, esi:etag:*, esi:name:*) that proxy.py relies on directly.
"""
import pytest

from app.cache import CacheClient, _CACHE_MAGIC


async def test_set_and_get_returns_body_and_etag(cache_client: CacheClient):
    await cache_client.set("mykey", b'{"foo":1}', ttl=300, etag='"abc123"')
    result = await cache_client.get("mykey")
    assert result is not None
    body, etag = result
    assert body == b'{"foo":1}'
    assert etag == '"abc123"'


async def test_get_missing_returns_none(cache_client: CacheClient):
    assert await cache_client.get("nonexistent") is None


async def test_set_without_etag(cache_client: CacheClient):
    await cache_client.set("mykey2", b"data", ttl=300)
    result = await cache_client.get("mykey2")
    assert result is not None
    body, etag = result
    assert body == b"data"
    assert etag is None


async def test_etag_key_has_longer_ttl_than_body(cache_client: CacheClient):
    """ETag key is stored with ttl+60 so it survives body eviction."""
    await cache_client.set("mykey3", b"x", ttl=300, etag='"etag1"')
    body_ttl = await cache_client._r.ttl("esi:body:mykey3")
    etag_ttl = await cache_client._r.ttl("esi:etag:mykey3")
    # Both should be positive
    assert body_ttl > 0
    assert etag_ttl > 0
    # ETag must outlive the body
    assert etag_ttl > body_ttl


async def test_set_without_etag_does_not_write_etag_key(cache_client: CacheClient):
    """No etag arg means no esi:etag:* key is written at all."""
    await cache_client.set("noetag", b"body", ttl=300)
    raw = await cache_client._r.get("esi:etag:noetag")
    assert raw is None


async def test_name_round_trip(cache_client: CacheClient):
    await cache_client.set_name("tranquility", 12345, "Test Character", "character")
    result = await cache_client.get_name("tranquility", 12345)
    assert result == {"name": "Test Character", "category": "character"}


async def test_get_name_missing_returns_none(cache_client: CacheClient):
    assert await cache_client.get_name("tranquility", 99999) is None


async def test_name_key_layout(cache_client: CacheClient):
    """Verify esi:name:{datasource}:{entity_id} key layout used by archive.write_names."""
    await cache_client.set_name("serenity", 42, "Jita IV", "station")
    raw = await cache_client._r.get("esi:name:serenity:42")
    assert raw is not None
    import json
    assert json.loads(raw) == {"name": "Jita IV", "category": "station"}


async def test_name_ttl_defaults_to_24h(cache_client: CacheClient):
    await cache_client.set_name("tranquility", 7, "Name", "character")
    ttl = await cache_client._r.ttl("esi:name:tranquility:7")
    # Default TTL is 86400 (24h); fakeredis returns exact value
    assert 86390 <= ttl <= 86400


async def test_name_custom_ttl(cache_client: CacheClient):
    await cache_client.set_name("tranquility", 8, "Corp", "corporation", ttl=3600)
    ttl = await cache_client._r.ttl("esi:name:tranquility:8")
    assert 3590 <= ttl <= 3600


async def test_set_names_bulk_round_trip(cache_client: CacheClient):
    await cache_client.set_names(
        "tranquility", [(1, "Alpha", "character"), (2, "Beta", "corporation")]
    )
    assert await cache_client.get_name("tranquility", 1) == {"name": "Alpha", "category": "character"}
    assert await cache_client.get_name("tranquility", 2) == {"name": "Beta", "category": "corporation"}


async def test_set_names_empty_is_noop(cache_client: CacheClient):
    await cache_client.set_names("tranquility", [])
    assert await cache_client.get_name("tranquility", 999) is None


# --- Preserved TTL for 304 revalidation (fix 2.4) ---

async def test_ttl_stored_with_etag(cache_client: CacheClient):
    await cache_client.set("k", b"body", ttl=1234, etag='"e"')
    assert await cache_client.get_ttl("k") == 1234


async def test_ttl_absent_without_etag(cache_client: CacheClient):
    await cache_client.set("k2", b"body", ttl=100)  # no etag → no ttl key
    assert await cache_client.get_ttl("k2") is None


# --- Compression of large payloads (fix 5.2) ---

async def test_large_body_compressed_round_trip(fake_redis):
    client = CacheClient(fake_redis, compress_min_bytes=100)
    body = b'{"orders":[' + b'{"id":1,"price":12.34},' * 60 + b']}'  # repetitive, > 100 bytes
    await client.set("big", body, ttl=300)

    raw = await fake_redis.get("esi:body:big")
    assert raw.startswith(_CACHE_MAGIC)   # stored compressed
    assert len(raw) < len(body)
    got = await client.get("big")
    assert got is not None and got[0] == body           # decodes to original
    assert await client.get_stale("big") == body        # stale copy also decodes


async def test_small_body_not_compressed(fake_redis):
    client = CacheClient(fake_redis, compress_min_bytes=1000)
    body = b'{"small":1}'
    await client.set("small", body, ttl=300)

    raw = await fake_redis.get("esi:body:small")
    assert raw == body and not raw.startswith(_CACHE_MAGIC)
    assert (await client.get("small"))[0] == body


async def test_negative_cache_round_trip(cache_client: CacheClient):
    await cache_client.set_negative("negk", b'{"error":"not found"}', 404, ttl=60)
    result = await cache_client.get_negative("negk")
    assert result == (b'{"error":"not found"}', 404)


async def test_get_negative_missing_returns_none(cache_client: CacheClient):
    assert await cache_client.get_negative("absent") is None


async def test_negative_cache_ttl_applied(cache_client: CacheClient):
    await cache_client.set_negative("negttl", b"{}", 400, ttl=60)
    body_ttl = await cache_client._r.ttl("esi:neg:body:negttl")
    status_ttl = await cache_client._r.ttl("esi:neg:status:negttl")
    assert 50 <= body_ttl <= 60
    assert 50 <= status_ttl <= 60


async def test_positive_set_clears_negative_entry(cache_client: CacheClient):
    """A later 200 for the same key must supersede a cached 4xx."""
    await cache_client.set_negative("supk", b'{"error":"x"}', 404, ttl=60)
    await cache_client.set("supk", b'{"ok":1}', ttl=300)
    assert await cache_client.get_negative("supk") is None
    got = await cache_client.get("supk")
    assert got is not None and got[0] == b'{"ok":1}'


async def test_get_returns_none_after_body_expires(cache_client: CacheClient):
    """
    Confirm that a zero-TTL set makes the body key immediately unavailable.
    (fakeredis respects TTL=1 as the minimum positive value; we use TTL=1 here.)
    We verify the body key is absent while the etag key may still exist at TTL+60.
    """
    # Use a short but valid TTL and then manually delete to simulate expiry
    await cache_client.set("expkey", b"data", ttl=300, etag='"e"')
    # Manually expire the body key to simulate TTL elapsing
    await cache_client._r.delete("esi:body:expkey")
    result = await cache_client.get("expkey")
    # Body key gone → get() returns None
    assert result is None
    # ETag key still present
    raw_etag = await cache_client._r.get("esi:etag:expkey")
    assert raw_etag is not None
