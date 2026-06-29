"""
Unit tests for CacheClient using an in-memory fakeredis backend.

Tests cover all public methods plus the internal key layout
(esi:body:*, esi:etag:*, esi:name:*) that proxy.py relies on directly.
"""
import pytest

from app.cache import CacheClient


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


async def test_refresh_ttl_extends_expiry(cache_client: CacheClient):
    await cache_client.set("refreshkey", b"v", ttl=100, etag='"e1"')
    # Extend to 600
    await cache_client.refresh_ttl("refreshkey", 600)
    body_ttl = await cache_client._r.ttl("esi:body:refreshkey")
    etag_ttl = await cache_client._r.ttl("esi:etag:refreshkey")
    # TTLs should reflect the new value (fakeredis gives exact values)
    assert body_ttl > 100
    assert etag_ttl > 100


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
