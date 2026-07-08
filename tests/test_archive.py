"""PostgreSQL-backed tests for the archive write layer."""
import hashlib
import json
import uuid

import pytest
from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import Insert as PGInsert
from sqlalchemy.sql.dml import Update

import app.archive as archive
from app.allowlist import ArchiveType
from app.archive import get_latest_payload, write_names, write_snapshot
from app.db import ArchivePayloadBlob, AsyncSessionLocal, IdNameCache, TimeSeriesSnapshot

_ORDERS_PATH = "/v1/markets/10000002/orders/"


class _RecordingResult:
    def __init__(self, scalar=None):
        self._scalar = scalar

    def scalar_one_or_none(self):
        return self._scalar

    def scalar_one(self):
        return self._scalar


class _RecordingSession:
    """AsyncSession stand-in that records statements and scripts execute() results."""
    def __init__(self, results=None):
        self.statements = []
        self._results = list(results or [])
        self.commits = 0

    async def execute(self, statement, *args, **kwargs):
        self.statements.append(statement)
        return self._results.pop(0) if self._results else _RecordingResult(None)

    async def commit(self):
        self.commits += 1

    async def get(self, *args, **kwargs):
        return None


def _params(statement) -> dict:
    return statement.compile(dialect=postgresql.dialect()).params


def _dml_for(statements, dml_type, table_name):
    return [
        s for s in statements
        if isinstance(s, dml_type)
        and getattr(s, "table", None) is not None
        and s.table.name == table_name
    ]


# ---------------------------------------------------------------------------
# Storage labeling + pyarrow fallback (fix 2.6)
# ---------------------------------------------------------------------------

async def test_market_orders_row_starts_compressed_json_then_relabeled(monkeypatch):
    """Row is inserted as compressed_json and only flipped to parquet_delta
    after the manifest is written."""
    monkeypatch.setattr(archive, "_PYARROW_AVAILABLE", True)
    monkeypatch.setattr(archive.settings, "enable_market_order_parquet", True)

    captured = {}

    async def fake_parquet(**kwargs):
        captured.update(kwargs)
        return 777

    monkeypatch.setattr(archive, "_write_market_orders_parquet", fake_parquet)

    session = _RecordingSession(results=[_RecordingResult(None), _RecordingResult(42)])
    await write_snapshot(
        session, "tranquility", _ORDERS_PATH, "qh", "c" * 64,
        b'[{"order_id":1}]', 200, None, None, ArchiveType.TIME_SERIES,
    )

    inserts = _dml_for(session.statements, PGInsert, "archive_timeseries")
    assert len(inserts) == 1
    assert _params(inserts[0])["payload_storage"] == "compressed_json"

    assert captured.get("snapshot_id") == 42
    updates = _dml_for(session.statements, Update, "archive_timeseries")
    assert len(updates) == 1
    assert _params(updates[0])["payload_storage"] == "market_parquet_delta"
    assert session.commits == 1


async def test_market_orders_pyarrow_missing_falls_back_without_relabel(monkeypatch):
    """pyarrow absent → Parquet path skipped, row stays compressed_json, write succeeds."""
    monkeypatch.setattr(archive, "_PYARROW_AVAILABLE", False)
    monkeypatch.setattr(archive.settings, "enable_market_order_parquet", True)

    called = {"parquet": False}

    async def fake_parquet(**kwargs):
        called["parquet"] = True
        return 1

    monkeypatch.setattr(archive, "_write_market_orders_parquet", fake_parquet)

    session = _RecordingSession(results=[_RecordingResult(None), _RecordingResult(42)])
    await write_snapshot(
        session, "tranquility", _ORDERS_PATH, "qh", "c" * 64,
        b'[{"order_id":1}]', 200, None, None, ArchiveType.TIME_SERIES,
    )

    assert called["parquet"] is False
    assert _dml_for(session.statements, Update, "archive_timeseries") == []
    inserts = _dml_for(session.statements, PGInsert, "archive_timeseries")
    assert _params(inserts[0])["payload_storage"] == "compressed_json"
    assert session.commits == 1  # archive write still succeeds


async def test_market_orders_manifest_none_keeps_compressed_json(monkeypatch):
    """If the Parquet manifest can't be obtained, the row is not relabeled."""
    monkeypatch.setattr(archive, "_PYARROW_AVAILABLE", True)
    monkeypatch.setattr(archive.settings, "enable_market_order_parquet", True)

    async def fake_parquet(**kwargs):
        return None

    monkeypatch.setattr(archive, "_write_market_orders_parquet", fake_parquet)

    session = _RecordingSession(results=[_RecordingResult(None), _RecordingResult(42)])
    await write_snapshot(
        session, "tranquility", _ORDERS_PATH, "qh", "c" * 64,
        b'[{"order_id":1}]', 200, None, None, ArchiveType.TIME_SERIES,
    )

    assert _dml_for(session.statements, Update, "archive_timeseries") == []
    assert session.commits == 1


# ---------------------------------------------------------------------------
# Manifest insert race → reuse instead of losing the whole snapshot (fix 2.5)
# ---------------------------------------------------------------------------

async def test_manifest_insert_conflict_reuses_existing(monkeypatch):
    """A concurrent writer wins the content_hash insert; we reuse its manifest
    (on_conflict_do_nothing → fallback SELECT) instead of raising and losing the
    whole snapshot. The Parquet file write is stubbed so this runs without pyarrow."""
    monkeypatch.setattr(archive.settings, "enable_market_order_deltas", False)

    file_writes = []

    def fake_file_write(**kwargs):
        file_writes.append(kwargs["content_hash"])
        return ("markets/orders/datasource=tranquility/snap.parquet", 123)

    monkeypatch.setattr(archive, "_write_market_orders_parquet_file", fake_file_write)

    # initial lookup → None; INSERT ... ON CONFLICT DO NOTHING RETURNING → None
    # (row already committed by the racing writer); fallback lookup → its id.
    session = _RecordingSession(results=[
        _RecordingResult(None),
        _RecordingResult(None),
        _RecordingResult(4242),
    ])

    manifest_id = await archive._write_market_orders_parquet(
        session=session,
        snapshot_id=1,
        datasource="tranquility",
        path=_ORDERS_PATH,
        query_hash="qh",
        content_hash="d" * 64,
        payload=b'[{"order_id":1,"type_id":34,"price":5.0}]',
        fetched_at=archive.datetime(2026, 6, 29, 21, 0, tzinfo=archive.timezone.utc),
    )

    assert manifest_id == 4242            # reused, no UniqueViolation, snapshot preserved
    assert file_writes == ["d" * 64]      # file written once before the insert attempt
    object_inserts = _dml_for(session.statements, PGInsert, "archive_object_files")
    assert len(object_inserts) == 1


# ---------------------------------------------------------------------------
# pyarrow availability helper (fix 2.6)
# ---------------------------------------------------------------------------

def test_pyarrow_available_caches(monkeypatch):
    monkeypatch.setattr(archive, "_PYARROW_AVAILABLE", None)
    first = archive._pyarrow_available()
    assert isinstance(first, bool)
    # second call returns the cached value without re-importing
    assert archive._pyarrow_available() is first


# ---------------------------------------------------------------------------
# write_names batches DB upserts and Redis writes (fix 5.4)
# ---------------------------------------------------------------------------

async def test_write_names_batches_db_and_redis():
    class _BulkCache:
        def __init__(self):
            self.calls = []

        async def set_names(self, datasource, items, ttl=86400):
            self.calls.append((datasource, list(items)))

    cache = _BulkCache()
    session = _RecordingSession()
    payload = json.dumps(
        [{"id": n, "name": f"name{n}", "category": "character"} for n in range(1, 6)]
    ).encode()

    await write_names(session, cache, "tranquility", payload)

    # A single batched upsert into id_name_cache, not one statement per ID.
    inserts = _dml_for(session.statements, PGInsert, "id_name_cache")
    assert len(inserts) == 1
    # A single bulk Redis call carrying all five mappings.
    assert len(cache.calls) == 1
    assert len(cache.calls[0][1]) == 5
    assert session.commits == 1


def test_extract_name_mappings_forms_and_edges():
    extract = archive._extract_name_mappings
    # list form (/universe/names/)
    assert extract([{"id": 5, "name": "X", "category": "character"}]) == [(5, "X", "character")]
    # dict form (/universe/ids/): category key singularized
    assert set(extract({"characters": [{"id": 1, "name": "A"}], "systems": [{"id": 2, "name": "B"}]})) == {
        (1, "A", "character"), (2, "B", "system"),
    }
    # entries missing id/name are skipped; id=0 is falsy and filtered
    assert extract([{"id": 0, "name": "Z"}, {"name": "noid"}, {"id": 9}]) == []
    # non-list/dict payloads yield nothing
    assert extract("nope") == []


class _FakeNameCache:
    def __init__(self) -> None:
        self.names = {}

    async def set_name(self, datasource: str, entity_id: int, name: str, category: str, ttl: int = 86400) -> None:
        self.names[(datasource, entity_id)] = {
            "name": name,
            "category": category,
            "ttl": ttl,
        }

    async def set_names(self, datasource: str, items, ttl: int = 86400) -> None:
        for entity_id, name, category in items:
            self.names[(datasource, entity_id)] = {"name": name, "category": category, "ttl": ttl}


def test_payload_compression_round_trip():
    payload = (b'[{"order_id":1,"type_id":34,"price":12.3}]' * 100)
    codec, compressed = archive._compress_payload(payload)

    assert codec in {"zstd", "zlib"}
    assert len(compressed) < len(payload)
    assert archive._decompress_payload(codec, compressed) == payload


def test_chunks_splits_large_delta_batches():
    rows = [{"id": i} for i in range(2501)]

    batches = list(archive._chunks(rows, 1000))

    assert [len(batch) for batch in batches] == [1000, 1000, 501]
    assert batches[0][0] == {"id": 0}
    assert batches[-1][-1] == {"id": 2500}


def test_market_orders_parquet_file_write(tmp_path, monkeypatch):
    pytest.importorskip("pyarrow")
    monkeypatch.setattr(archive.settings, "archive_data_dir", str(tmp_path))

    path, stored_size = archive._write_market_orders_parquet_file(
        datasource="tranquility",
        region_id=10000002,
        content_hash="a" * 64,
        fetched_at=archive.datetime(2026, 6, 29, 21, 0, tzinfo=archive.timezone.utc),
        orders=[
            {
                "order_id": 1,
                "type_id": 34,
                "price": 5.0,
                "volume_remain": 100,
                "is_buy_order": False,
            }
        ],
    )

    assert path.endswith(".parquet")
    assert (tmp_path / path).exists()
    assert stored_size > 0


@pytest.fixture
async def postgres_archive():
    try:
        async with AsyncSessionLocal() as session:
            table_exists = await session.scalar(text("SELECT to_regclass('archive_timeseries')"))
            if table_exists is None:
                pytest.skip("PostgreSQL archive schema is not migrated")
    except Exception as exc:
        pytest.skip(f"PostgreSQL test database is unavailable: {exc}")


@pytest.mark.postgres
async def test_timeseries_write_is_idempotent_within_retry_bucket(postgres_archive):
    path = f"/v1/test/{uuid.uuid4()}/"
    # Large/repetitive enough to actually compress smaller — a tiny blob is
    # dominated by codec framing overhead and ends up larger once compressed.
    payload = json.dumps([{"order_id": n, "type_id": 34, "price": 12.3} for n in range(40)]).encode()
    content_hash = hashlib.sha256(payload).hexdigest()

    try:
        async with AsyncSessionLocal() as session:
            await write_snapshot(
                session,
                "tranquility",
                path,
                "queryhash",
                content_hash,
                payload,
                200,
                '"etag"',
                None,
                ArchiveType.TIME_SERIES,
            )
            await write_snapshot(
                session,
                "tranquility",
                path,
                "queryhash",
                content_hash,
                payload,
                200,
                '"etag"',
                None,
                ArchiveType.TIME_SERIES,
            )

        async with AsyncSessionLocal() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(TimeSeriesSnapshot)
                .where(TimeSeriesSnapshot.path == path)
            )
            assert count == 1
            fallback = await get_latest_payload(session, "tranquility", path, "queryhash")
            assert fallback == payload

            blob = await session.get(ArchivePayloadBlob, content_hash)
            assert blob is not None
            assert blob.raw_size == len(payload)
            assert blob.compressed_size < blob.raw_size
    finally:
        async with AsyncSessionLocal() as session:
            await session.execute(delete(TimeSeriesSnapshot).where(TimeSeriesSnapshot.path == path))
            await session.commit()


@pytest.mark.postgres
async def test_write_names_extracts_universe_ids_object_response(postgres_archive):
    datasource = "tranquility"
    entity_ids = [99000001, 99000002]
    payload = (
        b'{"characters":[{"id":99000001,"name":"Pilot One"}],'
        b'"systems":[{"id":99000002,"name":"Jita"}]}'
    )
    cache = _FakeNameCache()

    try:
        async with AsyncSessionLocal() as session:
            await write_names(session, cache, datasource, payload)

        assert cache.names[(datasource, 99000001)]["category"] == "character"
        assert cache.names[(datasource, 99000002)]["category"] == "system"

        async with AsyncSessionLocal() as session:
            rows = (
                await session.execute(
                    select(IdNameCache)
                    .where(IdNameCache.datasource == datasource)
                    .where(IdNameCache.entity_id.in_(entity_ids))
                )
            ).scalars().all()

        assert {row.entity_name for row in rows} == {"Pilot One", "Jita"}
        assert {row.category for row in rows} == {"character", "system"}
    finally:
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(IdNameCache)
                .where(IdNameCache.datasource == datasource)
                .where(IdNameCache.entity_id.in_(entity_ids))
            )
            await session.commit()
