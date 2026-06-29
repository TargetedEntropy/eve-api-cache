"""
Route allowlist and path validation for the ESI proxy.

Only explicitly listed public ESI endpoints are proxied. All others return 404.
Paths are matched against the unversioned portion (after stripping the ESI version prefix).
"""
import hashlib
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ArchiveType(str, Enum):
    TIME_SERIES = "time_series"   # append every snapshot (markets, jumps, kills, sovereignty)
    REFERENCE = "reference"       # upsert by primary key (universe types, systems, corps)
    EVENT = "event"               # insert-once by natural key (killmails, contracts)
    NONE = "none"                 # proxy-only, not archived (/status)


VALID_VERSIONS = frozenset({"v1", "v2", "v3", "v4", "v5", "v6", "latest", "legacy", "dev"})


@dataclass
class EndpointSpec:
    pattern: re.Pattern
    methods: frozenset
    archive_type: ArchiveType
    extract_names: bool = False  # extract ID→name pairs from response (POST batch endpoints)


# All public ESI endpoints we proxy. Pattern matches the path WITHOUT the version prefix.
# Patterns must not match paths containing "..", encoded traversal, or other injection.
_ENDPOINTS: list[EndpointSpec] = [
    # Markets
    EndpointSpec(re.compile(r"^/markets/\d+/orders/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    EndpointSpec(re.compile(r"^/markets/\d+/history/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    EndpointSpec(re.compile(r"^/markets/prices/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    # Universe — reference data
    EndpointSpec(re.compile(r"^/universe/types/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/systems/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/systems/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/regions/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/regions/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/constellations/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/stations/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/planets/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/stars/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/factions/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/universe/groups/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    # Universe — time-series snapshots
    EndpointSpec(re.compile(r"^/universe/system_jumps/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    EndpointSpec(re.compile(r"^/universe/system_kills/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    # Universe — POST batch (extract per-ID names)
    EndpointSpec(re.compile(r"^/universe/names/?$"), frozenset({"POST"}), ArchiveType.REFERENCE, extract_names=True),
    EndpointSpec(re.compile(r"^/universe/ids/?$"), frozenset({"POST"}), ArchiveType.REFERENCE, extract_names=True),
    # Characters (public info only)
    EndpointSpec(re.compile(r"^/characters/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/characters/\d+/portrait/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/characters/\d+/corporationhistory/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    EndpointSpec(re.compile(r"^/characters/affiliation/?$"), frozenset({"POST"}), ArchiveType.REFERENCE, extract_names=True),
    # Corporations
    EndpointSpec(re.compile(r"^/corporations/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/corporations/\d+/icons/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    # Alliances
    EndpointSpec(re.compile(r"^/alliances/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/alliances/\d+/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/alliances/\d+/icons/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    EndpointSpec(re.compile(r"^/alliances/\d+/corporations/?$"), frozenset({"GET"}), ArchiveType.REFERENCE),
    # Contracts (public)
    EndpointSpec(re.compile(r"^/contracts/public/\d+/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    EndpointSpec(re.compile(r"^/contracts/public/items/\d+/?$"), frozenset({"GET"}), ArchiveType.EVENT),
    EndpointSpec(re.compile(r"^/contracts/public/bids/\d+/?$"), frozenset({"GET"}), ArchiveType.EVENT),
    # Killmails (public — requires both ID and hash)
    EndpointSpec(re.compile(r"^/killmails/\d+/[0-9a-f]{40}/?$"), frozenset({"GET"}), ArchiveType.EVENT),
    # Sovereignty / Incursions / Industry
    EndpointSpec(re.compile(r"^/sovereignty/map/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    EndpointSpec(re.compile(r"^/sovereignty/structures/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    EndpointSpec(re.compile(r"^/incursions/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    EndpointSpec(re.compile(r"^/industry/facilities/?$"), frozenset({"GET"}), ArchiveType.TIME_SERIES),
    # Status — proxy-only (not archived)
    EndpointSpec(re.compile(r"^/status/?$"), frozenset({"GET"}), ArchiveType.NONE),
]

# Reject paths containing these patterns regardless of allowlist match
_DANGEROUS = re.compile(r"\.\.|%2e|%2f|\\|[\x00-\x1f]|//", re.IGNORECASE)


def validate_path(full_path: str) -> Optional[tuple[str, str]]:
    """
    Validate and split a full ESI path into (version, rest_path).
    Returns None if invalid (dangerous chars, unknown version, etc.).
    full_path example: "/v1/markets/10000002/orders/"
    Returns: ("v1", "/markets/10000002/orders/")
    """
    if _DANGEROUS.search(full_path):
        return None
    parts = full_path.lstrip("/").split("/", 1)
    if len(parts) < 2:
        return None
    version, rest = parts[0], "/" + parts[1]
    if version not in VALID_VERSIONS:
        return None
    return version, rest


def match_endpoint(unversioned_path: str, method: str) -> Optional[EndpointSpec]:
    """Match an unversioned path + method against the allowlist."""
    method = method.upper()
    for spec in _ENDPOINTS:
        if spec.pattern.match(unversioned_path) and method in spec.methods:
            return spec
    return None


def normalize_params(params: dict) -> dict:
    """Remove proxy-internal params and sort for stable hashing. Strip page= (we merge pages)."""
    return {k: v for k, v in sorted(params.items()) if k not in ("page",)}


def compute_query_hash(params: dict, body: bytes | None) -> str:
    """Stable hash of query params + optional POST body for cache key."""
    normalized = normalize_params(params)
    h = hashlib.sha256()
    h.update(json.dumps(normalized, sort_keys=True).encode())
    if body:
        # Normalize JSON body: parse and re-serialize sorted
        try:
            parsed = json.loads(body)
            if isinstance(parsed, list):
                parsed = sorted(parsed)
            h.update(json.dumps(parsed, sort_keys=True).encode())
        except (ValueError, TypeError):
            h.update(body)
    return h.hexdigest()[:16]


def build_cache_key(datasource: str, method: str, full_path: str, params: dict, body: bytes | None) -> str:
    """Build the Redis cache key for a request."""
    query_hash = compute_query_hash(params, body)
    return f"esi:{datasource}:{method.upper()}:{full_path}:{query_hash}"
