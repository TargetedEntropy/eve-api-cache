"""Unit tests for the per-endpoint query-param allowlist (fix 4.2)."""
from app.allowlist import check_params, endpoint_label, match_endpoint


def _spec(path: str, method: str = "GET"):
    spec = match_endpoint(path, method)
    assert spec is not None, f"no allowlist entry for {method} {path}"
    return spec


def test_allows_known_params_and_proxy_params():
    spec = _spec("/markets/10000002/orders/")
    assert check_params(spec, {"order_type": "all"}) is None
    # page + datasource are proxy-managed and always allowed
    assert check_params(spec, {"order_type": "all", "type_id": "34", "page": "2", "datasource": "tranquility"}) is None


def test_rejects_unknown_param():
    spec = _spec("/markets/10000002/orders/")
    err = check_params(spec, {"order_type": "all", "junk": "1"})
    assert err is not None
    assert "junk" in err


def test_missing_required_param():
    spec = _spec("/markets/10000002/history/")
    err = check_params(spec, {})
    assert err is not None
    assert "type_id" in err


def test_required_param_satisfied():
    spec = _spec("/markets/10000002/history/")
    assert check_params(spec, {"type_id": "34"}) is None


def test_history_extra_param_rejected_even_with_required():
    spec = _spec("/markets/10000002/history/")
    err = check_params(spec, {"type_id": "34", "order_type": "all"})
    assert err is not None
    assert "order_type" in err


def test_empty_allowlist_rejects_extras_but_permits_proxy_params():
    spec = _spec("/status/")
    assert check_params(spec, {}) is None
    assert check_params(spec, {"datasource": "singularity"}) is None
    assert check_params(spec, {"x": "1"}) is not None


def test_language_allowed_on_localizable_universe_endpoint():
    spec = _spec("/universe/types/34/")
    assert check_params(spec, {"language": "en"}) is None
    assert check_params(spec, {"foo": "bar"}) is not None


# ---------------------------------------------------------------------------
# Bounded endpoint labels for metrics (fix 5.1)
# ---------------------------------------------------------------------------

def test_endpoint_label_is_a_bounded_template():
    assert endpoint_label(_spec("/markets/10000002/orders/")) == "/markets/{id}/orders/"
    assert endpoint_label(_spec("/markets/10000002/history/")) == "/markets/{id}/history/"
    assert endpoint_label(_spec("/status/")) == "/status/"
    assert endpoint_label(_spec("/universe/types/34/")) == "/universe/types/{id}/"
    km = _spec("/killmails/1/" + "a" * 40 + "/")
    assert endpoint_label(km) == "/killmails/{id}/{hash}/"


def test_endpoint_label_collapses_distinct_ids():
    """Different IDs on the same route share one label (bounds metric cardinality)."""
    a = endpoint_label(_spec("/markets/10000002/orders/"))
    b = endpoint_label(_spec("/markets/10000043/orders/"))
    assert a == b == "/markets/{id}/orders/"
