"""
Lightweight in-process metrics with Prometheus text exposition.

Dependency-free: a small registry of counters, gauges, and fixed-bucket
histograms, rendered at GET /metrics. Label cardinality is the caller's
responsibility — use matched endpoint patterns, never raw paths.

A single module-level `metrics` instance is shared across the app.
"""
import threading
from collections import defaultdict
from typing import Union

Number = Union[int, float]

# Buckets for request-latency histograms (seconds).
_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


def _key(labels: dict) -> tuple:
    return tuple(sorted(labels.items()))


def _num(value: Number) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return repr(value)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _fmt_labels(labels: tuple) -> str:
    if not labels:
        return ""
    return "{" + ",".join(f'{k}="{_escape(str(v))}"' for k, v in labels) + "}"


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple, float] = defaultdict(float)
        self._gauges: dict[tuple, float] = {}
        self._histograms: dict[tuple, dict] = {}
        self._meta: dict[str, tuple[str, str]] = {}  # name -> (type, help)

    def _register(self, name: str, metric_type: str, help_text: str) -> None:
        self._meta.setdefault(name, (metric_type, help_text))

    def inc_counter(self, name: str, value: Number = 1.0, help: str = "", **labels) -> None:
        self._register(name, "counter", help)
        with self._lock:
            self._counters[(name, _key(labels))] += value

    def set_gauge(self, name: str, value: Number, help: str = "", **labels) -> None:
        self._register(name, "gauge", help)
        with self._lock:
            self._gauges[(name, _key(labels))] = float(value)

    def observe(self, name: str, value: Number, help: str = "", **labels) -> None:
        self._register(name, "histogram", help)
        with self._lock:
            hist = self._histograms.get((name, _key(labels)))
            if hist is None:
                hist = {"buckets": [0] * len(_LATENCY_BUCKETS), "sum": 0.0, "count": 0}
                self._histograms[(name, _key(labels))] = hist
            hist["sum"] += value
            hist["count"] += 1
            for i, bound in enumerate(_LATENCY_BUCKETS):
                if value <= bound:
                    hist["buckets"][i] += 1

    # -- read helpers (tests / ops) ------------------------------------------

    def counter_value(self, name: str, **labels) -> float:
        with self._lock:
            return self._counters.get((name, _key(labels)), 0.0)

    def gauge_value(self, name: str, **labels) -> float:
        with self._lock:
            return self._gauges.get((name, _key(labels)), 0.0)

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()

    # -- exposition ----------------------------------------------------------

    def render(self) -> str:
        out: list[str] = []
        with self._lock:
            for name, samples in sorted(_group(self._counters).items()):
                out += self._header(name, "counter")
                for labels, value in sorted(samples):
                    out.append(f"{name}{_fmt_labels(labels)} {_num(value)}")
            for name, samples in sorted(_group(self._gauges).items()):
                out += self._header(name, "gauge")
                for labels, value in sorted(samples):
                    out.append(f"{name}{_fmt_labels(labels)} {_num(value)}")
            for name, samples in sorted(_group_hist(self._histograms).items()):
                out += self._header(name, "histogram")
                for labels, hist in sorted(samples, key=lambda item: item[0]):
                    for i, bound in enumerate(_LATENCY_BUCKETS):
                        le = labels + (("le", _num(bound)),)
                        out.append(f"{name}_bucket{_fmt_labels(le)} {hist['buckets'][i]}")
                    inf = labels + (("le", "+Inf"),)
                    out.append(f"{name}_bucket{_fmt_labels(inf)} {hist['count']}")
                    out.append(f"{name}_sum{_fmt_labels(labels)} {_num(hist['sum'])}")
                    out.append(f"{name}_count{_fmt_labels(labels)} {hist['count']}")
        return "\n".join(out) + "\n"

    def _header(self, name: str, default_type: str) -> list[str]:
        metric_type, help_text = self._meta.get(name, (default_type, ""))
        return [f"# HELP {name} {help_text}", f"# TYPE {name} {metric_type}"]


def _group(flat: dict) -> dict:
    grouped: dict = defaultdict(list)
    for (name, labels), value in flat.items():
        grouped[name].append((labels, value))
    return grouped


def _group_hist(flat: dict) -> dict:
    grouped: dict = defaultdict(list)
    for (name, labels), hist in flat.items():
        grouped[name].append((labels, hist))
    return grouped


metrics = MetricsRegistry()
