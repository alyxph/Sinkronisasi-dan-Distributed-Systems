from __future__ import annotations

import time
from typing import Dict, Optional, Tuple


def _label_key(labels: Optional[Dict[str, str]]) -> str:
    if not labels:
        return ""
    parts = [f"{k}={v}" for k, v in sorted(labels.items())]
    return ",".join(parts)


def _metric_key(name: str, labels: Optional[Dict[str, str]]) -> str:
    label_key = _label_key(labels)
    if not label_key:
        return name
    return f"{name}{{{label_key}}}"


class MetricsRegistry:
    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, Tuple[int, float]] = {}

    def inc_counter(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        key = _metric_key(name, labels)
        self.counters[key] = self.counters.get(key, 0.0) + value

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        key = _metric_key(name, labels)
        self.gauges[key] = value

    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        key = _metric_key(name, labels)
        count, total = self.histograms.get(key, (0, 0.0))
        self.histograms[key] = (count + 1, total + value)

    def render_prometheus(self) -> str:
        lines = []
        for key, value in sorted(self.counters.items()):
            lines.append(f"{key} {value}")
        for key, value in sorted(self.gauges.items()):
            lines.append(f"{key} {value}")
        for key, (count, total) in sorted(self.histograms.items()):
            lines.append(f"{key}_count {count}")
            lines.append(f"{key}_sum {total}")
        return "\n".join(lines) + "\n"


class LatencyTimer:
    def __init__(self, registry: MetricsRegistry, metric_name: str, labels: Optional[Dict[str, str]] = None) -> None:
        self.registry = registry
        self.metric_name = metric_name
        self.labels = labels
        self.start_time = 0.0

    def __enter__(self) -> "LatencyTimer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        elapsed = time.perf_counter() - self.start_time
        self.registry.observe(self.metric_name, elapsed, self.labels)


def track_latency(registry: MetricsRegistry, metric_name: str, labels: Optional[Dict[str, str]] = None) -> LatencyTimer:
    return LatencyTimer(registry, metric_name, labels)
