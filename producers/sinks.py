from __future__ import annotations

import random
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Protocol, TextIO

from producers.schemas import Envelope


class Sink(Protocol):
    def send(self, env: Envelope, now: datetime) -> None: ...
    def close(self) -> None: ...

class JsonlSink:
    """output/{topic}/dt=YYYY-MM-DD/hour=HH.jsonl  — varış saatine (UTC) göre bölünür."""

    def __init__(self, root: str | Path = "output"):
        self.root = Path(root)
        self._open: dict[str, tuple[Path, TextIO]] = {}
        self.counts: Counter[str] = Counter()

    def _path(self, topic: str, now: datetime) -> Path:
        return self.root / topic / f"dt={now:%Y-%m-%d}" / f"hour={now:%H}.jsonl"

    def send(self, env: Envelope, now: datetime) -> None:
        path = self._path(env.topic, now)
        current = self._open.get(env.topic)
        if current is None or current[0] != path:
            if current is not None:
                current[1].close()
            path.parent.mkdir(parents=True, exist_ok=True)
            current = (path, path.open("a", encoding="utf-8"))
            self._open[env.topic] = current
        current[1].write(env.to_json() + "\n")
        self.counts[env.topic] += 1

    def close(self) -> None:
        for _, f in self._open.values():
            f.close()
        self._open.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

class DuplicatingSink:
    """At-least-once teslimatı taklit eder: mesajların bir kısmı aynı event_id ile
    birkaç yüz mesaj SONRA tekrar gönderilir (yan yana değil)."""

    def __init__(self, inner: Sink, share: float = 0.01, seed: int = 99):
        self.inner = inner
        self.share = share
        self.rng = random.Random(seed)
        self._pending: list[list] = []          # [kalan_mesaj_sayısı, envelope]
        self._last_now: datetime | None = None
        self.duplicates = 0

    def send(self, env: Envelope, now: datetime) -> None:
        self._last_now = now
        self.inner.send(env, now)
        if self.rng.random() < self.share:
            self._pending.append([self.rng.randint(1, 500), env])

        still = []
        for item in self._pending:
            item[0] -= 1
            if item[0] <= 0:
                self.inner.send(item[1], now)
                self.duplicates += 1
            else:
                still.append(item)
        self._pending = still

    def close(self) -> None:
        for _, env in self._pending:
            self.inner.send(env, self._last_now)
            self.duplicates += 1
        self._pending.clear()
        self.inner.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

class KafkaSink:
    def __init__(self, bootstrap: str = "localhost:9092"):
        from confluent_kafka import Producer          # Kafka adımında kurulacak
        self._p = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "zstd",
        })
        self.counts: Counter[str] = Counter()
        self.errors = 0

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.errors += 1

    def send(self, env: Envelope, now: datetime) -> None:
        self._p.produce(env.topic,
                        key=env.key.encode(),
                        value=env.to_json().encode(),
                        timestamp=int(env.produced_at.timestamp() * 1000),
                        on_delivery=self._on_delivery)
        self._p.poll(0)
        self.counts[env.topic] += 1

    def close(self) -> None:
        self._p.flush(30)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

