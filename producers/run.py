"""E-ticaret event simülatörü.

Örnekler:
  python -m producers.run --backfill 30d
  python -m producers.run --backfill 7d --live
  python -m producers.run --live --sink kafka
"""
from __future__ import annotations

import argparse
import logging
import re
import shutil
import sys
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from producers.catalog import Catalog
from producers.generators import Generator
from producers.schemas import Envelope, utc_now
from producers.sinks import DuplicatingSink, JsonlSink, KafkaSink

log = logging.getLogger("simulator")

# Saat başına olasılıklar / oranlar
PRICE_CHANGE_RATE = 0.01        # ilanların saatte ~%1'inin fiyatı değişir
RECATEGORIZE_PER_HOUR = 0.05
ONBOARD_PER_HOUR = 0.05
DEACTIVATE_PER_HOUR = 0.01

def parse_duration(text: str) -> timedelta:
    m = re.fullmatch(r"(\d+)([dhm])", text.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"süre '30d', '12h' veya '15m' gibi olmalı: {text}")
    n, unit = int(m.group(1)), m.group(2)
    return {"d": timedelta(days=n), "h": timedelta(hours=n), "m": timedelta(minutes=n)}[unit]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="E-ticaret event simülatörü")

    mode = p.add_argument_group("çalışma modu")
    mode.add_argument("--backfill", type=parse_duration, help="geçmişe dönük üret: 30d, 12h")
    mode.add_argument("--live", action="store_true", help="gerçek zamanlı üret (Ctrl+C ile durur)")
    mode.add_argument("--step", type=parse_duration, default=timedelta(minutes=10),
                      help="backfill tick aralığı (en fazla 1h)")
    mode.add_argument("--live-interval", type=float, default=5.0,
                      help="live modda tick'ler arası saniye")

    out = p.add_argument_group("çıktı")
    out.add_argument("--sink", choices=["jsonl", "kafka"], default="jsonl")
    out.add_argument("--out", type=Path, default=Path("output"))
    out.add_argument("--overwrite", action="store_true", help="çıktı klasörü doluysa önce sil")
    out.add_argument("--bootstrap-servers", default="localhost:9092")
    out.add_argument("--duplicates", type=float, default=0.01,
                     help="gecikmeli tekrar oranı (0 = kapalı)")

    world = p.add_argument_group("simülasyon")
    world.add_argument("--seed", type=int, default=42)
    world.add_argument("--products", type=int, default=200)
    world.add_argument("--sellers", type=int, default=40)
    world.add_argument("--customers", type=int, default=2000)
    world.add_argument("--orders-per-hour", type=float, default=40)
    world.add_argument("--browse-ratio", type=float, default=10)
    world.add_argument("--v2-cutover-ago", type=parse_duration, default=timedelta(days=10),
                       help="bundan eski mesajlar schema v1 olarak üretilir")
    return p

def world_tick(cat: Catalog, gen: Generator, now: datetime, step: timedelta) -> list[Envelope]:
    hours = step / timedelta(hours=1)
    msgs: list[Envelope] = []

    expected_changes = len(cat.listings) * PRICE_CHANGE_RATE * hours
    if expected_changes >= 1 or cat.rng.random() < expected_changes:
        msgs += cat.change_prices(now, share=PRICE_CHANGE_RATE * hours)

    if cat.rng.random() < RECATEGORIZE_PER_HOUR * hours:
        msgs += cat.recategorize(now)
    if cat.rng.random() < ONBOARD_PER_HOUR * hours:
        msgs += cat.onboard(now)
    if cat.rng.random() < DEACTIVATE_PER_HOUR * hours:
        msgs += cat.deactivate(now)

    msgs += gen.tick(now, step)
    msgs += cat.restock(now)
    return msgs

def make_sink(args: argparse.Namespace):
    if args.sink == "kafka":
        sink = KafkaSink(args.bootstrap_servers)
    else:
        if args.out.exists() and any(args.out.rglob("*.jsonl")):
            if not args.overwrite:
                sys.exit(f"'{args.out}' klasörü dolu. Her çalıştırma ID'leri baştan üretir "
                         f"(O-0000001...), üzerine eklemek çakışan ID'ler yaratır. "
                         f"Silmek için --overwrite kullan.")
            shutil.rmtree(args.out)
        sink = JsonlSink(args.out)

    if args.duplicates > 0:
        return DuplicatingSink(sink, share=args.duplicates, seed=args.seed + 1)
    return sink

def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.backfill and not args.live:
        parser.error("--backfill veya --live seçmelisin")
    if args.step > timedelta(hours=1):
        parser.error("--step en fazla 1h olabilir")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")

    real_now = utc_now()
    start = real_now - (args.backfill or timedelta(0))
    cat = Catalog(v2_cutover=real_now - args.v2_cutover_ago, seed=args.seed)
    gen = Generator(cat, n_customers=args.customers, seed=args.seed + 7,
                    orders_per_hour=args.orders_per_hour, browse_ratio=args.browse_ratio)
    counts: Counter[str] = Counter()

    def send_all(sink, msgs: list[Envelope], now: datetime) -> None:
        for env in msgs:
            sink.send(env, now)
            counts[env.topic] += 1

    with make_sink(args) as sink:
        send_all(sink, cat.bootstrap(start, args.products, args.sellers), start)
        log.info("katalog hazır: %d ürün, %d ilan", len(cat.products), len(cat.listings))

        clock = start
        if args.backfill:
            next_report = clock + timedelta(days=1)
            while clock + args.step <= real_now:
                clock += args.step
                send_all(sink, world_tick(cat, gen, clock, args.step), clock)
                if clock >= next_report:
                    log.info("backfill  %s  %s", f"{clock:%m-%d %H:%M}", dict(counts))
                    next_report += timedelta(days=1)
            log.info("backfill bitti")

        if args.live:
            log.info("live mod başladı, durdurmak için Ctrl+C")
            last_log = time.monotonic()
            try:
                while True:
                    time.sleep(args.live_interval)
                    now = utc_now()
                    step, clock = now - clock, now
                    send_all(sink, world_tick(cat, gen, now, step), now)
                    if time.monotonic() - last_log >= 60:
                        log.info("live  %s", dict(counts))
                        last_log = time.monotonic()
            except KeyboardInterrupt:
                log.info("durduruluyor, açık dosyalar kapatılıyor...")

    log.info("toplam: %s  | gecikmeli tekrar: %d", dict(counts), getattr(sink, "duplicates", 0))


if __name__ == "__main__":
    main()

