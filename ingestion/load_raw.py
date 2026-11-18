"""output/ altındaki JSONL dosyalarını BigQuery raw tablolarına load job ile yükler.

Örnek:
  python -m ingestion.load_raw --project ecommerce-pipeline-ersu
  python -m ingestion.load_raw --project ecommerce-pipeline-ersu --dry-run
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

log = logging.getLogger("loader")

TOPICS = ["products", "listings", "orders", "events"]
PATH_RE = re.compile(r"(?P<topic>\w+)/dt=(?P<dt>\d{4}-\d{2}-\d{2})/hour=(?P<hour>\d{2})\.jsonl")
MAX_BATCH_BYTES = 64 * 1024 * 1024        # tek load job'a giden kaynak veri üst sınırı

RAW_SCHEMA = [
    bigquery.SchemaField("raw_json", "STRING", mode="REQUIRED",
                         description="Envelope, geldiği gibi (parse edilmemiş)"),
    bigquery.SchemaField("_landed_at", "TIMESTAMP", mode="REQUIRED",
                         description="Landing zone'a varış saati (dosya yolundan)"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED",
                         description="BigQuery'ye yüklendiği an"),
    bigquery.SchemaField("_source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_line_no", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("_kafka_partition", "INT64"),
    bigquery.SchemaField("_kafka_offset", "INT64"),
]

MANIFEST_SCHEMA = [
    bigquery.SchemaField("source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("topic", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("size_bytes", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("row_count", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

@dataclass(frozen=True)
class LandingFile:
    path: Path
    rel: str                 # "orders/dt=2026-11-18/hour=21.jsonl"
    topic: str
    landed_at: datetime
    size: int


def discover(root: Path) -> list[LandingFile]:
    files = []
    for p in sorted(root.rglob("*.jsonl")):
        rel = p.relative_to(root).as_posix()
        m = PATH_RE.fullmatch(rel)
        if not m or m["topic"] not in TOPICS:
            log.warning("beklenmeyen yol, atlandı: %s", rel)
            continue
        landed = datetime.strptime(f"{m['dt']} {m['hour']}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
        files.append(LandingFile(p, rel, m["topic"], landed, p.stat().st_size))
    return files

def ensure_tables(client: bigquery.Client, dataset: str) -> None:
    for topic in TOPICS:
        t = bigquery.Table(f"{dataset}.{topic}", schema=RAW_SCHEMA)
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="_landed_at")
        t.description = f"Ham '{topic}' mesajları. Dönüştürülmemiş, tekrar içerebilir."
        client.create_table(t, exists_ok=True)
    client.create_table(bigquery.Table(f"{dataset}._load_manifest", schema=MANIFEST_SCHEMA),
                        exists_ok=True)


def already_loaded(client: bigquery.Client, dataset: str) -> dict[str, int]:
    sql = f"SELECT source_file, MAX(size_bytes) AS size_bytes FROM `{dataset}._load_manifest` GROUP BY 1"
    return {r.source_file: r.size_bytes for r in client.query(sql).result()}

def ensure_tables(client: bigquery.Client, dataset: str) -> None:
    for topic in TOPICS:
        t = bigquery.Table(f"{dataset}.{topic}", schema=RAW_SCHEMA)
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="_landed_at")
        t.description = f"Ham '{topic}' mesajları. Dönüştürülmemiş, tekrar içerebilir."
        client.create_table(t, exists_ok=True)
    client.create_table(bigquery.Table(f"{dataset}._load_manifest", schema=MANIFEST_SCHEMA),
                        exists_ok=True)


def already_loaded(client: bigquery.Client, dataset: str) -> dict[str, int]:
    sql = f"SELECT source_file, MAX(size_bytes) AS size_bytes FROM `{dataset}._load_manifest` GROUP BY 1"
    return {r.source_file: r.size_bytes for r in client.query(sql).result()}

def batches(files: list[LandingFile], max_bytes: int):
    batch, size = [], 0
    for f in files:
        if batch and size + f.size > max_bytes:
            yield batch
            batch, size = [], 0
        batch.append(f)
        size += f.size
    if batch:
        yield batch


def to_ndjson(files: list[LandingFile], ingested_at: datetime) -> tuple[bytes, dict[str, int]]:
    buf, counts = io.StringIO(), {}
    for f in files:
        n = 0
        with f.path.open(encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.rstrip("\n")
                if not line:
                    continue
                buf.write(json.dumps({
                    "raw_json": line,
                    "_landed_at": f.landed_at.isoformat(),
                    "_ingested_at": ingested_at.isoformat(),
                    "_source_file": f.rel,
                    "_line_no": line_no,
                }, ensure_ascii=False))
                buf.write("\n")
                n += 1
        counts[f.rel] = n
    return buf.getvalue().encode("utf-8"), counts


def load_batch(client: bigquery.Client, table_id: str, data: bytes) -> bigquery.LoadJob:
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=RAW_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_file(io.BytesIO(data), table_id, job_config=cfg)
    job.result()                                   # bitene kadar bekle, hata varsa fırlatır
    return job


def record_manifest(client: bigquery.Client, dataset: str, rows: list[dict]) -> None:
    cfg = bigquery.LoadJobConfig(schema=MANIFEST_SCHEMA,
                                 write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    client.load_table_from_json(rows, f"{dataset}._load_manifest", job_config=cfg).result()

def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Landing zone (JSONL) → BigQuery raw")
    p.add_argument("--project", required=True)
    p.add_argument("--dataset", default="raw")
    p.add_argument("--location", default="EU")
    p.add_argument("--root", type=Path, default=Path("output"))
    p.add_argument("--include-open-hour", action="store_true",
                   help="içinde bulunulan saatin (hâlâ yazılıyor olabilir) dosyalarını da yükle")
    p.add_argument("--dry-run", action="store_true", help="sadece ne yükleneceğini göster")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")

    client = bigquery.Client(project=args.project, location=args.location)
    dataset = f"{args.project}.{args.dataset}"
    ensure_tables(client, dataset)
    loaded = already_loaded(client, dataset)

    open_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    todo, skipped_open = [], 0
    for f in discover(args.root):
        if f.rel in loaded:
            if loaded[f.rel] != f.size:
                log.warning("yüklendikten sonra değişmiş, atlandı: %s", f.rel)
            continue
        if f.landed_at >= open_hour and not args.include_open_hour:
            skipped_open += 1
            continue
        todo.append(f)

    log.info("yüklenecek: %d dosya | daha önce yüklenmiş: %d | açık saat (atlandı): %d",
             len(todo), len(loaded), skipped_open)
    if args.dry_run or not todo:
        return

    for topic in TOPICS:
        for batch in batches([f for f in todo if f.topic == topic], MAX_BATCH_BYTES):
            ingested_at = datetime.now(timezone.utc)
            data, counts = to_ndjson(batch, ingested_at)
            job = load_batch(client, f"{dataset}.{topic}", data)
            record_manifest(client, dataset, [
                {"source_file": f.rel, "topic": topic, "size_bytes": f.size,
                 "row_count": counts[f.rel], "job_id": job.job_id,
                 "loaded_at": ingested_at.isoformat()}
                for f in batch])
            log.info("%-9s %4d dosya  %8d satır  → %s", topic, len(batch), sum(counts.values()), job.job_id)


if __name__ == "__main__":
    main()