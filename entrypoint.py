"""Parser entrypoint: raw CSV.gz → state parquet + unified changelog.

Reads gzipped CSVs from GCS (written by smilefjes-collector), flattens
rows, diffs against stored state via content-hash comparison, resolves
each affected tilsyn's CSV orgnr to a PIT-proven hovedenhet via the
dated underenheter/enheter snapshots, and emits changelog rows for
tilsyn that resolved.

Data flow
---------
1. Download state/{tilsyn,vurderinger,pool}.parquet from GCS (prior).
2. Read raw/{tilsyn,vurderinger}/{RUN_DATE}.csv.gz from GCS.
3. Flatten rows, compute content hashes per tilsynid.
4. Classify new/modified/disappeared/unchanged against stored state.
5. PIT-resolve orgnrs for new/modified (at tilsyn dato) and
   disappeared (at stored dato).
6. Emit unified 12-col changelog for resolved events.
7. Apply diff to state dicts and write updated state parquet.
8. Upload state + changelog to GCS.

Modes
-----
``daily``
    Reads today's CSV files. Emits ``new``/``modified``/``disappeared``
    events with ``source_run_mode="daily"``.

``bootstrap``
    Same logic as daily but with an empty starting state (all current
    tilsyn are treated as ``new`` but tagged ``source_run_mode="bootstrap"``).
    Run once on fresh deploy.

Environment variables
---------------------
GCS_BUCKET : str
    Default ``sondre_brreg_data``. Empty = local-only.
GCS_PREFIX : str
    Default ``smilefjes``.
RUN_MODE : str
    ``daily`` or ``bootstrap``. Default ``daily``.
RUN_DATE : str
    ISO date used for raw input selection and changelog filename.
    Default: today Europe/Oslo.
STATE_DIR : str
    Local scratch directory. Default ``/tmp/smilefjes``.
"""

import csv
import gzip
import io
import os
import sys
import uuid
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from google.cloud import storage

from flatten import flatten_tilsyn, flatten_vurdering, content_hash
from resolve import Resolver
from state import StateManager, now_iso
from cdc import build_changelog_rows, build_disappeared_row, write_changelog


GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "smilefjes")
RUN_MODE = os.environ.get("RUN_MODE", "daily")
RUN_DATE = os.environ.get("RUN_DATE") or datetime.now(ZoneInfo("Europe/Oslo")).date().isoformat()
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/smilefjes")


def gcs_client():
    """Return a GCS storage client using ADC.

    Returns
    -------
    google.cloud.storage.Client
    """
    return storage.Client()


def sync_state_from_gcs(bucket):
    """Download the three state parquet files from GCS to STATE_DIR.

    Skips any file that doesn't exist on GCS (e.g., first run).

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    """
    os.makedirs(STATE_DIR, exist_ok=True)
    for fname in ("tilsyn.parquet", "vurderinger.parquet", "pool.parquet"):
        blob = bucket.blob(f"{GCS_PREFIX}/state/{fname}")
        local = os.path.join(STATE_DIR, fname)
        if blob.exists():
            blob.download_to_filename(local)
            print(f"  Downloaded state/{fname} ({os.path.getsize(local):,} bytes)", flush=True)


def sync_state_to_gcs(bucket, changelog_path):
    """Upload state + changelog from STATE_DIR to GCS.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    changelog_path : str
        Local path of the changelog parquet to upload.
    """
    for fname in ("tilsyn.parquet", "vurderinger.parquet", "pool.parquet"):
        local = os.path.join(STATE_DIR, fname)
        if os.path.exists(local):
            blob = bucket.blob(f"{GCS_PREFIX}/state/{fname}")
            blob.upload_from_filename(local)
            print(f"  Uploaded state/{fname} ({os.path.getsize(local):,} bytes)", flush=True)
    if os.path.exists(changelog_path):
        blob = bucket.blob(f"{GCS_PREFIX}/cdc/changelog/{RUN_DATE}.parquet")
        blob.upload_from_filename(changelog_path)
        print(f"  Uploaded cdc/changelog/{RUN_DATE}.parquet ({os.path.getsize(changelog_path):,} bytes)", flush=True)


def read_raw_csv(bucket, dataset):
    """Stream a gzipped CSV from GCS as a list of dicts.

    Parameters
    ----------
    bucket : google.cloud.storage.Bucket
    dataset : str
        ``"tilsyn"`` or ``"vurderinger"``.

    Returns
    -------
    list of dict
    """
    blob_path = f"{GCS_PREFIX}/raw/{dataset}/{RUN_DATE}.csv.gz"
    blob = bucket.blob(blob_path)
    raw = blob.download_as_bytes()
    body = gzip.decompress(raw)
    reader = csv.DictReader(io.StringIO(body.decode("utf-8")), delimiter=";")
    rows = list(reader)
    print(f"  Read {dataset} ({len(rows):,} rows, {len(body):,} decompressed bytes)", flush=True)
    return rows


def main():
    """Orchestrate one parser run (daily or bootstrap)."""
    run_id = str(uuid.uuid4())
    print(f"=== smilefjes-parser run_mode={RUN_MODE} run_date={RUN_DATE} run_id={run_id} ===", flush=True)

    client = gcs_client()
    bucket = client.bucket(GCS_BUCKET)

    if RUN_MODE != "bootstrap":
        sync_state_from_gcs(bucket)
    else:
        for fname in ("tilsyn.parquet", "vurderinger.parquet", "pool.parquet"):
            local = os.path.join(STATE_DIR, fname)
            if os.path.exists(local):
                os.unlink(local)

    state = StateManager(STATE_DIR)
    print(f"  Loaded state: tilsyn={len(state.tilsyn):,}  vurderinger={len(state.vurderinger):,}  pool={len(state.pool):,}", flush=True)

    tilsyn_rows_raw = read_raw_csv(bucket, "tilsyn")
    vurderinger_rows_raw = read_raw_csv(bucket, "vurderinger")

    flat_tilsyn = {}
    for r in tilsyn_rows_raw:
        ft = flatten_tilsyn(r)
        flat_tilsyn[ft["tilsynid"]] = ft

    flat_vurd_by_tid = defaultdict(list)
    for r in vurderinger_rows_raw:
        fv = flatten_vurdering(r)
        if fv["tilsynid"] in flat_tilsyn:
            flat_vurd_by_tid[fv["tilsynid"]].append(fv)

    ghost_vurd_tids = set(r["tilsynid"] for r in vurderinger_rows_raw) - set(flat_tilsyn)
    print(f"  Flattened: {len(flat_tilsyn):,} tilsyn / {sum(len(v) for v in flat_vurd_by_tid.values()):,} vurderinger ({len(ghost_vurd_tids):,} ghost vurd tilsynids discarded)", flush=True)

    hash_by_tid = {}
    for tid, ft in flat_tilsyn.items():
        hash_by_tid[tid] = content_hash(ft, flat_vurd_by_tid.get(tid, []))

    diff = state.diff(flat_tilsyn, hash_by_tid, flat_vurd_by_tid)
    print(f"  Diff: new={len(diff['new']):,}  modified={len(diff['modified']):,}  disappeared={len(diff['disappeared']):,}  unchanged={len(diff['unchanged']):,}", flush=True)

    resolver = Resolver(GCS_BUCKET)

    pairs_emit = []
    for tid in diff["new"] + diff["modified"]:
        ft = flat_tilsyn[tid]
        if ft["orgnummer"] and ft["dato"]:
            pairs_emit.append((ft["orgnummer"], ft["dato"]))

    pairs_disappeared = []
    for tid in diff["disappeared"]:
        stored = state.tilsyn[tid]
        if stored.get("orgnummer") and stored.get("dato"):
            pairs_disappeared.append((stored["orgnummer"], stored["dato"]))

    all_pairs = sorted(set(pairs_emit + pairs_disappeared))
    print(f"  Resolving {len(all_pairs):,} unique (orgnr, dato) pairs...", flush=True)
    resolutions = resolver.resolve_batch(all_pairs) if all_pairs else {}

    resolved_count = sum(1 for r in resolutions.values() if r["orgnr_resolution"] != "unmapped")
    unmapped_count = sum(1 for r in resolutions.values() if r["orgnr_resolution"] == "unmapped")
    print(f"  Resolution: pit_resolved={resolved_count:,}  unmapped={unmapped_count:,}", flush=True)

    detected_time = now_iso()
    source_run_mode = "bootstrap" if RUN_MODE == "bootstrap" else "daily"
    changelog_rows = []

    for tid in diff["new"]:
        ft = flat_tilsyn[tid]
        key = (ft["orgnummer"], ft["dato"])
        res = resolutions.get(key, {"orgnr": None, "underenhet_orgnr": ft["orgnummer"], "orgnr_resolution": "unmapped", "resolution_source_snapshot": None})
        row = build_changelog_rows(
            ft, flat_vurd_by_tid.get(tid, []), res,
            event_type="backfill" if source_run_mode == "bootstrap" else "new",
            detected_time=detected_time,
            source_run_mode=source_run_mode,
            run_id=run_id,
        )
        if row:
            changelog_rows.append(row)

    for tid in diff["modified"]:
        ft = flat_tilsyn[tid]
        key = (ft["orgnummer"], ft["dato"])
        res = resolutions.get(key, {"orgnr": None, "underenhet_orgnr": ft["orgnummer"], "orgnr_resolution": "unmapped", "resolution_source_snapshot": None})
        row = build_changelog_rows(
            ft, flat_vurd_by_tid.get(tid, []), res,
            event_type="modified",
            detected_time=detected_time,
            source_run_mode=source_run_mode,
            run_id=run_id,
            changed_fields=["content_hash"],
        )
        if row:
            changelog_rows.append(row)

    for tid in diff["disappeared"]:
        stored = state.tilsyn[tid]
        key = (stored.get("orgnummer"), stored.get("dato"))
        res = resolutions.get(key, {"orgnr": None, "underenhet_orgnr": stored.get("orgnummer"), "orgnr_resolution": "unmapped", "resolution_source_snapshot": None})
        row = build_disappeared_row(stored, res, detected_time, source_run_mode, run_id)
        if row:
            changelog_rows.append(row)

    by_event_type = defaultdict(int)
    for r in changelog_rows:
        by_event_type[r["event_type"]] += 1
    print(f"  Changelog rows: {len(changelog_rows):,} total  {dict(by_event_type)}", flush=True)

    state.apply(flat_tilsyn, hash_by_tid, flat_vurd_by_tid, diff)
    state.write()
    print(f"  Applied to state: tilsyn={len(state.tilsyn):,}  vurderinger={len(state.vurderinger):,}  pool={len(state.pool):,}", flush=True)

    changelog_path = os.path.join(STATE_DIR, f"changelog_{RUN_DATE}.parquet")
    write_changelog(changelog_rows, changelog_path)
    print(f"  Wrote local changelog ({os.path.getsize(changelog_path):,} bytes)", flush=True)

    if GCS_BUCKET:
        sync_state_to_gcs(bucket, changelog_path)

    print(f"=== done ===", flush=True)


if __name__ == "__main__":
    main()
