"""Parquet-based state management for smilefjes CDC.

Maintains three state files on GCS (synced via the entrypoint)::

    state/tilsyn.parquet        — one row per tilsynid, content-hashed
    state/vurderinger.parquet   — one row per (tilsynid, ordningsverdi)
    state/pool.parquet          — one row per orgnr (as published by source)

Change detection
----------------
Each tilsyn's content_hash is computed over the tilsyn row plus all
its kravpunkter. On ingest:

* tilsynid not in state → ``"new"`` event
* tilsynid in state, hash matches → no event, update ``last_seen``
* tilsynid in state, hash differs → ``"modified"`` event
* tilsynid in state, absent from current CSV → ``"disappeared"`` event

Disappeared events signal Mattilsynet's invalidation policy (the
tilsyn was withdrawn from open data).

The pool is keyed on the CSV orgnr (``orgnummer`` column, the
underenhet in 99.2% of cases). This is deliberately unfiltered — it
records every orgnr Mattilsynet has ever associated with a tilsyn,
regardless of PIT resolvability to a hovedenhet.
"""

import os
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq


TILSYN_SCHEMA = pa.schema([
    ("tilsynid", pa.string()),
    ("tilsynsobjektid", pa.string()),
    ("orgnummer", pa.string()),
    ("navn", pa.string()),
    ("adrlinje1", pa.string()),
    ("adrlinje2", pa.string()),
    ("postnr", pa.string()),
    ("poststed", pa.string()),
    ("sakref", pa.string()),
    ("status", pa.int32()),
    ("dato", pa.string()),
    ("total_karakter", pa.int32()),
    ("tilsynsbesoektype", pa.int32()),
    ("tema_karakter_json", pa.string()),
    ("content_hash", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
])


VURDERINGER_SCHEMA = pa.schema([
    ("tilsynid", pa.string()),
    ("dato", pa.string()),
    ("ordningsverdi", pa.string()),
    ("kravpunktnavn_no", pa.string()),
    ("kravpunktnavn_nn", pa.string()),
    ("karakter", pa.int32()),
    ("tekst_no", pa.string()),
    ("tekst_nn", pa.string()),
])


POOL_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("first_seen", pa.string()),
    ("last_seen", pa.string()),
    ("n_tilsyn", pa.int32()),
])


def now_iso():
    """Return current UTC timestamp in ISO 8601 format.

    Returns
    -------
    str
        UTC ISO 8601 with second precision and ``Z`` suffix.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class StateManager:
    """Local parquet state manager for smilefjes CDC.

    Reads existing state (if any) from a local directory, accepts
    ingestion of a current batch, emits change events, and writes
    updated state back.

    Parameters
    ----------
    state_dir : str
        Local directory containing / destined for the three state
        files.
    """

    def __init__(self, state_dir):
        self.state_dir = state_dir
        os.makedirs(state_dir, exist_ok=True)
        self._tilsyn = self._load_tilsyn()
        self._vurderinger = self._load_vurderinger()
        self._pool = self._load_pool()

    def _load_tilsyn(self):
        """Load tilsyn.parquet as a dict keyed by tilsynid.

        Returns
        -------
        dict
            ``{tilsynid: row_dict}`` or ``{}`` if file missing.
        """
        path = os.path.join(self.state_dir, "tilsyn.parquet")
        if not os.path.exists(path):
            return {}
        tbl = pq.read_table(path)
        return {row["tilsynid"]: row for row in tbl.to_pylist()}

    def _load_vurderinger(self):
        """Load vurderinger.parquet as a list.

        Returns
        -------
        list of dict
            Raw row list, or ``[]`` if file missing.
        """
        path = os.path.join(self.state_dir, "vurderinger.parquet")
        if not os.path.exists(path):
            return []
        return pq.read_table(path).to_pylist()

    def _load_pool(self):
        """Load pool.parquet as a dict keyed by orgnr.

        Returns
        -------
        dict
            ``{orgnr: row_dict}`` or ``{}`` if file missing.
        """
        path = os.path.join(self.state_dir, "pool.parquet")
        if not os.path.exists(path):
            return {}
        tbl = pq.read_table(path)
        return {row["orgnr"]: row for row in tbl.to_pylist()}

    def diff(self, current_tilsyn_by_id, current_hash_by_id, current_kravpunkter_by_tid):
        """Classify every tilsynid in current vs stored state.

        Parameters
        ----------
        current_tilsyn_by_id : dict
            ``{tilsynid: flat_tilsyn_dict}``.
        current_hash_by_id : dict
            ``{tilsynid: content_hash}``.
        current_kravpunkter_by_tid : dict
            ``{tilsynid: [flat_vurdering_dict, ...]}``.

        Returns
        -------
        dict
            Keys ``new``, ``modified``, ``disappeared``, ``unchanged``,
            each a list of tilsynids.
        """
        current_ids = set(current_tilsyn_by_id)
        stored_ids = set(self._tilsyn)

        new_ids = sorted(current_ids - stored_ids)
        disappeared_ids = sorted(stored_ids - current_ids)

        modified_ids = []
        unchanged_ids = []
        for tid in current_ids & stored_ids:
            if current_hash_by_id[tid] != self._tilsyn[tid]["content_hash"]:
                modified_ids.append(tid)
            else:
                unchanged_ids.append(tid)

        return {
            "new": new_ids,
            "modified": sorted(modified_ids),
            "disappeared": disappeared_ids,
            "unchanged": unchanged_ids,
        }

    def apply(self, current_tilsyn_by_id, current_hash_by_id,
              current_kravpunkter_by_tid, diff):
        """Apply changes to in-memory state.

        Updates ``_tilsyn``, ``_vurderinger``, ``_pool`` dicts in place
        to reflect the new, modified, and disappeared tilsyn.

        Parameters
        ----------
        current_tilsyn_by_id : dict
        current_hash_by_id : dict
        current_kravpunkter_by_tid : dict
        diff : dict
            Output of :meth:`diff`.
        """
        import json
        ts = now_iso()

        for tid in diff["disappeared"]:
            del self._tilsyn[tid]
        self._vurderinger = [v for v in self._vurderinger if v["tilsynid"] not in set(diff["disappeared"])]

        for tid in diff["modified"]:
            self._vurderinger = [v for v in self._vurderinger if v["tilsynid"] != tid]

        for tid in diff["new"] + diff["modified"]:
            flat = current_tilsyn_by_id[tid]
            stored_first_seen = (self._tilsyn.get(tid) or {}).get("first_seen") or ts
            self._tilsyn[tid] = {
                "tilsynid": flat["tilsynid"],
                "tilsynsobjektid": flat["tilsynsobjektid"],
                "orgnummer": flat["orgnummer"],
                "navn": flat["navn"],
                "adrlinje1": flat["adrlinje1"],
                "adrlinje2": flat["adrlinje2"],
                "postnr": flat["postnr"],
                "poststed": flat["poststed"],
                "sakref": flat["sakref"],
                "status": flat["status"],
                "dato": flat["dato"],
                "total_karakter": flat["total_karakter"],
                "tilsynsbesoektype": flat["tilsynsbesoektype"],
                "tema_karakter_json": json.dumps(flat["tema_karakter"], sort_keys=True, ensure_ascii=False),
                "content_hash": current_hash_by_id[tid],
                "first_seen": stored_first_seen,
                "last_seen": ts,
            }
            for kp in current_kravpunkter_by_tid.get(tid, []):
                self._vurderinger.append(kp)

        for tid in diff["unchanged"]:
            self._tilsyn[tid]["last_seen"] = ts

        pool_counts = {}
        for row in self._tilsyn.values():
            on = row["orgnummer"]
            if on:
                pool_counts[on] = pool_counts.get(on, 0) + 1

        for orgnr, n in pool_counts.items():
            existing = self._pool.get(orgnr)
            first_seen = existing["first_seen"] if existing else ts
            self._pool[orgnr] = {
                "orgnr": orgnr,
                "first_seen": first_seen,
                "last_seen": ts,
                "n_tilsyn": n,
            }

        stale_orgnrs = set(self._pool) - set(pool_counts)
        for orgnr in stale_orgnrs:
            self._pool[orgnr]["n_tilsyn"] = 0
            self._pool[orgnr]["last_seen"] = ts

    def write(self):
        """Write state dicts to local parquet files."""
        tbl_t = pa.Table.from_pylist(list(self._tilsyn.values()), schema=TILSYN_SCHEMA)
        pq.write_table(tbl_t, os.path.join(self.state_dir, "tilsyn.parquet"), compression="zstd")

        tbl_v = pa.Table.from_pylist(self._vurderinger, schema=VURDERINGER_SCHEMA)
        pq.write_table(tbl_v, os.path.join(self.state_dir, "vurderinger.parquet"), compression="zstd")

        tbl_p = pa.Table.from_pylist(list(self._pool.values()), schema=POOL_SCHEMA)
        pq.write_table(tbl_p, os.path.join(self.state_dir, "pool.parquet"), compression="zstd")

    @property
    def tilsyn(self):
        """Access current in-memory tilsyn state (dict keyed by tilsynid).

        Returns
        -------
        dict
        """
        return self._tilsyn

    @property
    def vurderinger(self):
        """Access current in-memory vurderinger state (list of dicts).

        Returns
        -------
        list of dict
        """
        return self._vurderinger

    @property
    def pool(self):
        """Access current in-memory pool state (dict keyed by orgnr).

        Returns
        -------
        dict
        """
        return self._pool
