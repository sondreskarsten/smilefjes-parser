"""Point-in-time orgnr resolution: CSV underenhet → hovedenhet.

The smilefjes CSV publishes ``orgnummer`` which is empirically 99.2%
an underenhet (BEDR) and 0.8% an enhet. For the ledger we need the
hovedenhet orgnr — but only if it can be proven at the tilsyn's
``valid_time`` from an immutable dated snapshot.

Resolution rule (strict daily PIT)
----------------------------------
For each tilsyn at ``valid_time = D``:

1. Find the latest ``underenheter/parsed/v2/state/{E}.parquet`` where
   ``E <= D``. If it exists and contains the CSV orgnr with a
   non-null ``overordnet_enhet`` → map::

       hovedenhet = overordnet_enhet
       resolution = "pit_resolved"

2. Else find the latest ``enheter/parsed/v1/state/{E}.parquet`` where
   ``E <= D``. If it contains the CSV orgnr (i.e. the CSV orgnr is
   itself a hovedenhet)::

       hovedenhet = csv_orgnr
       resolution = "pit_resolved_enhet"

3. Else::

       resolution = "unmapped"

Unmapped tilsyn remain in the snapshot but are NOT written to the
ledger.

Memory model
------------
Snapshots are streamed from GCS into memory (pyarrow read of only
the two needed columns) — no local disk cache. Each distinct
snapshot date is fetched once per resolver run, and the orgnrs
needing that snapshot are filtered in a single pass.
"""

import bisect
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from google.cloud import storage


class Resolver:
    """PIT orgnr resolver backed by GCS-hosted dated snapshots.

    Parameters
    ----------
    bucket_name : str
        GCS bucket holding the snapshot parquet files.
    """

    def __init__(self, bucket_name):
        self._bucket_name = bucket_name
        self._gcs = storage.Client()
        self._bucket = self._gcs.bucket(bucket_name)

        self._u_dates = self._list_snapshot_dates("underenheter/parsed/v2/state/")
        self._e_dates = self._list_snapshot_dates("enheter/parsed/v1/state/")

        print(f"  Resolver: underenheter v2 {self._u_dates[0] if self._u_dates else 'NONE'}..{self._u_dates[-1] if self._u_dates else 'NONE'} (n={len(self._u_dates)})", flush=True)
        print(f"  Resolver: enheter v1      {self._e_dates[0] if self._e_dates else 'NONE'}..{self._e_dates[-1] if self._e_dates else 'NONE'} (n={len(self._e_dates)})", flush=True)

    def _list_snapshot_dates(self, prefix):
        """List ISO snapshot dates present under a GCS prefix.

        Parameters
        ----------
        prefix : str
            GCS prefix (without bucket).

        Returns
        -------
        list of str
        """
        dates = []
        for blob in self._bucket.list_blobs(prefix=prefix):
            name = blob.name.split("/")[-1]
            if name.endswith(".parquet"):
                dates.append(name[:-len(".parquet")])
        return sorted(dates)

    def _snapshot_on_or_before(self, target_date, dates):
        """Find latest snapshot date ≤ target.

        Parameters
        ----------
        target_date : str
            ISO target date.
        dates : list of str
            Sorted list of available snapshot ISO dates.

        Returns
        -------
        str or None
        """
        idx = bisect.bisect_right(dates, target_date)
        if idx == 0:
            return None
        return dates[idx - 1]

    def _fetch_orgnr_subset(self, blob_path, orgnr_set, key_col, value_col):
        """Stream a snapshot from GCS and return a filtered subset.

        Downloads the blob into memory, reads only the two needed
        columns, filters to the orgnr set, and returns the rows.

        Parameters
        ----------
        blob_path : str
            GCS blob path (without bucket).
        orgnr_set : set of str
            Orgnrs to filter for.
        key_col : str
            Column name to match against ``orgnr_set``.
        value_col : str or None
            Column name for the resolved value. ``None`` for
            existence-only lookup (enheter case).

        Returns
        -------
        list of tuple
            ``(orgnr, value)`` pairs. For existence-only lookups the
            second element is ``True``.
        """
        blob = self._bucket.blob(blob_path)
        raw = blob.download_as_bytes()
        cols = [key_col] if value_col is None else [key_col, value_col]
        tbl = pq.read_table(io.BytesIO(raw), columns=cols)
        mask = pc.is_in(tbl[key_col], pa.array(sorted(orgnr_set)))
        filtered = tbl.filter(mask)
        if value_col is None:
            return [(o, True) for o in filtered[key_col].to_pylist()]
        return list(zip(filtered[key_col].to_pylist(), filtered[value_col].to_pylist()))

    def resolve_batch(self, pairs):
        """Resolve a batch of ``(orgnr, valid_date)`` pairs.

        Groups pairs by the snapshot date each will use (strict daily
        PIT: the latest snapshot ≤ the tilsyn's dato), fetches each
        distinct snapshot once in memory, filters for the relevant
        orgnrs, and releases.

        Parameters
        ----------
        pairs : list of tuple
            ``(csv_orgnr, iso_date)`` tuples to resolve.

        Returns
        -------
        dict
            ``{(csv_orgnr, iso_date): resolution_dict}``.
        """
        results = {}
        if not pairs:
            return results

        by_u_snap = {}
        for orgnr, d in pairs:
            snap = self._snapshot_on_or_before(d, self._u_dates)
            by_u_snap.setdefault(snap, set()).add(orgnr)

        u_hits = {}
        u_snap_dates = sorted([s for s in by_u_snap if s is not None])
        print(f"  Resolving against {len(u_snap_dates):,} distinct underenheter snapshot dates", flush=True)
        for i, snap in enumerate(u_snap_dates, 1):
            orgnr_set = by_u_snap[snap]
            if i % 50 == 0 or i == len(u_snap_dates):
                print(f"    underenheter snapshot {i}/{len(u_snap_dates)} {snap}: {len(orgnr_set):,} orgnrs", flush=True)
            rows = self._fetch_orgnr_subset(
                f"underenheter/parsed/v2/state/{snap}.parquet",
                orgnr_set,
                "organisasjonsnummer",
                "overordnet_enhet",
            )
            for orgnr, parent in rows:
                if parent is not None:
                    u_hits[(orgnr, snap)] = parent

        for orgnr, d in pairs:
            snap = self._snapshot_on_or_before(d, self._u_dates)
            if snap is not None and (orgnr, snap) in u_hits:
                results[(orgnr, d)] = {
                    "orgnr": u_hits[(orgnr, snap)],
                    "underenhet_orgnr": orgnr,
                    "orgnr_resolution": "pit_resolved",
                    "resolution_source_snapshot": f"underenheter/v2/state/{snap}",
                }

        unresolved_after_u = [(o, d) for (o, d) in pairs if (o, d) not in results]
        by_e_snap = {}
        for orgnr, d in unresolved_after_u:
            snap = self._snapshot_on_or_before(d, self._e_dates)
            by_e_snap.setdefault(snap, set()).add(orgnr)

        e_hits = {}
        e_snap_dates = sorted([s for s in by_e_snap if s is not None])
        if e_snap_dates:
            print(f"  Resolving against {len(e_snap_dates):,} distinct enheter snapshot dates", flush=True)
        for i, snap in enumerate(e_snap_dates, 1):
            orgnr_set = by_e_snap[snap]
            if i % 50 == 0 or i == len(e_snap_dates):
                print(f"    enheter snapshot {i}/{len(e_snap_dates)} {snap}: {len(orgnr_set):,} orgnrs", flush=True)
            rows = self._fetch_orgnr_subset(
                f"enheter/parsed/v1/state/{snap}.parquet",
                orgnr_set,
                "org_nr",
                None,
            )
            for orgnr, _ in rows:
                e_hits[(orgnr, snap)] = True

        for orgnr, d in unresolved_after_u:
            snap = self._snapshot_on_or_before(d, self._e_dates)
            if snap is not None and (orgnr, snap) in e_hits:
                results[(orgnr, d)] = {
                    "orgnr": orgnr,
                    "underenhet_orgnr": None,
                    "orgnr_resolution": "pit_resolved_enhet",
                    "resolution_source_snapshot": f"enheter/v1/state/{snap}",
                }

        for orgnr, d in pairs:
            if (orgnr, d) not in results:
                results[(orgnr, d)] = {
                    "orgnr": None,
                    "underenhet_orgnr": orgnr,
                    "orgnr_resolution": "unmapped",
                    "resolution_source_snapshot": None,
                }

        return results
