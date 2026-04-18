"""Emit unified 12-column changelog from smilefjes CDC decisions.

Writes only PIT-resolved tilsyn to the changelog. Unmapped tilsyn
remain in ``state/tilsyn.parquet`` but do not enter the ledger, per
the platform rule that every ``orgnr`` in the canonical ledger must
be proven from an immutable dated source at ``valid_time``.

Schema is the unified 12-column CDC contract used by enheter, roller,
underenheter, doffin, and aksjeeierbok::

    orgnr, document_id, data_source, event_type, event_subtype,
    summary, changed_fields, valid_time, detected_time, details_json,
    source_run_mode, run_id
"""

import json
import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq


CHANGELOG_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("document_id", pa.string()),
    ("data_source", pa.string()),
    ("event_type", pa.string()),
    ("event_subtype", pa.string()),
    ("summary", pa.string()),
    ("changed_fields", pa.string()),
    ("valid_time", pa.string()),
    ("detected_time", pa.string()),
    ("details_json", pa.string()),
    ("source_run_mode", pa.string()),
    ("run_id", pa.string()),
])


def besoek_label(code):
    """Map tilsynsbesoektype code to a semantic slug.

    Parameters
    ----------
    code : int or None
        Raw code ``0`` = ordinary, ``1`` = follow-up.

    Returns
    -------
    str
    """
    if code == 0:
        return "ordinaer"
    if code == 1:
        return "oppfolging"
    return "ukjent"


def karakter_label(code):
    """Map total_karakter code to a semantic slug.

    Parameters
    ----------
    code : int or None
        Raw code 0-5 per Mattilsynet scale.

    Returns
    -------
    str
    """
    labels = {
        0: "stort_smil",
        1: "stort_smil_mindre_brudd",
        2: "strekmunn",
        3: "sur_munn",
    }
    return labels.get(code, f"karakter_{code}")


def build_changelog_rows(flat_tilsyn, kravpunkter, resolution, event_type,
                        detected_time, source_run_mode, run_id,
                        changed_fields=None):
    """Build one unified 12-col changelog row for a tilsyn event.

    Parameters
    ----------
    flat_tilsyn : dict
        Output of :func:`flatten.flatten_tilsyn`.
    kravpunkter : list of dict
        All :func:`flatten.flatten_vurdering` outputs for this tilsyn.
    resolution : dict
        Output of :meth:`resolve.Resolver.resolve_batch` per pair.
    event_type : str
        One of ``new``, ``modified``, ``disappeared``, ``backfill``.
    detected_time : str
        ISO 8601 UTC timestamp when this event was detected.
    source_run_mode : str
        ``daily``, ``bootstrap``, or ``backfill``.
    run_id : str
        UUID identifying this parser execution.
    changed_fields : list or None
        Only populated for ``modified``.

    Returns
    -------
    dict or None
        Row dict, or ``None`` if unmapped (excluded from changelog).
    """
    if resolution["orgnr_resolution"] == "unmapped":
        return None

    total_k = flat_tilsyn["total_karakter"]
    besoek = flat_tilsyn["tilsynsbesoektype"]
    event_subtype = f"{karakter_label(total_k)}_{besoek_label(besoek)}"

    dato = flat_tilsyn["dato"]
    valid_time = f"{dato}T00:00:00Z" if dato else None

    summary_parts = [
        flat_tilsyn["navn"] or "?",
        f"karakter {total_k}" if total_k is not None else "karakter ?",
        besoek_label(besoek),
    ]
    if dato:
        summary_parts.append(dato)
    summary = " — ".join(str(p) for p in summary_parts)

    details = {
        "tilsynid": flat_tilsyn["tilsynid"],
        "tilsynsobjektid": flat_tilsyn["tilsynsobjektid"],
        "underenhet_orgnr": resolution["underenhet_orgnr"],
        "hovedenhet_orgnr": resolution["orgnr"],
        "orgnr_resolution": resolution["orgnr_resolution"],
        "resolution_source_snapshot": resolution["resolution_source_snapshot"],
        "navn": flat_tilsyn["navn"],
        "adrlinje1": flat_tilsyn["adrlinje1"],
        "adrlinje2": flat_tilsyn["adrlinje2"],
        "postnr": flat_tilsyn["postnr"],
        "poststed": flat_tilsyn["poststed"],
        "sakref": flat_tilsyn["sakref"],
        "status": flat_tilsyn["status"],
        "dato": flat_tilsyn["dato"],
        "total_karakter": total_k,
        "tilsynsbesoektype": flat_tilsyn["tilsynsbesoektype"],
        "tilsynsbesoektype_label": besoek_label(besoek),
        "karakter_label": karakter_label(total_k),
        "tema_karakter": flat_tilsyn["tema_karakter"],
        "kravpunkter": [
            {
                "ordningsverdi": kp["ordningsverdi"],
                "kravpunktnavn": kp["kravpunktnavn_no"],
                "karakter": kp["karakter"],
                "tekst": kp["tekst_no"],
            }
            for kp in sorted(kravpunkter, key=lambda x: x["ordningsverdi"])
        ],
    }

    return {
        "orgnr": resolution["orgnr"],
        "document_id": f"{flat_tilsyn['tilsynsobjektid']}|{flat_tilsyn['tilsynid']}",
        "data_source": "smilefjes",
        "event_type": event_type,
        "event_subtype": event_subtype,
        "summary": summary,
        "changed_fields": json.dumps(changed_fields) if changed_fields else None,
        "valid_time": valid_time,
        "detected_time": detected_time,
        "details_json": json.dumps(details, ensure_ascii=False),
        "source_run_mode": source_run_mode,
        "run_id": run_id,
    }


def build_disappeared_row(stored_tilsyn_row, resolution, detected_time, source_run_mode, run_id):
    """Build a ``disappeared`` changelog row from stored state.

    Disappeared events are only emitted if the tilsyn's original
    ``dato`` can still be PIT-resolved to a hovedenhet — i.e., it
    was in the ledger to begin with. Unmapped tilsyn that disappear
    produce no event (they were never in the ledger). The caller
    re-resolves stored rows against their original ``dato`` before
    invoking this function.

    Parameters
    ----------
    stored_tilsyn_row : dict
        Row from state/tilsyn.parquet being removed.
    resolution : dict
        Fresh resolution for ``(stored_tilsyn_row['orgnummer'],
        stored_tilsyn_row['dato'])``.
    detected_time : str
    source_run_mode : str
    run_id : str

    Returns
    -------
    dict or None
        Unified 12-col row, or ``None`` if the stored row was unmapped.
    """
    if resolution["orgnr_resolution"] == "unmapped":
        return None

    dato = stored_tilsyn_row["dato"]
    valid_time = f"{dato}T00:00:00Z" if dato else None
    return {
        "orgnr": resolution["orgnr"],
        "document_id": f"{stored_tilsyn_row['tilsynsobjektid']}|{stored_tilsyn_row['tilsynid']}",
        "data_source": "smilefjes",
        "event_type": "disappeared",
        "event_subtype": "invalidated",
        "summary": f"{stored_tilsyn_row.get('navn') or '?'} — tilsyn withdrawn from open data",
        "changed_fields": None,
        "valid_time": valid_time,
        "detected_time": detected_time,
        "details_json": json.dumps({
            "tilsynid": stored_tilsyn_row["tilsynid"],
            "tilsynsobjektid": stored_tilsyn_row["tilsynsobjektid"],
            "underenhet_orgnr": resolution["underenhet_orgnr"],
            "hovedenhet_orgnr": resolution["orgnr"],
            "orgnr_resolution": resolution["orgnr_resolution"],
            "resolution_source_snapshot": resolution["resolution_source_snapshot"],
            "first_seen": stored_tilsyn_row["first_seen"],
            "last_seen_before_disappear": stored_tilsyn_row["last_seen"],
        }, ensure_ascii=False),
        "source_run_mode": source_run_mode,
        "run_id": run_id,
    }


def write_changelog(rows, out_path):
    """Write a list of changelog row dicts to parquet.

    Parameters
    ----------
    rows : list of dict
        Rows conforming to ``CHANGELOG_SCHEMA``.
    out_path : str
        Local parquet output path.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tbl = pa.Table.from_pylist(rows, schema=CHANGELOG_SCHEMA)
    pq.write_table(tbl, out_path, compression="zstd")
