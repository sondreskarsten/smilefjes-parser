# smilefjes-parser

Pattern-B CDC parser for Mattilsynet smilefjestilsyn.
LUAS = `(hovedenhet_orgnr, tilsynsobjektid)`.

## Ledger rule

Every row in the unified 12-column changelog has an `orgnr` value
that was proven from an immutable dated source at the tilsyn's
`valid_time`:

1. Look up CSV `orgnummer` in `underenheter/parsed/v2/state/{E}.parquet`
   where `E ≤ tilsyn_dato`. If `overordnet_enhet` is non-null →
   `orgnr = overordnet_enhet`, `orgnr_resolution = "pit_resolved"`.
2. Else look up CSV `orgnummer` in `enheter/parsed/v1/state/{E}.parquet`
   where `E ≤ tilsyn_dato`. If found → `orgnr = csv_orgnummer`,
   `orgnr_resolution = "pit_resolved_enhet"`.
3. Else → `orgnr_resolution = "unmapped"`. Row stays in
   `state/tilsyn.parquet` (authoritative record of what Mattilsynet
   published) but is NOT written to `cdc/changelog/`.

Snapshot coverage (April 2026):
- underenheter v2: 2024-07-02 onward
- enheter v1: 2025-10-21 onward

Pre-2024-07 tilsyn (~36K of 44K total) are all unmapped until dated
historical state becomes available. When that happens, unmapped
tilsyn are retroactively resolvable and will emit supplementary
`resolution_update` events (not implemented yet — see `cdc.py`).

## I/O

Input (read):
```
gs://sondre_brreg_data/smilefjes/raw/tilsyn/{RUN_DATE}.csv.gz
gs://sondre_brreg_data/smilefjes/raw/vurderinger/{RUN_DATE}.csv.gz
gs://sondre_brreg_data/underenheter/parsed/v2/state/{E}.parquet
gs://sondre_brreg_data/enheter/parsed/v1/state/{E}.parquet
```

Input (read + write state):
```
gs://sondre_brreg_data/smilefjes/state/tilsyn.parquet       # ALL tilsynids
gs://sondre_brreg_data/smilefjes/state/vurderinger.parquet  # ALL kravpunkter
gs://sondre_brreg_data/smilefjes/state/pool.parquet         # orgnr pool
```

Output (write):
```
gs://sondre_brreg_data/smilefjes/cdc/changelog/{RUN_DATE}.parquet
```

## Changelog schema (unified 12-col)

| Column | Notes |
|---|---|
| `orgnr` | Hovedenhet (PIT-resolved). Never unmapped. |
| `document_id` | `{tilsynsobjektid}\|{tilsynid}` |
| `data_source` | `smilefjes` |
| `event_type` | `new` / `modified` / `disappeared` / `backfill` |
| `event_subtype` | e.g. `sur_munn_ordinaer`, `stort_smil_oppfolging` |
| `summary` | `{navn} — karakter {N} — {type} — {dato}` |
| `changed_fields` | `["content_hash"]` for modified, else null |
| `valid_time` | tilsyn dato, ISO 8601 UTC midnight |
| `detected_time` | parser run time |
| `details_json` | Full payload: underenhet_orgnr, tema_karakter, kravpunkter[], etc. |
| `source_run_mode` | `daily` / `bootstrap` |
| `run_id` | UUID per parser run |

## Deployment

- Cloud Run Job: `smilefjes-parser`, `europe-north1`, 4 vCPU, 8 GiB,
  timeout 1800s (bootstrap), 600s (daily).
- Schedule: `35 7 * * *` Europe/Oslo (after underenheter-parser at 07:20).
- Image: `europe-north1-docker.pkg.dev/sondreskarsten-d7d14/brreg-pipelines/smilefjes-parser:latest`.

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `GCS_BUCKET` | `sondre_brreg_data` | Target bucket. Empty = local. |
| `GCS_PREFIX` | `smilefjes` | Path prefix. |
| `RUN_MODE` | `daily` | `daily` or `bootstrap`. |
| `RUN_DATE` | today Europe/Oslo | Override raw input date. |
| `STATE_DIR` | `/tmp/smilefjes` | Local scratch. |

## Bootstrap

```
RUN_MODE=bootstrap gcloud run jobs execute smilefjes-parser --region europe-north1 --wait
```

All current tilsyn become `event_type="backfill"` rows with
`source_run_mode="bootstrap"`. Feature generators distinguish
backfill (retroactive ingestion) from daily (real-time detection)
via the `source_run_mode` column.
