"""CSV row → flat typed dict.

Handles the two Mattilsynet CSV schemas and normalizes their
`ddmmyyyy` date format to ISO `YYYY-MM-DD`. Integers are coerced
where the CSV uses them as codes, strings otherwise.

Mattilsynet score codes (karakter)::

    0 = Ingen brudd på regelverket funnet (stort smil)
    1 = Mindre brudd som ikke krever oppfølging (stort smil)
    2 = Brudd som krever oppfølging (strekmunn)
    3 = Alvorlig brudd (sur munn)
    4 = Ikke aktuelt (påvirker ikke smilefjeskarakter)
    5 = Ikke vurdert (påvirker ikke smilefjeskarakter)

tilsynsbesoektype::

    0 = ordinary
    1 = follow-up
"""

import hashlib
import json


TEMA_SLUGS = {
    "Rutiner og ledelse": "rutiner_og_ledelse",
    "Lokaler og utstyr": "lokaler_og_utstyr",
    "Mathåndtering og tilberedning": "mathandtering_og_tilberedning",
    "Merking og sporbarhet": "merking_og_sporbarhet",
}


def parse_dato(s):
    """Convert `ddmmyyyy` to ISO `YYYY-MM-DD`.

    Parameters
    ----------
    s : str
        Date string in `ddmmyyyy` format.

    Returns
    -------
    str or None
        ISO date, or ``None`` if empty.
    """
    if not s or len(s) != 8:
        return None
    return f"{s[4:8]}-{s[2:4]}-{s[0:2]}"


def slugify_tema(name):
    """Map Bokmål tema name to stable lowercase slug.

    Parameters
    ----------
    name : str
        Raw tema name.

    Returns
    -------
    str
        Slug, or the lowercased original if not in the canonical map.
    """
    return TEMA_SLUGS.get(name, name.lower().replace(" ", "_"))


def flatten_tilsyn(row):
    """Flatten a tilsyn.csv row to a typed dict.

    Parameters
    ----------
    row : dict
        Raw CSV row (all values strings).

    Returns
    -------
    dict
        Flat dict with typed values and ISO dates.
    """
    tema_karakter = {}
    for i in (1, 2, 3, 4):
        name = row.get(f"tema{i}_no") or ""
        karakter = row.get(f"karakter{i}")
        if name:
            tema_karakter[slugify_tema(name)] = int(karakter) if karakter != "" else None

    total_karakter = row.get("total_karakter")
    tilsynsbesoektype = row.get("tilsynsbesoektype")
    return {
        "tilsynid": row["tilsynid"],
        "tilsynsobjektid": row["tilsynsobjektid"],
        "orgnummer": row.get("orgnummer") or None,
        "navn": row.get("navn") or None,
        "adrlinje1": row.get("adrlinje1") or None,
        "adrlinje2": row.get("adrlinje2") or None,
        "postnr": row.get("postnr") or None,
        "poststed": row.get("poststed") or None,
        "sakref": row.get("sakref") or None,
        "status": int(row["status"]) if row.get("status") else None,
        "dato": parse_dato(row.get("dato", "")),
        "total_karakter": int(total_karakter) if total_karakter not in (None, "") else None,
        "tilsynsbesoektype": int(tilsynsbesoektype) if tilsynsbesoektype not in (None, "") else None,
        "tema_karakter": tema_karakter,
    }


def flatten_vurdering(row):
    """Flatten a vurderinger.csv row to a typed dict.

    Parameters
    ----------
    row : dict
        Raw CSV row (all values strings).

    Returns
    -------
    dict
        Flat dict with typed values and ISO dates.
    """
    karakter = row.get("karakter")
    return {
        "tilsynid": row["tilsynid"],
        "dato": parse_dato(row.get("dato", "")),
        "ordningsverdi": row["ordningsverdi"],
        "kravpunktnavn_no": row.get("kravpunktnavn_no") or None,
        "kravpunktnavn_nn": row.get("kravpunktnavn_nn") or None,
        "karakter": int(karakter) if karakter not in (None, "") else None,
        "tekst_no": row.get("tekst_no") or None,
        "tekst_nn": row.get("tekst_nn") or None,
    }


def content_hash(flat_tilsyn, kravpunkter):
    """Compute content hash over tilsyn + its kravpunkter.

    Hash changes if any scored content changes — allows CDC to detect
    modifications (e.g., Mattilsynet corrects a score after the tilsyn
    was first published).

    Parameters
    ----------
    flat_tilsyn : dict
        Output of :func:`flatten_tilsyn`.
    kravpunkter : list of dict
        All :func:`flatten_vurdering` outputs for this tilsynid.

    Returns
    -------
    str
        16-char hex prefix of SHA-256 over canonical JSON.
    """
    payload = {
        "tilsyn": {k: flat_tilsyn[k] for k in (
            "tilsynid", "tilsynsobjektid", "orgnummer", "navn",
            "adrlinje1", "postnr", "poststed", "sakref",
            "status", "dato", "total_karakter", "tilsynsbesoektype",
            "tema_karakter",
        )},
        "kravpunkter": sorted(
            [{k: kp[k] for k in ("ordningsverdi", "karakter", "kravpunktnavn_no", "tekst_no")} for kp in kravpunkter],
            key=lambda x: x["ordningsverdi"],
        ),
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
