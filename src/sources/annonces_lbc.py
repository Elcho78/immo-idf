"""
Scraper LeBonCoin Immo — Annonces vente immobilière IDF
Utilise l'API finder de LBC (clé publique, documentée dans plusieurs projets open source).

Catégorie 9 = Immobilier > Ventes
real_estate_type : 1=Maison, 2=Appartement, 3=Terrain, 4=Parking, 5=Autre
"""
from __future__ import annotations

import hashlib
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

LBC_SEARCH_URL = "https://api.leboncoin.fr/finder/search"
LBC_BASE_URL   = "https://www.leboncoin.fr"

# Clé API publique LBC (utilisée par l'app mobile, documentée publiquement)
LBC_API_KEY = "ba0c2dad52b3585c9a2ac9a42a6b3dc5e5e1ea27"

TYPE_MAP = {
    "appartement": 2,
    "maison": 1,
}

HEADERS = {
    "api_key": LBC_API_KEY,
    "Content-Type": "application/json; charset=utf-8",
    "User-Agent": "LeBonCoin/8.0.0 (Android)",
    "Accept": "application/json",
}

# Coordonnées approximatives des communes IDF pour le filtre géo
# geo.api.gouv.fr fournit ces centroïdes — on utilise les caches transports si dispo
from pathlib import Path
import json


def _get_centroide(code_insee: str) -> Optional[tuple[float, float]]:
    cache = Path(f"data/cache/transports/geo_{code_insee}.json")
    if cache.exists():
        d = json.loads(cache.read_text())
        return d["lat"], d["lon"]
    try:
        resp = httpx.get(
            f"https://geo.api.gouv.fr/communes/{code_insee}",
            params={"fields": "centre"}, timeout=8
        )
        resp.raise_for_status()
        coords = resp.json().get("centre", {}).get("coordinates", [None, None])
        return float(coords[1]), float(coords[0])
    except Exception:
        return None


def scrape_commune(
    code_insee: str,
    nom_commune: str,
    types: list[str],
    prix_max: int,
    surface_min: int,
    surface_max: Optional[int],
    nb_pieces_min: int = 1,
    rayon_m: int = 3000,
) -> list[dict]:
    """Scrape les annonces LeBonCoin pour une commune (rayon autour du centroïde)."""
    centroide = _get_centroide(code_insee)
    if not centroide:
        logger.warning(f"LBC {nom_commune} — centroïde introuvable")
        return []

    lat, lon = centroide
    real_estate_types = [TYPE_MAP[t] for t in types if t in TYPE_MAP]
    if not real_estate_types:
        real_estate_types = [1, 2]

    annonces = []
    offset = 0
    limit = 100
    max_pages = 3

    for _ in range(max_pages):
        batch = _search_page(
            lat=lat, lon=lon,
            rayon_m=rayon_m,
            real_estate_types=real_estate_types,
            prix_max=prix_max,
            surface_min=surface_min,
            surface_max=surface_max,
            nb_pieces_min=nb_pieces_min,
            limit=limit,
            offset=offset,
            code_insee=code_insee,
            nom_commune=nom_commune,
            types_str=types,
        )
        annonces.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(1.0)

    logger.info(f"LBC {nom_commune} — {len(annonces)} annonces")
    return annonces


def _search_page(
    lat, lon, rayon_m,
    real_estate_types, prix_max,
    surface_min, surface_max,
    nb_pieces_min, limit, offset,
    code_insee, nom_commune, types_str
) -> list[dict]:

    ranges: dict = {
        "price": {"min": 30000, "max": prix_max},
    }
    if surface_min:
        ranges["square"] = {"min": surface_min}
    if surface_max:
        ranges.setdefault("square", {})["max"] = surface_max
    if nb_pieces_min and nb_pieces_min > 1:
        ranges["rooms"] = {"min": nb_pieces_min}

    payload = {
        "filters": {
            "category": {"id": "9"},
            "enums": {
                "ad_type": ["offer"],
                "real_estate_type": [str(t) for t in real_estate_types],
            },
            "location": {
                "area": {"lat": lat, "lng": lon, "radius": rayon_m},
            },
            "ranges": ranges,
            "keywords": {},
        },
        "limit": limit,
        "offset": offset,
        "sort_by": "time",
        "sort_order": "desc",
        "owner": {},
    }

    try:
        resp = httpx.post(LBC_SEARCH_URL, json=payload, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"LBC {nom_commune} — {e}")
        return []

    ads = data.get("ads", [])
    annonces = []
    type_map_inv = {2: "appartement", 1: "maison", 3: "terrain", 4: "parking"}

    for ad in ads:
        try:
            ann = _parse_ad(ad, code_insee, nom_commune, type_map_inv)
            if ann:
                annonces.append(ann)
        except Exception:
            continue

    return annonces


def _parse_ad(ad: dict, code_insee: str, nom_commune: str, type_map_inv: dict) -> Optional[dict]:
    ad_id = str(ad.get("list_id", ""))
    if not ad_id:
        return None

    prix = ad.get("price", [None])
    prix = prix[0] if isinstance(prix, list) and prix else prix
    if not isinstance(prix, (int, float)) or prix <= 0:
        return None
    prix = int(prix)

    attrs = {a["key"]: a.get("value_label", a.get("values", [""])[0] if a.get("values") else "")
             for a in ad.get("attributes", []) if "key" in a}

    surface = _parse_float(attrs.get("square", attrs.get("surface", "")))
    nb_pieces = _parse_int(attrs.get("rooms", attrs.get("piece", "")))

    # Type bien
    re_type_raw = attrs.get("real_estate_type", "")
    re_type_int = _parse_int(str(re_type_raw))
    type_bien = type_map_inv.get(re_type_int, "appartement")

    prix_m2 = round(prix / surface, 0) if surface and surface > 5 else None

    # LBC real URL format
    url = (ad.get("url") or 
           f"{LBC_BASE_URL}/ad/ventes_immobilieres/{ad_id}")
    ann_id = "lbc_" + hashlib.md5(ad_id.encode()).hexdigest()[:12]

    titre = ad.get("subject", "Annonce LeBonCoin")[:200]

    return {
        "id": ann_id,
        "source": "leboncoin",
        "code_commune": code_insee,
        "nom_commune": nom_commune,
        "titre": titre,
        "prix": prix,
        "surface": surface,
        "prix_m2": prix_m2,
        "type_bien": type_bien,
        "nb_pieces": nb_pieces,
        "url": url,
        "date_scraping": _now(),
        "actif": True,
    }


def _parse_float(val) -> Optional[float]:
    import re
    if val is None:
        return None
    m = re.search(r"(\d+[\.,]?\d*)", str(val))
    if m:
        v = float(m.group(1).replace(",", "."))
        if 5 < v < 5000:
            return v
    return None


def _parse_int(val) -> Optional[int]:
    import re
    if val is None:
        return None
    m = re.search(r"(\d+)", str(val))
    return int(m.group(1)) if m else None


def _make_id(source: str, key: str) -> str:
    return source + "_" + hashlib.md5(key.encode()).hexdigest()[:12]


def _now() -> str:
    from datetime import datetime
    return datetime.now().isoformat(timespec="seconds")
