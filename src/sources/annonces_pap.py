"""
Scraper PAP.fr — via flux RSS (pas de blocage, public)
URL RSS par région IDF : https://www.pap.fr/annonce/ventes-immobilieres-ile-de-france-g439.rss
URL RSS par département : https://www.pap.fr/annonce/ventes-{type}-{dept}-g{dept}.rss

Le RSS PAP contient dans la description HTML :
  - Prix, surface, nb pièces, ville
  - Lien vers l'annonce
"""
from __future__ import annotations

import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://www.pap.fr"

# RSS par département IDF
PAP_RSS = "https://www.pap.fr/annonce/ventes-{type}-{dept}-g{dept}.rss"
PAP_RSS_IDF = "https://www.pap.fr/annonce/ventes-immobilieres-ile-de-france-g439.rss"

TYPE_MAP = {"appartement": "appartements", "maison": "maisons"}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RSS reader)",
    "Accept": "application/rss+xml, application/xml, text/xml",
}


def scrape_commune(
    code_insee: str,
    nom_commune: str,
    types: list[str],
    prix_max: int,
    surface_min: int,
    surface_max: Optional[int],
    nb_pieces_min: int = 1,
    **kwargs,
) -> list[dict]:
    dept = code_insee[:2]
    annonces = []

    for type_bien in types:
        type_pap = TYPE_MAP.get(type_bien, "appartements")
        url = PAP_RSS.format(type=type_pap, dept=dept)
        items = _fetch_rss(url)

        for item in items:
            ann = _parse_item(item, type_bien, code_insee, nom_commune)
            if not ann:
                continue
            # Filtres
            if ann["prix"] and ann["prix"] > prix_max:
                continue
            if ann["surface"] and surface_min and ann["surface"] < surface_min:
                continue
            if ann["surface"] and surface_max and ann["surface"] > surface_max:
                continue
            # Filtre commune : le RSS est département-large, on filtre par nom
            loc = (ann.get("_localisation") or "").lower()
            if loc and nom_commune.lower()[:6] not in loc and code_insee not in loc:
                # Accepter quand même si pas de localisation précise
                if loc and len(loc) > 3:
                    continue
            annonces.append(ann)

    logger.info(f"PAP {nom_commune} — {len(annonces)} annonces (RSS)")
    return annonces


def _fetch_rss(url: str) -> list[dict]:
    try:
        resp = httpx.get(url, headers=HEADERS, timeout=20, follow_redirects=True)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for item in root.iter("item"):
            d = {}
            for child in item:
                tag = child.tag.split("}")[-1]  # strip namespace
                d[tag] = child.text or ""
            items.append(d)
        logger.debug(f"RSS {url[:60]} — {len(items)} items")
        return items
    except Exception as e:
        logger.warning(f"PAP RSS {url[:60]} — {e}")
        return []


def _parse_item(item: dict, type_bien: str, code_insee: str, nom_commune: str) -> Optional[dict]:
    link = item.get("link", "")
    if not link:
        return None

    titre = item.get("title", "Annonce PAP")[:200]
    desc  = item.get("description", "")

    # Prix — dans title ou description
    prix = _parse_prix(titre) or _parse_prix(desc)
    if not prix:
        return None

    # Surface
    surface = _parse_surface(titre) or _parse_surface(desc)

    # Pièces
    pieces = _parse_pieces(titre) or _parse_pieces(desc)

    # Localisation (ville mentionnée)
    loc = _parse_ville(titre) or _parse_ville(desc) or ""

    prix_m2 = round(prix / surface, 0) if surface and surface > 5 else None
    ann_id  = "pap_" + hashlib.md5(link.encode()).hexdigest()[:12]

    return {
        "id": ann_id,
        "source": "pap",
        "code_commune": code_insee,
        "nom_commune": nom_commune,
        "titre": titre,
        "prix": prix,
        "surface": surface,
        "prix_m2": prix_m2,
        "type_bien": type_bien,
        "nb_pieces": pieces,
        "url": link,
        "date_scraping": datetime.now().isoformat(timespec="seconds"),
        "actif": True,
        "_localisation": loc,
    }


# ── Parseurs ─────────────────────────────────────────────────────────────────

def _parse_prix(text: str) -> Optional[int]:
    if not text: return None
    # "250 000 €" ou "250000€" ou "250.000 €"
    m = re.search(r"(\d[\d\s\.\u00a0]{3,})\s*€", text)
    if m:
        digits = re.sub(r"[^\d]", "", m.group(1))
        if digits and 10_000 <= int(digits) <= 5_000_000:
            return int(digits)
    return None


def _parse_surface(text: str) -> Optional[float]:
    if not text: return None
    m = re.search(r"(\d+[\.,]?\d*)\s*m²?", text, re.I)
    if m:
        v = float(m.group(1).replace(",", "."))
        if 5 < v < 2000:
            return v
    return None


def _parse_pieces(text: str) -> Optional[int]:
    if not text: return None
    m = re.search(r"(\d)\s*(pièce|chambre|p\.)\b", text, re.I)
    return int(m.group(1)) if m else None


def _parse_ville(text: str) -> Optional[str]:
    if not text: return None
    # Ex : "À Massy" / "Massy (91)" / "91300 Massy"
    m = re.search(r"\b(?:à|À|dans)\s+([A-ZÀ-Ü][a-zà-ü\-]+(?:\s[A-ZÀ-Ü][a-zà-ü\-]+)?)", text)
    if m: return m.group(1).lower()
    m = re.search(r"9[0-9]\d{3}\s+([A-ZÀ-Ü][a-zà-ü\-]+)", text)
    if m: return m.group(1).lower()
    return None
