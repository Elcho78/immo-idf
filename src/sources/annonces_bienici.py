"""
Scraper Bienici v4 — via page de recherche HTML
URL : https://www.bienici.com/recherche/achat/{ville}-{cp}
Les données sont dans window.__REDUXSTATE__ ou un script JSON embarqué.
"""
from __future__ import annotations
import hashlib, json, logging, re, time
from datetime import datetime
from typing import Optional
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
BASE_URL = "https://www.bienici.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "fr-FR,fr;q=0.9",
}
TYPE_MAP = {"appartement": "flat", "maison": "house"}

# Slug ville pour l'URL Bienici
VILLE_SLUG = {
    "91377": "massy-91300", "91477": "palaiseau-91120", "91692": "les-ulis-91940",
    "91272": "gif-sur-yvette-91190", "91471": "orsay-91400", "91345": "longjumeau-91160",
    "91149": "chilly-mazarin-91380", "91589": "savigny-sur-orge-91600",
    "91323": "juvisy-sur-orge-91260", "91687": "viry-chatillon-91170",
    "91027": "athis-mons-91200", "91432": "morangis-91290",
    "91228": "evry-courcouronnes-91000", "91182": "corbeil-essonnes-91100",
    "92002": "antony-92160", "92023": "clamart-92140", "92017": "chatenay-malabry-92290",
    "92060": "le-plessis-robinson-92350", "92071": "sceaux-92330",
    "92007": "bagneux-92220", "92032": "fontenay-aux-roses-92260",
    "94034": "fresnes-94260", "94076": "villejuif-94800", "94038": "l-hay-les-roses-94240",
    "94017": "chevilly-larue-94550", "94073": "thiais-94320",
    "94022": "choisy-le-roi-94600", "94055": "orly-94310", "94080": "vitry-sur-seine-94400",
    "78686": "velizy-villacoublay-78140", "78297": "guyancourt-78280",
    "78208": "elancourt-78990", "78621": "trappes-78190", "78430": "montigny-le-bretonneux-78180",
}

def scrape_commune(code_insee, nom_commune, types, prix_max, surface_min, surface_max, nb_pieces_min=1, **kwargs):
    slug = VILLE_SLUG.get(code_insee)
    if not slug:
        logger.debug(f"Bienici {nom_commune} — slug inconnu")
        return []

    property_types = [TYPE_MAP[t] for t in types if t in TYPE_MAP] or ["flat","house"]
    type_str = ",".join(property_types)

    url = f"{BASE_URL}/recherche/achat/{slug}"
    params = {"prix-max": prix_max, "surface-min": surface_min, "types": type_str}
    if surface_max: params["surface-max"] = surface_max
    if nb_pieces_min > 1: params["pieces-min"] = nb_pieces_min

    try:
        resp = httpx.get(url, params=params, headers=HEADERS, timeout=20, follow_redirects=True)
        if resp.status_code == 403:
            logger.warning(f"Bienici {nom_commune} — 403")
            return []
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Bienici {nom_commune} — {e}")
        return []

    # Chercher les données JSON dans le HTML
    ads = _extract_ads(resp.text)
    annonces = [a for a in [_parse(ad, code_insee, nom_commune) for ad in ads] if a]
    logger.info(f"Bienici {nom_commune} — {len(annonces)} annonces")
    return annonces


def _extract_ads(html: str) -> list[dict]:
    """Extrait les annonces depuis le JSON embarqué dans la page."""
    # Tentative 1 : window.__REDUXSTATE__
    m = re.search(r'window\.__REDUXSTATE__\s*=\s*(\{.*?\});?\s*</script>', html, re.DOTALL)
    if m:
        try:
            state = json.loads(m.group(1))
            ads = (state.get("realEstateAds", {}).get("ads", []) or
                   state.get("searchResults", {}).get("ads", []))
            if ads: return ads
        except Exception:
            pass

    # Tentative 2 : JSON-LD ou scripts application/json
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script"):
        text = script.string or ""
        if '"realEstateAds"' in text or '"price"' in text and '"surfaceArea"' in text:
            try:
                # Chercher un objet JSON dans le script
                m2 = re.search(r'\{["\']realEstateAds["\'].*?\}(?=\s*[;,]?\s*(?:var|let|const|window|\Z))',
                               text, re.DOTALL)
                if m2:
                    data = json.loads(m2.group(0))
                    return data.get("realEstateAds", [])
            except Exception:
                pass

    # Tentative 3 : chercher tableau d'annonces JSON
    matches = re.findall(r'"id"\s*:\s*"[^"]+"\s*,\s*"title"\s*:\s*"[^"]+"\s*,\s*"postalCode"', html)
    if matches:
        logger.debug(f"Bienici — {len(matches)} patterns trouvés mais extraction JSON échouée")

    return []


def _parse(ad, code_insee, nom_commune):
    ad_id = str(ad.get("id", ""))
    if not ad_id: return None
    prix_raw = ad.get("price")
    if isinstance(prix_raw, list): prix = int(prix_raw[0]) if prix_raw else None
    elif isinstance(prix_raw, (int,float)): prix = int(prix_raw)
    else: return None
    if not prix or prix < 10000: return None
    surf_raw = ad.get("surfaceArea") or ad.get("area")
    surface = float(surf_raw[0] if isinstance(surf_raw,list) else surf_raw) if surf_raw else None
    nb_pieces = ad.get("roomsQuantity") or ad.get("rooms")
    type_raw = ad.get("propertyType", "flat")
    type_bien = "maison" if type_raw == "house" else "appartement"
    prix_m2 = round(prix/surface,0) if surface and surface > 5 else None
    photos = ad.get("photos", [])
    image_url = photos[0].get("url_photo",photos[0].get("url")) if photos and isinstance(photos[0],dict) else None
    cp = str(ad.get("postalCode",""))
    ville = ad.get("city", nom_commune)
    district = ad.get("district", {})
    quartier = district.get("libelle", ville) if isinstance(district,dict) else ville
    titre = ad.get("title") or f"{type_bien} {nb_pieces or ''} pièces"
    return {
        "id": "bienici_" + hashlib.md5(ad_id.encode()).hexdigest()[:12],
        "source":"bienici","code_commune":code_insee,"nom_commune":nom_commune,
        "titre":str(titre)[:200],"prix":prix,"surface":surface,"prix_m2":prix_m2,
        "type_bien":type_bien,"nb_pieces":int(nb_pieces) if nb_pieces else None,
        "url":f"{BASE_URL}/annonces/{ad_id}","image_url":image_url,
        "quartier":quartier,"ville_lbc":ville,"code_postal":cp,
        "date_scraping":datetime.now().isoformat(timespec="seconds"),"actif":True,
    }
