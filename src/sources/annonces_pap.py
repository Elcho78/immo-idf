"""
Scraper PAP — via API officielle PAP (JSON, pas de scraping HTML)
URL : https://api.pap.fr/annonces?categorie=vente&typebien=appartement,maison&departementId=91
"""
from __future__ import annotations
import hashlib, logging, time
from datetime import datetime
from typing import Optional
import httpx

logger = logging.getLogger(__name__)
PAP_API = "https://api.pap.fr/annonces"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible)",
    "Accept": "application/json",
    "Origin": "https://www.pap.fr",
    "Referer": "https://www.pap.fr/",
}
TYPE_MAP = {"appartement": "appartement", "maison": "maison"}
DEPT_PAP = {"75":"75","77":"77","78":"78","91":"91","92":"92","93":"93","94":"94","95":"95"}

def scrape_commune(code_insee, nom_commune, types, prix_max, surface_min, surface_max, nb_pieces_min=1, **kwargs):
    dept = DEPT_PAP.get(code_insee[:2])
    if not dept: return []
    type_str = ",".join(TYPE_MAP[t] for t in types if t in TYPE_MAP) or "appartement,maison"
    params = {"categorie":"vente","typebien":type_str,"departementId":dept,
              "prixmax":prix_max,"surfacemin":surface_min,"nb_resultats":100,"page":1}
    if surface_max: params["surfacemax"] = surface_max
    if nb_pieces_min > 1: params["nbpiecesmin"] = nb_pieces_min
    annonces = []
    for page in range(1, 4):
        params["page"] = page
        batch = _fetch(params, code_insee, nom_commune)
        annonces.extend(batch)
        if len(batch) < 100: break
        time.sleep(1.5)
    logger.info(f"PAP API {nom_commune} — {len(annonces)} annonces")
    return annonces

def _fetch(params, code_insee, nom_commune):
    try:
        resp = httpx.get(PAP_API, params=params, headers=HEADERS, timeout=20, follow_redirects=True)
        if resp.status_code == 403:
            logger.warning(f"PAP API — 403")
            return []
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"PAP API {nom_commune} — {e}")
        return []
    ads = data.get("annonces", data.get("results", data if isinstance(data, list) else []))
    if not isinstance(ads, list):
        logger.debug(f"PAP API structure: {list(data.keys()) if isinstance(data,dict) else type(data)}")
        return []
    return [a for a in [_parse(ad, code_insee, nom_commune) for ad in ads] if a]

def _parse(ad, code_insee, nom_commune):
    ad_id = str(ad.get("id", ad.get("reference", "")))
    if not ad_id: return None
    prix = ad.get("prix") or ad.get("price")
    if not prix: return None
    try: prix = int(float(str(prix).replace(" ","").replace("€","")))
    except: return None
    if prix <= 0: return None
    surface = _f(ad.get("surface") or ad.get("surfaceArea"))
    nb_pieces = _i(ad.get("nb_pieces") or ad.get("rooms"))
    type_raw = str(ad.get("typebien", ad.get("type","appartement"))).lower()
    type_bien = "maison" if "maison" in type_raw else "appartement"
    prix_m2 = round(prix/surface, 0) if surface and surface > 5 else None
    photos = ad.get("photos", ad.get("images", []))
    image_url = (photos[0].get("url") if isinstance(photos[0],dict) else photos[0]) if photos else None
    ville = ad.get("ville", ad.get("city", nom_commune))
    cp = str(ad.get("cp", ad.get("codePostal", "")))
    quartier = ad.get("quartier", ville)
    titre = ad.get("titre", ad.get("title", f"{type_bien} {nb_pieces or ''} pièces"))
    slug = ad.get("slug", ad_id)
    return {
        "id": "pap_" + hashlib.md5(ad_id.encode()).hexdigest()[:12],
        "source": "pap", "code_commune": code_insee, "nom_commune": nom_commune,
        "titre": str(titre)[:200], "prix": prix, "surface": surface, "prix_m2": prix_m2,
        "type_bien": type_bien, "nb_pieces": nb_pieces,
        "url": f"https://www.pap.fr/annonces/{slug}",
        "image_url": image_url, "quartier": quartier, "ville_lbc": ville, "code_postal": cp,
        "date_scraping": datetime.now().isoformat(timespec="seconds"), "actif": True,
    }

def _f(v):
    try: return float(str(v).replace(",",".").replace(" ","")) if v else None
    except: return None

def _i(v):
    try: return int(float(str(v))) if v else None
    except: return None
