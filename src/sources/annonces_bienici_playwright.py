"""
Scraper Bienici — via Playwright (navigateur headless)
"""
from __future__ import annotations
import hashlib, json, logging, re, time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)
BASE_URL = "https://www.bienici.com"

VILLE_SLUG = {
    "91377":"massy-91300","91477":"palaiseau-91120","91692":"les-ulis-91940",
    "91272":"gif-sur-yvette-91190","91471":"orsay-91400","91345":"longjumeau-91160",
    "91149":"chilly-mazarin-91380","91589":"savigny-sur-orge-91600",
    "91323":"juvisy-sur-orge-91260","91687":"viry-chatillon-91170",
    "91027":"athis-mons-91200","91432":"morangis-91290",
    "91228":"evry-courcouronnes-91000","91182":"corbeil-essonnes-91100",
    "92002":"antony-92160","92023":"clamart-92140","92017":"chatenay-malabry-92290",
    "92060":"le-plessis-robinson-92350","92071":"sceaux-92330",
    "92007":"bagneux-92220","92032":"fontenay-aux-roses-92260",
    "94034":"fresnes-94260","94076":"villejuif-94800","94038":"l-hay-les-roses-94240",
    "94017":"chevilly-larue-94550","94073":"thiais-94320",
    "94022":"choisy-le-roi-94600","94055":"orly-94310","94080":"vitry-sur-seine-94400",
    "78686":"velizy-villacoublay-78140","78297":"guyancourt-78280",
    "78208":"elancourt-78990","78621":"trappes-78190","78430":"montigny-le-bretonneux-78180",
}

def scrape_commune(code_insee, nom_commune, types, prix_max, surface_min, surface_max, nb_pieces_min=1, **kwargs):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright non installé")
        return []

    slug = VILLE_SLUG.get(code_insee)
    if not slug:
        return []

    type_map = {"appartement": "flat", "maison": "house"}
    type_str = ",".join(type_map[t] for t in types if t in type_map)

    annonces = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            locale="fr-FR",
        )
        page = ctx.new_page()
        page.route("**/*.{woff,woff2,ttf,otf}", lambda r: r.abort())

        # Intercepter les appels API Bienici (la page fait des XHR vers realEstateAds.json)
        ads_from_xhr = []

        def handle_response(response):
            if "realEstateAds.json" in response.url and response.status == 200:
                try:
                    data = response.json()
                    ads_from_xhr.extend(data.get("realEstateAds", []))
                except Exception:
                    pass

        page.on("response", handle_response)

        try:
            url = f"{BASE_URL}/recherche/achat/{slug}"
            params = f"?prix-max={prix_max}&surface-min={surface_min}"
            if type_str: params += f"&types={type_str}"
            if surface_max: params += f"&surface-max={surface_max}"
            if nb_pieces_min > 1: params += f"&pieces-min={nb_pieces_min}"

            page.goto(url + params, wait_until="networkidle", timeout=20000)
            page.wait_for_timeout(2000)

        except Exception as e:
            logger.warning(f"Bienici Playwright {nom_commune} — {e}")
        finally:
            browser.close()

        # Utiliser les données interceptées depuis XHR
        for ad in ads_from_xhr:
            try:
                ann = _parse(ad, code_insee, nom_commune)
                if ann: annonces.append(ann)
            except Exception:
                continue

    logger.info(f"Bienici Playwright {nom_commune} — {len(annonces)} annonces")
    return annonces


def _parse(ad, code_insee, nom_commune):
    ad_id = str(ad.get("id",""))
    if not ad_id: return None
    prix_raw = ad.get("price")
    if isinstance(prix_raw, list): prix = int(prix_raw[0]) if prix_raw else None
    elif isinstance(prix_raw,(int,float)): prix = int(prix_raw)
    else: return None
    if not prix or prix < 10000: return None
    surf_raw = ad.get("surfaceArea") or ad.get("area")
    surface = float(surf_raw[0] if isinstance(surf_raw,list) else surf_raw) if surf_raw else None
    nb_pieces = ad.get("roomsQuantity") or ad.get("rooms")
    type_raw = ad.get("propertyType","flat")
    type_bien = "maison" if type_raw == "house" else "appartement"
    prix_m2 = round(prix/surface,0) if surface and surface > 5 else None
    photos = ad.get("photos",[])
    image_url = photos[0].get("url_photo",photos[0].get("url")) if photos and isinstance(photos[0],dict) else None
    cp = str(ad.get("postalCode",""))
    ville = ad.get("city", nom_commune)
    district = ad.get("district",{})
    quartier = district.get("libelle",ville) if isinstance(district,dict) else ville
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
