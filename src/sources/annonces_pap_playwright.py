"""
Scraper PAP — Playwright + Stealth v3
Structure confirmée : div.col-1-3 > div.item-body > span.item-price.txt-indigo
"""
from __future__ import annotations
import hashlib, logging, re, time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)
BASE_URL = "https://www.pap.fr"

DEPT_URL = {
    "91": "https://www.pap.fr/annonce/vente-immobiliere-essonne-91-g455",
    "92": "https://www.pap.fr/annonce/vente-immobiliere-hauts-de-seine-92-g456",
    "78": "https://www.pap.fr/annonce/vente-immobiliere-yvelines-78-g442",
}
URL_IDF = "https://www.pap.fr/annonce/vente-immobiliere-ile-de-france-g471"

def scrape_commune(code_insee, nom_commune, types, prix_max, surface_min, surface_max, nb_pieces_min=1, **kwargs):
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
    except ImportError:
        logger.error("Playwright/stealth non installé")
        return []

    dept = code_insee[:2]
    url = DEPT_URL.get(dept, URL_IDF)
    params = f"?prix-max={prix_max}&surface-min={surface_min}"
    if surface_max: params += f"&surface-max={surface_max}"
    if nb_pieces_min > 1: params += f"&nb-pieces-min={nb_pieces_min}"

    html = ""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            locale="fr-FR", timezone_id="Europe/Paris", viewport={"width":1280,"height":800},
        )
        page = ctx.new_page()
        page.route("**/*.{woff,woff2,ttf,otf}", lambda r: r.abort())
        Stealth().use_sync(page)

        try:
            page.goto(url + params, wait_until="domcontentloaded", timeout=25000)
            for _ in range(12):
                if "instant" not in page.title().lower() and "moment" not in page.title().lower(): break
                page.wait_for_timeout(1000)
            page.wait_for_timeout(2000)

            # Fermer cookies
            for sel in ["#didomi-notice-agree-button", "button:has-text('Tout accepter')", "button:has-text('Accepter')"]:
                try:
                    if page.locator(sel).first.is_visible(timeout=500):
                        page.locator(sel).first.click()
                        page.wait_for_timeout(800)
                        break
                except: continue

            # Scroll pour déclencher lazy-load
            for pos in [500, 1000, 1500, 2000, 2500, 3000]:
                page.evaluate(f"window.scrollTo(0,{pos})")
                page.wait_for_timeout(400)
            page.wait_for_timeout(2000)
            html = page.content()

        except Exception as e:
            logger.warning(f"PAP {nom_commune} — {e}")
        finally:
            browser.close()

    if not html:
        return []

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    annonces = []

    # Structure PAP : div.col-1-3 contenant div.item-body
    for card in soup.find_all("div", class_="col-1-3"):
        body = card.find("div", class_="item-body")
        if not body: continue
        try:
            ann = _parse_card(card, body, code_insee, nom_commune)
            if ann: annonces.append(ann)
        except Exception:
            continue

    # Filtre commune par code postal
    from .annonces_bienici_playwright import VILLE_SLUG
    from . import annonces_bienici_playwright as _b
    # Récupérer les CP attendus
    cp_communes = {
        "91377":["91300"],"91477":["91120"],"91692":["91940"],
        "91272":["91190"],"91471":["91400"],"91345":["91160"],
        "91149":["91380"],"91589":["91600"],"91323":["91260"],
        "91687":["91170"],"91027":["91200"],"91432":["91290"],
        "91228":["91000","91080"],"91182":["91100"],
        "92002":["92160"],"92023":["92140"],"92017":["92290"],
        "92060":["92350"],"92071":["92330"],"92007":["92220"],
        "92032":["92260"],
        "94034":["94260"],"94076":["94800"],"94038":["94240"],
        "94017":["94550"],"94073":["94320"],"94022":["94600"],
        "94055":["94310"],"94080":["94400"],
        "78686":["78140"],"78297":["78280"],"78208":["78990"],
        "78621":["78190"],"78430":["78180"],
    }
    cp_list = cp_communes.get(code_insee, [])
    # Filtre par CP si disponible, sinon par nom ville
    cp_list = cp_communes.get(code_insee, [])
    if cp_list:
        # Essayer d'abord par CP
        par_cp = [a for a in annonces if a.get("code_postal","") in cp_list]
        if par_cp:
            filtrees = par_cp
        else:
            # Fallback : par nom ville (PAP ne retourne pas toujours le CP)
            nom_lower = nom_commune.lower().replace("-"," ")
            filtrees = [a for a in annonces
                        if nom_lower in (a.get("ville_lbc","") or "").lower()
                        or nom_commune.split("-")[0].lower() in (a.get("ville_lbc","") or "").lower()]
    else:
        nom_lower = nom_commune.lower().replace("-"," ").split(" ")[0]
        filtrees = [a for a in annonces if nom_lower in (a.get("ville_lbc","") or "").lower()]

    # Déduplication par id
    seen = set()
    deduped = []
    for a in filtrees:
        if a["id"] not in seen:
            seen.add(a["id"])
            deduped.append(a)
    logger.info(f"PAP {nom_commune} — {len(deduped)}/{len(annonces)} annonces")
    return deduped


def _parse_card(card, body, code_insee, nom_commune):
    txt = body.get_text(" ", strip=True)

    # Prix : span.item-price
    prix_el = body.find("span", class_=re.compile(r"item-price"))
    prix = _parse_prix(prix_el.get_text() if prix_el else txt)
    if not prix: return None

    # Surface
    surface = _parse_surface(txt)

    # Pièces
    nb_pieces = _parse_pieces(txt)

    # Ville — format "Ville (CP)"
    ville_match = re.search(r"([A-ZÀ-Ü][a-zà-üA-ZÀ-Ü\s\-]+)\s*\(\d{5}\)", txt)
    ville = ville_match.group(1).strip() if ville_match else nom_commune
    cp_match = re.search(r"\((\d{5})\)", txt)
    cp = cp_match.group(1) if cp_match else ""

    # Quartier / titre
    # Titre propre : Type + pièces + surface + ville
    type_str = "Maison" if any(w in txt.lower() for w in ["maison","villa","pavillon"]) else "Appartement"
    pieces_str = f" {nb_pieces} pièces" if nb_pieces else ""
    surf_str = f" {int(surface)}m²" if surface else ""
    titre = f"{type_str}{pieces_str}{surf_str} — {ville}"

    # URL — PAP met les liens dans des <a> avec href absolu ou relatif
    url = ""
    ad_id = ""
    for a in card.find_all("a", href=True):
        href = a.get("href","")
        if re.search(r"\d{6,}", href):
            url = BASE_URL + href if href.startswith("/") else href
            m_id = re.search(r"(\d{6,})", href)
            ad_id = m_id.group(1) if m_id else ""
            break
    if not ad_id:
        ad_id = hashlib.md5(txt[:50].encode()).hexdigest()[:10]

    # Image
    img = card.find("img")
    image_url = None
    if img:
        image_url = img.get("src") or img.get("data-src")
        if image_url and image_url.startswith("//"): image_url = "https:" + image_url
        elif image_url and image_url.startswith("/"): image_url = BASE_URL + image_url

    # Type bien
    type_bien = "maison" if any(w in txt.lower() for w in ["maison","villa","pavillon","chalet"]) else "appartement"
    prix_m2 = round(prix/surface,0) if surface and surface > 5 else None

    return {
        "id": "pap_" + hashlib.md5(ad_id.encode()).hexdigest()[:12],
        "source":"pap","code_commune":code_insee,"nom_commune":nom_commune,
        "titre":str(titre)[:200],"prix":prix,"surface":surface,"prix_m2":prix_m2,
        "type_bien":type_bien,"nb_pieces":nb_pieces,"url":url,
        "image_url":image_url,"quartier":ville,"ville_lbc":ville,"code_postal":cp,
        "date_scraping":datetime.now().isoformat(timespec="seconds"),"actif":True,
    }


def _parse_prix(text):
    m = re.search(r"([\d]{2,3}[\.\s][\d]{3})", str(text))
    if m:
        digits = re.sub(r"[^\d]","",m.group(1))
        if digits and 10000 <= int(digits) <= 5_000_000:
            return int(digits)
    m2 = re.search(r"([\d\s\u00a0]{5,10})\s*€", str(text))
    if m2:
        digits = re.sub(r"[^\d]","",m2.group(1))
        if digits and 10000 <= int(digits) <= 5_000_000:
            return int(digits)
    return None

def _parse_surface(text):
    m = re.search(r"(\d+[\.,]?\d*)\s*m²?", str(text), re.I)
    if m:
        v = float(m.group(1).replace(",","."))
        if 5 < v < 2000: return v
    return None

def _parse_pieces(text):
    m = re.search(r"(\d)\s*(pièce|chambre|p\.)\b", str(text), re.I)
    return int(m.group(1)) if m else None
