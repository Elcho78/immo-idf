"""
Orchestrateur scraping annonces — PAP + LeBonCoin
Lance les scrapers pour toutes les communes actives,
calcule le delta vs prix DVF, stocke en base.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ..config import Config
from ..storage import Storage

logger = logging.getLogger(__name__)


@dataclass
class FiltresAnnonces:
    types: list[str]
    surface_min: int
    surface_max: Optional[int]
    prix_max: int
    nb_pieces_min: int
    seuil_sous_cote_pct: float = -10.0


def run_scraping(config: Config, force: bool = False) -> dict:
    """
    Lance le scraping pour toutes les communes actives.
    Retourne un résumé {total, nouvelles, sources}.
    """
    cfg_ann = getattr(config, "annonces", None)
    if cfg_ann and not cfg_ann.get("actif", True):
        logger.info("Scraping annonces désactivé dans config.yml")
        return {"total": 0, "nouvelles": 0}

    filtres = _build_filtres(cfg_ann or {})
    sources = (cfg_ann or {}).get("sources", ["pap", "leboncoin"])
    storage = Storage()
    dvf_prices = storage.get_dvf_prices()

    total, nouvelles = 0, 0
    communes = config.communes_actives

    logger.info(f"Scraping annonces — {len(communes)} communes, sources: {sources}")

    for commune in communes:
        for source in sources:
            try:
                annonces = _scrape_source(
                    source=source,
                    code_insee=commune.code_insee,
                    nom_commune=commune.nom,
                    filtres=filtres,
                )
                # Enrichir avec delta DVF
                prix_dvf = dvf_prices.get(commune.code_insee)
                for ann in annonces:
                    ann = _enrich(ann, prix_dvf, filtres.seuil_sous_cote_pct)
                    is_new = storage.save_annonce(ann)
                    total += 1
                    if is_new:
                        nouvelles += 1
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Scraping {source} {commune.nom} : {e}", exc_info=True)

    # Marquer les annonces disparues (non vues depuis 24h) comme inactives
    storage.deactivate_old_annonces(hours=36)

    logger.info(f"Scraping terminé — {total} annonces traitées, {nouvelles} nouvelles")
    return {"total": total, "nouvelles": nouvelles}


def _scrape_source(source, code_insee, nom_commune, filtres) -> list[dict]:
    if source == "pap":
        from .annonces_pap import scrape_commune
    elif source == "leboncoin":
        from .annonces_lbc import scrape_commune
    else:
        logger.warning(f"Source inconnue : {source}")
        return []

    return scrape_commune(
        code_insee=code_insee,
        nom_commune=nom_commune,
        types=filtres.types,
        prix_max=filtres.prix_max,
        surface_min=filtres.surface_min,
        surface_max=filtres.surface_max,
        nb_pieces_min=filtres.nb_pieces_min,
    )


def _enrich(ann: dict, prix_dvf_m2: Optional[float], seuil: float) -> dict:
    """Calcule delta DVF et rendement estimé."""
    ann["delta_dvf_pct"] = None
    ann["sous_cote"] = False
    ann["rendement_estime"] = None

    if ann.get("prix_m2") and prix_dvf_m2 and prix_dvf_m2 > 0:
        delta = (ann["prix_m2"] - prix_dvf_m2) / prix_dvf_m2 * 100
        ann["delta_dvf_pct"] = round(delta, 1)
        ann["sous_cote"] = delta <= seuil
        ann["prix_dvf_ref"] = round(prix_dvf_m2, 0)

    # Rendement estimé (loyer dept / prix annonce)
    dept = ann["code_commune"][:2]
    from ..pipeline import LOYER_M2_PAR_DEPT, LOYER_M2_DEFAUT
    loyer = LOYER_M2_PAR_DEPT.get(dept, LOYER_M2_DEFAUT)
    if ann.get("prix") and ann["prix"] > 0:
        ann["rendement_estime"] = round(loyer * 12 / ann["prix"] *
                                        (ann.get("surface") or 50) * 100, 2)

    return ann


def _build_filtres(cfg: dict) -> FiltresAnnonces:
    return FiltresAnnonces(
        types=cfg.get("types", ["appartement", "maison"]),
        surface_min=cfg.get("surface_min_m2", 20),
        surface_max=cfg.get("surface_max_m2", None),
        prix_max=cfg.get("prix_max", 500000),
        nb_pieces_min=cfg.get("nb_pieces_min", 1),
        seuil_sous_cote_pct=cfg.get("seuil_sous_cote_pct", -10.0),
    )
