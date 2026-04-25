"""
Pipeline — collecte, agrège et score toutes les communes configurées.

Usage CLI :
    python main.py pipeline             # run normal (cache respecté)
    python main.py pipeline --force     # force le re-téléchargement
    python main.py pipeline --commune 93001  # une seule commune
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from .config import Config, CommuneConfig
from .scoring import CommuneScore
from .sources.dvf import get_prix_commune
from .sources.insee import get_revenus_commune, get_demographie_commune, get_revenus_iris
from .sources.transports import get_score_transports
from .storage import Storage

logger = logging.getLogger(__name__)

# Loyer moyen estimé par m² selon le département (source OLAP IDF / CLAMEUR 2023)
LOYER_M2_PAR_DEPT = {
    "75": 27.0, "92": 20.5, "93": 14.5, "94": 16.5,
    "78": 14.0, "91": 12.5, "95": 12.5, "77": 11.0,
}
LOYER_M2_DEFAUT = 13.5


class Pipeline:

    def __init__(self, config: Config):
        self.config = config
        self.storage = Storage()

    # ── Point d'entrée principal ─────────────────────────────────────────────

    def run(
        self,
        force_refresh: bool = False,
        code_insee_filtre: Optional[str] = None,
    ) -> list[CommuneScore]:
        """
        Lance le pipeline pour toutes les communes actives (ou une seule si filtre).
        Retourne la liste des scores calculés.
        """
        communes = self.config.communes_actives
        if code_insee_filtre:
            communes = [c for c in communes if c.code_insee == code_insee_filtre]
            if not communes:
                raise ValueError(f"Commune {code_insee_filtre} non active ou absente de la config.")

        logger.info(f"═══ Pipeline démarré — {len(communes)} commune(s) ═══")
        t0 = time.time()
        results: list[CommuneScore] = []

        for i, commune in enumerate(communes, 1):
            logger.info(f"[{i}/{len(communes)}] {commune.nom} ({commune.code_insee})")
            try:
                score = self._process_commune(commune, force_refresh)
                results.append(score)
                self.storage.save_commune(score)
                _log_result(score)
            except Exception as exc:
                logger.error(f"  ✗ Erreur pour {commune.nom} : {exc}", exc_info=True)

        elapsed = round(time.time() - t0, 1)
        logger.info(f"═══ Pipeline terminé en {elapsed}s — {len(results)}/{len(communes)} OK ═══")
        return results

    # ── Traitement d'une commune ─────────────────────────────────────────────

    def _process_commune(self, commune: CommuneConfig, force_refresh: bool) -> CommuneScore:
        p = self.config.parametres

        # 1 — Prix transactions (DVF)
        dvf = get_prix_commune(
            commune.code_insee,
            annees=p.annees_dvf,
            cache_ttl_jours=0 if force_refresh else p.cache_ttl_jours,
        )

        # 2 — Revenus (INSEE Filosofi)
        revenus = get_revenus_commune(commune.code_insee)

        # 3 — Démographie (INSEE RP)
        demo = get_demographie_commune(commune.code_insee)

        # 4 — Données IRIS sub-communales
        iris = get_revenus_iris(commune.code_insee)

        # 5 — Transports (IDFM)
        transports = get_score_transports(
            commune.code_insee,
            rayon_m=p.rayon_transports_m,
            cache_ttl_jours=0 if force_refresh else 30,
        )

        # 6 — Estimation loyer par département (OLAP IDF)
        dept = commune.code_insee[:2]
        loyer_estime = LOYER_M2_PAR_DEPT.get(dept, LOYER_M2_DEFAUT)

        # 7 — Construction et calcul du score
        score = CommuneScore(
            code_insee=commune.code_insee,
            nom=commune.nom,
            prix_m2=dvf.get("prix_m2_median"),
            prix_m2_p25=dvf.get("prix_m2_p25"),
            prix_m2_p75=dvf.get("prix_m2_p75"),
            loyer_m2_estime=loyer_estime,
            revenu_median=revenus.get("revenu_median"),
            taux_pauvrete=revenus.get("taux_pauvrete"),
            part_locataires=demo.get("part_locataires"),
            part_proprietaires=demo.get("part_proprietaires"),
            taux_vacance=demo.get("taux_vacance_locative"),
            part_moins_30=demo.get("part_moins_30"),
            score_transports=transports["score"],
            detail_transports=transports.get("detail", {}),
            evolution_annuelle_pct=dvf.get("evolution_annuelle_pct"),
            nb_transactions_dvf=dvf.get("nb_transactions", 0),
            iris=iris,
        ).compute(self.config.scoring)

        return score


def _log_result(score: CommuneScore) -> None:
    rdt = f"{score.rendement_brut}%" if score.rendement_brut else "N/A"
    prix = f"{score.prix_m2:,.0f} €/m²" if score.prix_m2 else "N/A"
    logger.info(f"  ✓ score={score.score_final}/10  rdt={rdt}  prix={prix}")
