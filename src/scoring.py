"""
Scoring investisseur — agrège toutes les données et calcule les scores
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .config import ScoringWeights


@dataclass
class CommuneScore:
    code_insee: str
    nom: str

    # Données brutes collectées par le pipeline
    prix_m2: Optional[float] = None
    prix_m2_p25: Optional[float] = None
    prix_m2_p75: Optional[float] = None
    loyer_m2_estime: Optional[float] = None
    revenu_median: Optional[float] = None
    taux_pauvrete: Optional[float] = None
    part_locataires: Optional[float] = None
    part_proprietaires: Optional[float] = None
    taux_vacance: Optional[float] = None
    part_moins_30: Optional[float] = None
    taux_chomage: Optional[float] = None
    score_transports: float = 5.0
    evolution_annuelle_pct: Optional[float] = None
    nb_transactions_dvf: int = 0
    detail_transports: dict = field(default_factory=dict)
    iris: list = field(default_factory=list)

    # Scores calculés
    rendement_brut: Optional[float] = None
    score_rendement: Optional[float] = None
    score_tension: Optional[float] = None
    score_final: Optional[float] = None
    radar: dict = field(default_factory=dict)

    def compute(self, weights: ScoringWeights) -> CommuneScore:
        """
        Calcule tous les scores à partir des données brutes.
        Doit être appelé après que toutes les données sont renseignées.
        """
        # ── Rendement brut ────────────────────────────────────────────
        if self.prix_m2 and self.loyer_m2_estime and self.prix_m2 > 0:
            self.rendement_brut = round(
                (self.loyer_m2_estime * 12 / self.prix_m2) * 100, 2
            )
            self.score_rendement = round(min(self.rendement_brut / 6.5, 1.0) * 10, 1)
        else:
            self.rendement_brut = None
            self.score_rendement = 0.0

        # ── Tension locative ─────────────────────────────────────────
        # Proxy : part de locataires (plus il y a de locataires, plus la tension est forte)
        if self.part_locataires is not None:
            # Normalisation : 30% → 3pts, 70% → 10pts
            self.score_tension = round(min(max(self.part_locataires / 7.0, 0), 10), 1)
        else:
            self.score_tension = 5.0

        # ── Score final pondéré ──────────────────────────────────────
        self.score_final = round(
            weights.poids_rendement * (self.score_rendement or 0)
            + weights.poids_tension_locative * self.score_tension
            + weights.poids_transports * self.score_transports,
            1,
        )

        # ── Radar 6 axes (0–10) ──────────────────────────────────────
        # Solvabilité : capacité locataires à payer (revenu médian normalisé)
        solv = 0.0
        if self.revenu_median:
            solv = round(max(0, min((self.revenu_median - 15000) / 40000, 1)) * 10, 1)

        # Accessibilité prix : ticket d'entrée pour l'investisseur (prix bas = meilleur)
        acc = 0.0
        if self.prix_m2:
            acc = round(max(0, (10000 - self.prix_m2) / 8000 * 10), 1)

        # Dynamisme marché : évolution des prix (positif = appréciation)
        dyn = 0.0
        if self.evolution_annuelle_pct is not None:
            dyn = round(min(max((self.evolution_annuelle_pct + 3) / 18 * 10, 0), 10), 1)

        self.radar = {
            "rendement": self.score_rendement or 0,
            "transports": round(self.score_transports, 1),
            "tension_locative": self.score_tension,
            "solvabilite_locataires": solv,
            "accessibilite_prix": acc,
            "dynamisme_marche": dyn,
        }

        return self

    def to_dict(self) -> dict:
        return {
            # Identité
            "code_insee": self.code_insee,
            "nom": self.nom,
            # Prix
            "prix_m2": self.prix_m2,
            "prix_m2_p25": self.prix_m2_p25,
            "prix_m2_p75": self.prix_m2_p75,
            "loyer_m2_estime": self.loyer_m2_estime,
            "rendement_brut": self.rendement_brut,
            # Socio-démographie
            "revenu_median": self.revenu_median,
            "taux_pauvrete": self.taux_pauvrete,
            "part_locataires": self.part_locataires,
            "part_proprietaires": self.part_proprietaires,
            "taux_vacance": self.taux_vacance,
            "part_moins_30": self.part_moins_30,
            "taux_chomage": self.taux_chomage,
            # Marché
            "evolution_annuelle_pct": self.evolution_annuelle_pct,
            "nb_transactions_dvf": self.nb_transactions_dvf,
            # Transports
            "score_transports": self.score_transports,
            "detail_transports": self.detail_transports,
            # Scores
            "score_rendement": self.score_rendement,
            "score_tension": self.score_tension,
            "score_final": self.score_final,
            "radar": self.radar,
            # IRIS
            "iris": self.iris,
        }
