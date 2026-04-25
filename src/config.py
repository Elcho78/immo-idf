"""
Configuration — chargement et validation du fichier config.yml
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List
import yaml


@dataclass
class CommuneConfig:
    code_insee: str
    nom: str
    actif: bool = True

    def __post_init__(self):
        self.code_insee = str(self.code_insee).zfill(5)


@dataclass
class PipelineParams:
    surface_reference_m2: int = 50
    charges_locatives_pct: float = 25.0
    frais_acquisition_pct: float = 8.0
    cache_ttl_jours: int = 7
    annees_dvf: int = 2
    rayon_transports_m: int = 800


@dataclass
class ScoringWeights:
    poids_rendement: float = 0.50
    poids_tension_locative: float = 0.30
    poids_transports: float = 0.20

    def __post_init__(self):
        total = self.poids_rendement + self.poids_tension_locative + self.poids_transports
        if abs(total - 1.0) > 0.005:
            raise ValueError(
                f"Les poids de scoring doivent sommer à 1.0 (total actuel : {total:.3f})"
            )


@dataclass
class Config:
    communes: List[CommuneConfig]
    parametres: PipelineParams = field(default_factory=PipelineParams)
    scoring: ScoringWeights = field(default_factory=ScoringWeights)
    _path: str = field(default="config.yml", repr=False)

    @property
    def communes_actives(self) -> List[CommuneConfig]:
        return [c for c in self.communes if c.actif]

    def add_commune(self, code_insee: str, nom: str) -> None:
        """Ajoute une commune et sauvegarde le fichier."""
        if any(c.code_insee == code_insee for c in self.communes):
            raise ValueError(f"La commune {code_insee} ({nom}) est déjà présente.")
        self.communes.append(CommuneConfig(code_insee=code_insee, nom=nom, actif=True))
        self._save()

    def remove_commune(self, code_insee: str) -> None:
        """Retire une commune et sauvegarde le fichier."""
        before = len(self.communes)
        self.communes = [c for c in self.communes if c.code_insee != code_insee]
        if len(self.communes) == before:
            raise ValueError(f"Commune {code_insee} introuvable dans la configuration.")
        self._save()

    def toggle_commune(self, code_insee: str, actif: bool) -> None:
        """Active ou désactive une commune sans la supprimer."""
        for c in self.communes:
            if c.code_insee == code_insee:
                c.actif = actif
                self._save()
                return
        raise ValueError(f"Commune {code_insee} introuvable.")

    def _save(self) -> None:
        """Réécrit config.yml en préservant la structure."""
        data = {
            "communes": [
                {"code_insee": c.code_insee, "nom": c.nom, "actif": c.actif}
                for c in self.communes
            ],
            "parametres": {
                "surface_reference_m2": self.parametres.surface_reference_m2,
                "charges_locatives_pct": self.parametres.charges_locatives_pct,
                "frais_acquisition_pct": self.parametres.frais_acquisition_pct,
                "cache_ttl_jours": self.parametres.cache_ttl_jours,
                "annees_dvf": self.parametres.annees_dvf,
                "rayon_transports_m": self.parametres.rayon_transports_m,
            },
            "scoring": {
                "poids_rendement": self.scoring.poids_rendement,
                "poids_tension_locative": self.scoring.poids_tension_locative,
                "poids_transports": self.scoring.poids_transports,
            },
        }
        with open(self._path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    @classmethod
    def load(cls, path: str = "config.yml") -> Config:
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        communes = [CommuneConfig(**c) for c in raw.get("communes", [])]
        params = PipelineParams(**raw.get("parametres", {}))
        scoring = ScoringWeights(**raw.get("scoring", {}))

        return cls(communes=communes, parametres=params, scoring=scoring, _path=path)
