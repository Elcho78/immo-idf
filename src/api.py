"""
API FastAPI — sert les données au dashboard React.

Endpoints :
  GET  /communes                  → liste toutes les communes scorées
  GET  /communes/{code}           → détail d'une commune
  GET  /communes/{code}/iris      → quartiers IRIS d'une commune
  GET  /config                    → configuration actuelle
  POST /config/communes           → ajoute une commune
  DELETE /config/communes/{code}  → retire une commune
  PATCH /config/communes/{code}   → active / désactive
  POST /pipeline/run              → relance le pipeline
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .config import Config
from .pipeline import Pipeline
from .storage import Storage

logger = logging.getLogger(__name__)


# ── Scheduler (remplace systemd timers sur Railway) ──────────────────────────

from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

scheduler = AsyncIOScheduler()

def _scheduled_scraping():
    try:
        from .sources.annonces import run_scraping
        cfg = _config()
        run_scraping(cfg)
    except Exception as e:
        logger.error(f"Scheduled scraping : {e}", exc_info=True)

def _scheduled_pipeline():
    try:
        from .pipeline import Pipeline
        cfg = _config()
        Pipeline(cfg).run()
    except Exception as e:
        logger.error(f"Scheduled pipeline : {e}", exc_info=True)

@asynccontextmanager
async def lifespan(app):
    # Scraping LBC toutes les 12h
    scheduler.add_job(_scheduled_scraping, IntervalTrigger(hours=12),
                      id="scraping", replace_existing=True)
    # Pipeline DVF toutes les semaines
    scheduler.add_job(_scheduled_pipeline, IntervalTrigger(weeks=1),
                      id="pipeline", replace_existing=True)
    scheduler.start()
    logger.info("Scheduler démarré (scraping 12h, pipeline 7j)")
    yield
    scheduler.shutdown()

app = FastAPI(title="IMMO·IDF", version="1.0.0", docs_url="/docs", lifespan=lifespan)

# Servir le dashboard HTML
@app.get("/")
async def root():
    return FileResponse("dashboard.html")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_storage = Storage()
_config_path = "config.yml"


def _config() -> Config:
    return Config.load(_config_path)


# ── Communes (données scorées) ───────────────────────────────────────────────

@app.get("/communes")
def list_communes():
    """Retourne toutes les communes analysées, triées par score décroissant."""
    return _storage.get_all_communes()


@app.get("/communes/{code_insee}")
def get_commune(code_insee: str):
    commune = _storage.get_commune(code_insee)
    if not commune:
        raise HTTPException(404, f"Commune {code_insee} non trouvée dans la base.")
    return commune


@app.get("/communes/{code_insee}/iris")
def get_iris(code_insee: str):
    """Données IRIS sub-communales pour une commune."""
    return _storage.get_iris_by_commune(code_insee)


# ── Configuration ────────────────────────────────────────────────────────────

@app.get("/config")
def get_config():
    cfg = _config()
    return {
        "communes": [
            {"code_insee": c.code_insee, "nom": c.nom, "actif": c.actif}
            for c in cfg.communes
        ],
        "parametres": cfg.parametres.__dict__,
        "scoring": cfg.scoring.__dict__,
    }


class CommunePayload(BaseModel):
    code_insee: str
    nom: str
    actif: bool = True


@app.post("/config/communes", status_code=201)
def add_commune(payload: CommunePayload, bg: BackgroundTasks):
    """
    Ajoute une commune à config.yml et relance le pipeline
    pour cette commune en arrière-plan.
    """
    cfg = _config()
    try:
        cfg.add_commune(payload.code_insee, payload.nom)
    except ValueError as e:
        raise HTTPException(409, str(e))

    bg.add_task(_run_single, payload.code_insee)
    return {"message": f"{payload.nom} ({payload.code_insee}) ajoutée, pipeline en cours…"}


@app.delete("/config/communes/{code_insee}")
def remove_commune(code_insee: str):
    """Retire une commune de config.yml et de la base."""
    cfg = _config()
    try:
        cfg.remove_commune(code_insee)
    except ValueError as e:
        raise HTTPException(404, str(e))
    _storage.delete_commune(code_insee)
    return {"message": f"Commune {code_insee} retirée."}


class TogglePayload(BaseModel):
    actif: bool


@app.patch("/config/communes/{code_insee}")
def toggle_commune(code_insee: str, payload: TogglePayload):
    """Active ou désactive une commune sans la supprimer."""
    cfg = _config()
    try:
        cfg.toggle_commune(code_insee, payload.actif)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"message": f"Commune {code_insee} → actif={payload.actif}"}


# ── Pipeline ─────────────────────────────────────────────────────────────────

class PipelinePayload(BaseModel):
    force_refresh: bool = False
    code_insee: Optional[str] = None


@app.post("/pipeline/run")
def run_pipeline(payload: PipelinePayload, bg: BackgroundTasks):
    """Lance le pipeline en arrière-plan."""
    bg.add_task(_run_pipeline, payload.force_refresh, payload.code_insee)
    msg = f"Pipeline démarré"
    if payload.code_insee:
        msg += f" pour {payload.code_insee}"
    return {"message": msg + " (arrière-plan)"}


# ── Recherche INSEE ──────────────────────────────────────────────────────────

@app.get("/search/commune")
async def search_commune(q: str):
    """
    Recherche de communes par nom via l'API géo officielle.
    Utile pour trouver un code INSEE avant d'ajouter une commune.
    """
    try:
        resp = httpx.get(
            "https://geo.api.gouv.fr/communes",
            params={"nom": q, "fields": "nom,code,departement", "boost": "population", "limit": 10},
            timeout=10,
        )
        resp.raise_for_status()
        return [
            {
                "code_insee": c["code"],
                "nom": c["nom"],
                "departement": c.get("departement", {}).get("nom", ""),
                "code_dept": c.get("departement", {}).get("code", ""),
            }
            for c in resp.json()
        ]
    except Exception as e:
        raise HTTPException(503, f"Erreur API géo : {e}")


# ── Tâches arrière-plan ──────────────────────────────────────────────────────

def _run_pipeline(force_refresh: bool, code_insee: Optional[str]) -> None:
    try:
        cfg = _config()
        pipeline = Pipeline(cfg)
        pipeline.run(force_refresh=force_refresh, code_insee_filtre=code_insee)
    except Exception as e:
        logger.error(f"Pipeline erreur : {e}", exc_info=True)


def _run_single(code_insee: str) -> None:
    _run_pipeline(force_refresh=True, code_insee=code_insee)


# ── Annonces ─────────────────────────────────────────────────────────────────

@app.get("/annonces")
def get_annonces(
    code_commune: str = None,
    source: str = None,
    type_bien: str = None,
    surface_min: float = None,
    surface_max: float = None,
    prix_max: int = None,
    sous_cote_only: bool = False,
    limit: int = 200,
):
    """Liste des annonces avec filtres dynamiques."""
    return _storage.get_annonces(
        code_commune=code_commune,
        source=source,
        type_bien=type_bien,
        surface_min=surface_min,
        surface_max=surface_max,
        prix_max=prix_max,
        sous_cote_only=sous_cote_only,
        limit=limit,
    )


@app.get("/annonces/stats")
def get_annonces_stats():
    """Statistiques globales des annonces scrapées."""
    return _storage.get_annonces_stats()


class ScrapingPayload(BaseModel):
    force: bool = False


@app.post("/annonces/scrape")
def trigger_scraping(payload: ScrapingPayload, bg: BackgroundTasks):
    """Lance le scraping des annonces en arrière-plan."""
    bg.add_task(_run_scraping, payload.force)
    return {"message": "Scraping lancé en arrière-plan"}


def _run_scraping(force: bool = False) -> None:
    try:
        from .sources.annonces import run_scraping
        cfg = _config()
        run_scraping(cfg, force=force)
    except Exception as e:
        logger.error(f"Scraping annonces : {e}", exc_info=True)
