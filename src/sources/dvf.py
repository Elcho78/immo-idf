"""
Source DVF — fichiers officiels geo-dvf (data.gouv.fr / Etalab)
Remplace api.cquest.org (instable).
URL : https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz
"""
from __future__ import annotations
import gzip, io, json, logging, time
from datetime import datetime
from pathlib import Path
from typing import Optional
import httpx
import pandas as pd

logger = logging.getLogger(__name__)
CACHE_DIR = Path("data/cache/dvf")
DVF_BASE = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz"
ANNEES = [2024, 2023, 2022]

def get_prix_commune(code_insee: str, annees: int = 2, cache_ttl_jours: int = 7) -> dict:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{code_insee}.json"
    if _cache_valide(cache_file, cache_ttl_jours):
        logger.debug(f"DVF {code_insee} — cache hit")
        return json.loads(cache_file.read_text())

    dept = code_insee[:3] if code_insee.startswith("97") else code_insee[:2]
    logger.info(f"DVF {code_insee} — téléchargement dept {dept}")
    dfs = []
    for annee in ANNEES[:annees + 1]:
        df = _download_dept(dept, annee)
        if df is not None and not df.empty:
            dfs.append(df)

    if not dfs:
        logger.warning(f"DVF {code_insee} — aucun fichier")
        return _dvf_vide()

    df_all = pd.concat(dfs, ignore_index=True)
    col_com = _col(df_all, ["code_commune", "codecommune"])
    if not col_com:
        return _dvf_vide()

    df = df_all[df_all[col_com].astype(str) == code_insee].copy()
    col_type = _col(df, ["type_local", "typelocal"])
    if col_type:
        apts = df[df[col_type].astype(str).str.lower().str.contains("appartement", na=False)]
        if not apts.empty:
            df = apts
        else:
            mais = df[df[col_type].astype(str).str.lower().str.contains("maison", na=False)]
            if not mais.empty:
                df = mais
                logger.info(f"DVF {code_insee} — fallback maisons ({len(mais)} trans.)")

    if df.empty:
        logger.warning(f"DVF {code_insee} — aucune transaction appartement")
        return _dvf_vide()

    col_val = _col(df, ["valeur_fonciere", "valeurfonciere"])
    col_surf = _col(df, ["surface_reelle_bati", "surfacereellebati"])
    col_date = _col(df, ["date_mutation", "datemutation"])
    if not col_val or not col_surf:
        return _dvf_vide()

    df[col_val] = pd.to_numeric(df[col_val], errors="coerce")
    df[col_surf] = pd.to_numeric(df[col_surf], errors="coerce")
    df = df[df[col_surf].notna() & (df[col_surf] > 9) & df[col_val].notna() & (df[col_val] > 10000)]
    df["prix_m2"] = df[col_val] / df[col_surf]
    q05, q95 = df["prix_m2"].quantile([0.05, 0.95])
    df = df[(df["prix_m2"] >= q05) & (df["prix_m2"] <= q95)]
    df = df[df["prix_m2"] <= 12000]  # plafond IDF — élimine locaux d'activité/entrepôts
    if df.empty:
        return _dvf_vide()

    result = {
        "prix_m2_median": int(df["prix_m2"].median()),
        "prix_m2_moyen": int(df["prix_m2"].mean()),
        "prix_m2_p25": int(df["prix_m2"].quantile(0.25)),
        "prix_m2_p75": int(df["prix_m2"].quantile(0.75)),
        "nb_transactions": len(df),
        "evolution_annuelle_pct": None,
        "distribution_postal": [],
    }
    if col_date:
        try:
            df["annee"] = pd.to_datetime(df[col_date], errors="coerce").dt.year
            an = int(df["annee"].max())
            m_n  = df[df["annee"] == an]["prix_m2"].median()
            m_n1 = df[df["annee"] == an - 1]["prix_m2"].median()
            if pd.notna(m_n) and pd.notna(m_n1) and m_n1 > 0:
                result["evolution_annuelle_pct"] = round((m_n - m_n1) / m_n1 * 100, 1)
        except Exception:
            pass

    cache_file.write_text(json.dumps(result))
    logger.info(f"DVF {code_insee} — {result['nb_transactions']} transactions, {result['prix_m2_median']} €/m²")
    return result

def _download_dept(dept: str, annee: int) -> Optional[pd.DataFrame]:
    cache_pkl = CACHE_DIR / f"dept_{dept}_{annee}.pkl"
    if cache_pkl.exists() and _cache_valide(cache_pkl, 30):
        return pd.read_pickle(cache_pkl)
    url = DVF_BASE.format(year=annee, dept=dept)
    try:
        resp = httpx.get(url, timeout=180, follow_redirects=True)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        with gzip.open(io.BytesIO(resp.content)) as gz:
            df = pd.read_csv(gz, dtype=str, low_memory=False)
        df.to_pickle(cache_pkl)
        logger.info(f"DVF dept {dept} {annee} — {len(df)} lignes")
        return df
    except Exception as e:
        logger.warning(f"DVF dept {dept} {annee} — {e}")
        return None

def _col(df, candidates):
    low = {c.lower(): c for c in df.columns}
    for n in candidates:
        if n.lower() in low:
            return low[n.lower()]
    return None

def _cache_valide(path, ttl):
    return path.exists() and (time.time() - path.stat().st_mtime) < ttl * 86400

def _dvf_vide():
    return {"prix_m2_median": None, "prix_m2_moyen": None, "prix_m2_p25": None,
            "prix_m2_p75": None, "nb_transactions": 0, "evolution_annuelle_pct": None, "distribution_postal": []}
