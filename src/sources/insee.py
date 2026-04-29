from __future__ import annotations
import os as _os
_DATA_ROOT = _os.environ.get("DATA_DIR", "data")
"""
Source INSEE v5
- RP2020 LOGEMENT individuel → agrégation par commune si nécessaire
- Sentinel 24h si INSEE down
- Filosofi : toujours en attente du retour du serveur INSEE
"""
import io, json, logging, time, zipfile
from pathlib import Path
from typing import Optional
import httpx
import pandas as pd

logger = logging.getLogger(__name__)
CACHE_DIR = Path(_os.environ.get("DATA_DIR","data")) / "cache" / "insee"

FILO_COM_URLS = [
    "https://www.insee.fr/fr/statistiques/fichier/7756855/FILO2021_DEC_COM.xlsx",
    "https://api.insee.fr/melodi/file/FILOSOFI_COM_2021/FILOSOFI_COM_2021_CSV_FR",
    "https://www.insee.fr/fr/statistiques/fichier/6692220/FILO2020_DEC_COM.xlsx",
    "__DATAGOUV_FILO__",
]
FILO_IRIS_URLS = [
    "https://www.insee.fr/fr/statistiques/fichier/8229323/FILO2021_DEC_IRIS.xlsx",
    "https://www.insee.fr/fr/statistiques/fichier/7756856/FILO2021_DEC_IRIS.xlsx",
]
RP_COM_URLS = [
    # Fichier individuel logements → agrégation communale
    "https://www.insee.fr/fr/statistiques/fichier/7705908/RP2020_LOGEMT_csv.zip",
    # Résumé commune (si le serveur revient)
    "https://www.insee.fr/fr/statistiques/fichier/6543200/rp2020_com_txt.zip",
    "__DATAGOUV_RP__",
]
DATAGOUV_ORG = "534fff81a3a7292c64a77e5c"

# ── API publique ─────────────────────────────────────────────────────────────

def get_revenus_commune(code_insee:str) -> dict:
    df = _load("filosofi_commune", FILO_COM_URLS)
    if df is None: return {}
    col_geo = _col(df,["CODGEO","codgeo","COM","com","Code géographique"])
    if not col_geo: return {}
    row = df[df[col_geo].astype(str)==code_insee]
    if row.empty: return {}
    r = row.iloc[0]
    return {
        "revenu_median": _int(_find(r,["Q2","MED21","MED20","DISP_MED21"])),
        "revenu_d1":     _int(_find(r,["D1","D121"])),
        "revenu_d9":     _int(_find(r,["D9","D921"])),
        "taux_pauvrete": _float(_find(r,["TP60","TP6021","TP6020"])),
        "gini":          _float(_find(r,["GI","GI21","GI20"])),
    }

def get_revenus_iris(code_commune:str) -> list[dict]:
    df = _load("filosofi_iris", FILO_IRIS_URLS)
    if df is None: return []
    col_iris = _col(df,["IRIS","iris","CODEIRIS"])
    if not col_iris: return []
    mask = df[col_iris].astype(str).str.startswith(str(code_commune))
    result = []
    for _,row in df[mask].iterrows():
        code = str(row.get(col_iris,""))
        result.append({
            "code_iris":code,
            "nom_iris": str(row.get("LIBIRIS",f"Zone {code[-4:]}")),
            "revenu_median": _int(_find(row,["DISP_MED21","DISP_MED20","Q2"])),
            "taux_pauvrete": _float(_find(row,["TP6021","TP6020","TP60"])),
            "population_menages": _int(_find(row,["NBPERSM21","NBPERSM20"])),
            "part_menages_bas_revenus": None,
        })
    return sorted(result, key=lambda x: x["revenu_median"] or 0, reverse=True)

def get_demographie_commune(code_insee:str) -> dict:
    df = _load("rp_commune", RP_COM_URLS)
    if df is None: return {}
    col_com = _col(df,["COM","com","CODGEO","codgeo"])
    if not col_com: return {}
    row = df[df[col_com].astype(str)==code_insee]
    if row.empty: return {}
    r = row.iloc[0]
    pop  = _float(_find(r,["P21_POP","P20_POP","pop"])) or 1
    rp   = _float(_find(r,["P21_RP","P20_RP","rp","nb_rp"])) or 1
    log  = _float(_find(r,["P21_LOG","P20_LOG","nb_log"])) or 1
    loc  = _float(_find(r,["P21_RP_LOC","P20_RP_LOC","nb_loc"])) or 0
    prop = _float(_find(r,["P21_RP_PROP","P20_RP_PROP","nb_prop"])) or 0
    vac  = _float(_find(r,["P21_LOGVAC","P20_LOGVAC","nb_vac"])) or 0
    p15  = _float(_find(r,["P21_POP1529","P20_POP1529"])) or 0
    return {
        "population": int(pop),
        "part_locataires":       round(loc/rp*100,1),
        "part_proprietaires":    round(prop/rp*100,1),
        "taux_vacance_locative": round(vac/log*100,1),
        "part_moins_30": round(p15/pop*100,1) if pop>0 else None,
        "pop_15_29": int(p15),
    }

# ── Loader ───────────────────────────────────────────────────────────────────

def _load(name:str, urls:list[str]) -> Optional[pd.DataFrame]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pkl  = CACHE_DIR/f"{name}.pkl"
    sntl = CACHE_DIR/f"{name}.sentinel"
    if pkl.exists(): return pd.read_pickle(pkl)
    if sntl.exists() and _cv(sntl,1):
        logger.debug(f"{name} — sentinel actif"); return None

    for url in _resolve(urls):
        df = _try(name, url)
        if df is not None and not df.empty:
            df.to_pickle(pkl)
            logger.info(f"  ✓ {name} — {len(df)} lignes")
            return df

    logger.error(f"{name} : tout échoué — sentinel 24h")
    sntl.write_text(str(time.time()))
    return None

def _try(name:str, url:str) -> Optional[pd.DataFrame]:
    try:
        logger.info(f"Téléchargement {name} : {url[:72]}…")
        resp = httpx.get(url, timeout=900, follow_redirects=True, verify=False)
        resp.raise_for_status()
        content = resp.content

        # Cas spécial : fichier RP individuel (>1M lignes) → agréger
        if "LOGEMT" in url or "logemt" in url.lower():
            df = _parse_content(content)
            if df is not None and len(df) > 500_000:
                logger.info(f"  Agrégation RP individuel → commune ({len(df):,} lignes)…")
                return _agreger_rp_logement(df)
            return df

        # xlsx
        for sheet in ["ENSEMBLE","Sheet1",0]:
            try:
                df = pd.read_excel(io.BytesIO(content), sheet_name=sheet, dtype=str)
                if not df.empty: return df
            except Exception: continue

        return _parse_content(content)

    except Exception as e:
        logger.warning(f"{name} {url[:60]} — {e}")
    return None

def _parse_content(content:bytes) -> Optional[pd.DataFrame]:
    # ZIP
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            csv_name = next((n for n in z.namelist()
                             if n.endswith((".csv",".txt"))), None)
            if csv_name:
                raw = z.read(csv_name)
                sep = ";" if b";" in raw[:2000] else ","
                return pd.read_csv(io.BytesIO(raw), sep=sep, dtype=str, low_memory=False)
    except Exception: pass
    # CSV direct
    try:
        sep = ";" if b";" in content[:2000] else ","
        return pd.read_csv(io.BytesIO(content), sep=sep, dtype=str, low_memory=False)
    except Exception: pass
    return None

def _agreger_rp_logement(df:pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Agrège le fichier individuel RP logements en résumé commune.
    Colonnes attendues : COM, CATL (1=RP), STAT (1=prop,2=HLM,3=loc,4=gratuit), IPONDI
    """
    try:
        col_com  = _col(df, ["COM","com","CODECOM"])
        col_catl = _col(df, ["CATL","catl"])        # 1 = résidence principale
        col_stat = _col(df, ["STOCD","stocd","STAT"]) # statut occupation
        col_pond = _col(df, ["IPONDI","ipondi","POIDS"])

        if not col_com:
            logger.warning("Agrégation RP : colonne commune introuvable")
            return None

        df[col_com] = df[col_com].astype(str).str.zfill(5)

        # Pondération
        if col_pond:
            df["_w"] = pd.to_numeric(df[col_pond], errors="coerce").fillna(1)
        else:
            df["_w"] = 1.0

        # Résidences principales uniquement
        if col_catl:
            df["_rp"] = pd.to_numeric(df[col_catl], errors="coerce") == 1
        else:
            df["_rp"] = True

        rp = df[df["_rp"]].copy()
        agg = rp.groupby(col_com)["_w"].sum().reset_index()
        agg.columns = ["COM","P20_RP"]

        if col_stat:
            rp["_stat"] = pd.to_numeric(rp[col_stat], errors="coerce")
            loc  = rp[rp["_stat"].isin([2,3])].groupby(col_com)["_w"].sum().reset_index()
            prop = rp[rp["_stat"]==1].groupby(col_com)["_w"].sum().reset_index()
            loc.columns  = ["COM","P20_RP_LOC"]
            prop.columns = ["COM","P20_RP_PROP"]
            agg = agg.merge(loc,  on="COM", how="left")
            agg = agg.merge(prop, on="COM", how="left")
        else:
            agg["P20_RP_LOC"]  = None
            agg["P20_RP_PROP"] = None

        # Log vacants (CATL == 3)
        if col_catl:
            vac = df[pd.to_numeric(df[col_catl],errors="coerce")==3].groupby(col_com)["_w"].sum().reset_index()
            vac.columns = ["COM","P20_LOGVAC"]
            all_log = df.groupby(col_com)["_w"].sum().reset_index()
            all_log.columns = ["COM","P20_LOG"]
            agg = agg.merge(vac,     on="COM", how="left")
            agg = agg.merge(all_log, on="COM", how="left")

        agg = agg.fillna(0)
        logger.info(f"  Agrégation RP → {len(agg)} communes")
        return agg

    except Exception as e:
        logger.error(f"Agrégation RP : {e}")
        return None

def _resolve(urls:list[str]) -> list[str]:
    result = []
    done = False
    for url in urls:
        if url.startswith("__DATAGOUV_") and not done:
            done = True
            try:
                tag = "recensement" if "RP" in url else "filosofi"
                r = httpx.get(
                    "https://www.data.gouv.fr/api/1/datasets/",
                    params={"q":f"{tag} communes 2021",
                            "organization":DATAGOUV_ORG,"page_size":5},
                    timeout=10)
                r.raise_for_status()
                for ds in r.json().get("data",[]):
                    for res in ds.get("resources",[])[:3]:
                        fu = res.get("url","")
                        if fu and any(fu.endswith(x) for x in (".csv",".zip",".xlsx",".txt")):
                            result.append(fu)
            except Exception as e:
                logger.debug(f"data.gouv : {e}")
        else:
            result.append(url)
    return result

# ── Helpers ──────────────────────────────────────────────────────────────────

def _col(df,candidates):
    low={c.lower():c for c in df.columns}
    for n in candidates:
        if n.lower() in low: return low[n.lower()]
    return None

def _find(row,candidates):
    idx={str(k).lower():v for k,v in row.items()}
    for n in candidates:
        if n.lower() in idx: return idx[n.lower()]
    return None

def _int(val) -> Optional[int]:
    try: return int(float(val)) if val not in (None,"","nan","s","nd") and str(val) not in ("","nan","s") else None
    except: return None

def _float(val) -> Optional[float]:
    try: return float(val) if val not in (None,"","nan","s","nd") and str(val) not in ("","nan","s") else None
    except: return None

def _cv(path,ttl):
    return Path(path).exists() and (time.time()-Path(path).stat().st_mtime)<ttl*86400
