"""
Source Transports — IDFM open data v4
Fix SSL : verify=False pour contourner TLS alert sur data.iledefrance-mobilites.fr
Stratégie : téléchargement unique CSV → cache → haversine local
"""
from __future__ import annotations
import io, json, logging, math, time
from pathlib import Path
from typing import Optional
import httpx
import pandas as pd

logger = logging.getLogger(__name__)
CACHE_DIR       = Path("data/cache/transports")
GEO_API         = "https://geo.api.gouv.fr/communes"
CACHE_STOPS_PKL = CACHE_DIR / "stops_idfm.pkl"
CACHE_TTL_STOPS = 30

IDFM_BASE = "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/csv"
IDFM_DATASETS = [
    ("zones-d-arrets",      "TransportMode"),
    ("arrets-transporteur", "TransportMode"),
    ("arrets",              "TransportMode"),
]
MODE_POIDS = {
    "Metro":3.0,"RER":2.5,"Train":2.0,"Rail":2.0,
    "Tramway":1.5,"Bus":0.25,"Coach":0.2,"Navette":0.1,
}

def get_score_transports(code_insee:str, rayon_m:int=800, cache_ttl_jours:int=30) -> dict:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{code_insee}.json"
    if _cv(cache_file, cache_ttl_jours):
        return json.loads(cache_file.read_text())

    lon, lat = _get_centroide(code_insee)
    if lon is None:
        return _vide()

    stops_df = _load_stops()
    if stops_df is None or stops_df.empty:
        result = _vide()
        cache_file.write_text(json.dumps(result))
        return result

    nearby = _filter_radius(stops_df, lat, lon, rayon_m)
    detail: dict[str,int] = {}
    for _, row in nearby.iterrows():
        m = str(row.get("mode","Bus"))
        detail[m] = detail.get(m,0)+1

    score_brut = sum(n*MODE_POIDS.get(m,0.1) for m,n in detail.items())
    score = round(min(score_brut/8.0,10.0),1)
    result = {"score":score,"nb_arrets":len(nearby),"detail":detail,
              "centroide":{"lon":lon,"lat":lat},"rayon_m":rayon_m}
    cache_file.write_text(json.dumps(result))
    logger.info(f"Transports {code_insee} — score={score} nb_arrets={len(nearby)}")
    return result

def _load_stops() -> Optional[pd.DataFrame]:
    if CACHE_STOPS_PKL.exists() and _cv(CACHE_STOPS_PKL, CACHE_TTL_STOPS):
        return pd.read_pickle(CACHE_STOPS_PKL)
    for dataset, col_mode in IDFM_DATASETS:
        df = _dl_stops(dataset, col_mode)
        if df is not None and not df.empty:
            df.to_pickle(CACHE_STOPS_PKL)
            logger.info(f"Stops IDFM : {len(df)} arrêts (dataset={dataset})")
            return df
    logger.error("Stops IDFM : tous les datasets ont échoué")
    return None

def _dl_stops(dataset:str, col_mode:str) -> Optional[pd.DataFrame]:
    url = IDFM_BASE.format(dataset=dataset)
    try:
        # verify=False pour contourner TLSV1_ALERT_INTERNAL_ERROR
        resp = httpx.get(url, params={"lang":"fr","delimiter":";"},
                         timeout=120, verify=False)
        if resp.status_code in (404,400):
            return None
        resp.raise_for_status()
        df = pd.read_csv(io.BytesIO(resp.content), sep=";", dtype=str, low_memory=False)
        if df.empty: return None

        lat_col = _col(df,["ZdALat","Latitude","lat","latitude","stop_lat"])
        lon_col = _col(df,["ZdALong","ZdALon","Longitude","lon","longitude","stop_lon"])
        geo_col = _col(df,["geo_point_2d","Geopoint","geopunt"])

        if lat_col and lon_col:
            df["_lat"] = pd.to_numeric(df[lat_col], errors="coerce")
            df["_lon"] = pd.to_numeric(df[lon_col], errors="coerce")
        elif geo_col:
            coords = df[geo_col].str.split(",", expand=True)
            if coords.shape[1]>=2:
                c0=pd.to_numeric(coords[0],errors="coerce")
                c1=pd.to_numeric(coords[1],errors="coerce")
                if c0.median()>10: df["_lat"],df["_lon"]=c0,c1
                else:              df["_lat"],df["_lon"]=c1,c0
        else:
            logger.warning(f"IDFM {dataset} — pas de colonnes géo")
            return None

        mc = _col(df,[col_mode,"TransportMode","mode","type","mode_transport"])
        df["mode"] = df[mc].str.strip() if mc else "Bus"
        result = df[["_lat","_lon","mode"]].dropna(subset=["_lat","_lon"])
        logger.info(f"IDFM {dataset} — {len(result)} stops")
        return result
    except Exception as e:
        logger.warning(f"IDFM {dataset} — {e}")
        return None

def _filter_radius(df, lat, lon, rayon_m):
    R=6_371_000
    dlat=(df["_lat"]-lat).apply(math.radians)
    dlon=(df["_lon"]-lon).apply(math.radians)
    a=dlat.apply(lambda x:math.sin(x/2)**2)+\
      math.cos(math.radians(lat))*\
      df["_lat"].apply(lambda x:math.cos(math.radians(x)))*\
      dlon.apply(lambda x:math.sin(x/2)**2)
    dist=R*2*a.apply(lambda x:math.asin(min(1,math.sqrt(x))))
    return df[dist<=rayon_m].copy()

def _get_centroide(code_insee:str):
    cache_f=CACHE_DIR/f"geo_{code_insee}.json"
    if cache_f.exists():
        d=json.loads(cache_f.read_text()); return d["lon"],d["lat"]
    try:
        resp=httpx.get(f"{GEO_API}/{code_insee}",params={"fields":"centre"},timeout=10)
        resp.raise_for_status()
        coords=resp.json().get("centre",{}).get("coordinates",[None,None])
        lon,lat=float(coords[0]),float(coords[1])
        cache_f.write_text(json.dumps({"lon":lon,"lat":lat}))
        return lon,lat
    except Exception as e:
        logger.error(f"Géocodage {code_insee} : {e}"); return None,None

def _col(df,candidates):
    low={c.lower():c for c in df.columns}
    for n in candidates:
        if n.lower() in low: return low[n.lower()]
    return None

def _cv(path,ttl):
    return Path(path).exists() and (time.time()-Path(path).stat().st_mtime)<ttl*86400

def _vide():
    return {"score":5.0,"nb_arrets":0,"detail":{},"centroide":None,"rayon_m":0}
