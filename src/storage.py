"""
Storage v2 — PostgreSQL (Railway) + SQLite (local dev)
Détecte automatiquement via DATABASE_URL env var.
"""
from __future__ import annotations
import json, logging, os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from .scoring import CommuneScore

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_PG = bool(DATABASE_URL)


def _get_conn():
    if USE_PG:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        return conn, True
    else:
        import sqlite3
        data_dir = Path(os.environ.get("DATA_DIR", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(data_dir / "immo_idf.db")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn, False


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class Storage:

    def __init__(self):
        self._init_schema()

    def _init_schema(self) -> None:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            if pg:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS communes (
                        code_insee  TEXT PRIMARY KEY,
                        nom         TEXT NOT NULL,
                        data        JSONB NOT NULL,
                        updated_at  TEXT NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS iris (
                        code_iris     TEXT PRIMARY KEY,
                        code_commune  TEXT NOT NULL,
                        data          JSONB NOT NULL,
                        updated_at    TEXT NOT NULL
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_iris_commune ON iris (code_commune)")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS annonces (
                        id               TEXT PRIMARY KEY,
                        source           TEXT NOT NULL,
                        code_commune     TEXT NOT NULL,
                        nom_commune      TEXT,
                        titre            TEXT,
                        prix             INTEGER,
                        surface          REAL,
                        prix_m2          REAL,
                        delta_dvf_pct    REAL,
                        sous_cote        BOOLEAN DEFAULT FALSE,
                        prix_dvf_ref     REAL,
                        rendement_estime REAL,
                        type_bien        TEXT,
                        nb_pieces        INTEGER,
                        url              TEXT,
                        date_scraping    TEXT,
                        actif            BOOLEAN DEFAULT TRUE
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ann_commune ON annonces (code_commune)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ann_actif ON annonces (actif)")
            else:
                cur.executescript("""
                    CREATE TABLE IF NOT EXISTS communes (
                        code_insee TEXT PRIMARY KEY, nom TEXT NOT NULL,
                        data TEXT NOT NULL, updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS iris (
                        code_iris TEXT PRIMARY KEY, code_commune TEXT NOT NULL,
                        data TEXT NOT NULL, updated_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_iris_commune ON iris (code_commune);
                    CREATE TABLE IF NOT EXISTS annonces (
                        id TEXT PRIMARY KEY, source TEXT NOT NULL,
                        code_commune TEXT NOT NULL, nom_commune TEXT,
                        titre TEXT, prix INTEGER, surface REAL, prix_m2 REAL,
                        delta_dvf_pct REAL, sous_cote INTEGER DEFAULT 0,
                        prix_dvf_ref REAL, rendement_estime REAL,
                        type_bien TEXT, nb_pieces INTEGER, url TEXT,
                        date_scraping TEXT, actif INTEGER DEFAULT 1
                    );
                    CREATE INDEX IF NOT EXISTS idx_ann_commune ON annonces (code_commune);
                """)
            conn.commit()
        finally:
            conn.close()

    # ── Communes ─────────────────────────────────────────────────────────────

    def save_commune(self, score: CommuneScore) -> None:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            data = json.dumps(score.to_dict())
            if pg:
                cur.execute("""
                    INSERT INTO communes (code_insee, nom, data, updated_at)
                    VALUES (%s, %s, %s::jsonb, %s)
                    ON CONFLICT (code_insee) DO UPDATE
                    SET nom=EXCLUDED.nom, data=EXCLUDED.data, updated_at=EXCLUDED.updated_at
                """, (score.code_insee, score.nom, data, _now()))
            else:
                cur.execute(
                    "INSERT OR REPLACE INTO communes (code_insee,nom,data,updated_at) VALUES (?,?,?,?)",
                    (score.code_insee, score.nom, data, _now())
                )
            conn.commit()
        finally:
            conn.close()

    def get_all_communes(self) -> list[dict]:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            if pg:
                cur.execute("SELECT data FROM communes ORDER BY (data->>'score_final')::float DESC NULLS LAST")
            else:
                cur.execute("SELECT data FROM communes ORDER BY json_extract(data,'$.score_final') DESC")
            rows = cur.fetchall()
        finally:
            conn.close()
        return [json.loads(r[0]) if isinstance(r[0], str) else r[0] for r in rows]

    def get_commune(self, code_insee: str) -> dict | None:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            q = "SELECT data FROM communes WHERE code_insee=%s" if pg else "SELECT data FROM communes WHERE code_insee=?"
            cur.execute(q, (code_insee,))
            row = cur.fetchone()
        finally:
            conn.close()
        if not row: return None
        return json.loads(row[0]) if isinstance(row[0], str) else row[0]

    def delete_commune(self, code_insee: str) -> None:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            ph = "%s" if pg else "?"
            cur.execute(f"DELETE FROM communes WHERE code_insee={ph}", (code_insee,))
            cur.execute(f"DELETE FROM iris WHERE code_commune={ph}", (code_insee,))
            conn.commit()
        finally:
            conn.close()

    def get_dvf_prices(self) -> dict[str, float]:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            if pg:
                cur.execute("SELECT code_insee, (data->>'prix_m2')::float FROM communes")
            else:
                cur.execute("SELECT code_insee, json_extract(data,'$.prix_m2') FROM communes")
            rows = cur.fetchall()
        finally:
            conn.close()
        return {r[0]: r[1] for r in rows if r[1]}

    # ── IRIS ─────────────────────────────────────────────────────────────────

    def save_iris(self, code_commune: str, iris_list: list[dict]) -> None:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            ph = "%s" if pg else "?"
            cur.execute(f"DELETE FROM iris WHERE code_commune={ph}", (code_commune,))
            for iris in iris_list:
                if not iris.get("code_iris"): continue
                data = json.dumps(iris)
                if pg:
                    cur.execute(
                        "INSERT INTO iris (code_iris,code_commune,data,updated_at) VALUES (%s,%s,%s::jsonb,%s) ON CONFLICT (code_iris) DO UPDATE SET data=EXCLUDED.data",
                        (iris["code_iris"], code_commune, data, _now())
                    )
                else:
                    cur.execute(
                        "INSERT OR REPLACE INTO iris (code_iris,code_commune,data,updated_at) VALUES (?,?,?,?)",
                        (iris["code_iris"], code_commune, data, _now())
                    )
            conn.commit()
        finally:
            conn.close()

    def get_iris_by_commune(self, code_commune: str) -> list[dict]:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            if pg:
                cur.execute("SELECT data FROM iris WHERE code_commune=%s ORDER BY (data->>'revenu_median')::float DESC NULLS LAST", (code_commune,))
            else:
                cur.execute("SELECT data FROM iris WHERE code_commune=? ORDER BY json_extract(data,'$.revenu_median') DESC", (code_commune,))
            rows = cur.fetchall()
        finally:
            conn.close()
        return [json.loads(r[0]) if isinstance(r[0], str) else r[0] for r in rows]

    # ── Annonces ─────────────────────────────────────────────────────────────

    def save_annonce(self, ann: dict) -> bool:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            ph = "%s" if pg else "?"
            cur.execute(f"SELECT id FROM annonces WHERE id={ph}", (ann["id"],))
            existing = cur.fetchone()
            vals = (
                ann["id"], ann["source"], ann["code_commune"],
                ann.get("nom_commune"), ann.get("titre"),
                ann.get("prix"), ann.get("surface"), ann.get("prix_m2"),
                ann.get("delta_dvf_pct"), bool(ann.get("sous_cote")),
                ann.get("prix_dvf_ref"), ann.get("rendement_estime"),
                ann.get("type_bien"), ann.get("nb_pieces"),
                ann.get("url"), ann.get("date_scraping"),
            )
            if pg:
                cur.execute("""
                    INSERT INTO annonces
                    (id,source,code_commune,nom_commune,titre,prix,surface,prix_m2,
                     delta_dvf_pct,sous_cote,prix_dvf_ref,rendement_estime,
                     type_bien,nb_pieces,url,date_scraping,actif)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE)
                    ON CONFLICT (id) DO UPDATE SET
                        prix=EXCLUDED.prix, surface=EXCLUDED.surface,
                        prix_m2=EXCLUDED.prix_m2, delta_dvf_pct=EXCLUDED.delta_dvf_pct,
                        sous_cote=EXCLUDED.sous_cote, rendement_estime=EXCLUDED.rendement_estime,
                        date_scraping=EXCLUDED.date_scraping, actif=TRUE
                """, vals)
            else:
                cur.execute("""
                    INSERT OR REPLACE INTO annonces
                    (id,source,code_commune,nom_commune,titre,prix,surface,prix_m2,
                     delta_dvf_pct,sous_cote,prix_dvf_ref,rendement_estime,
                     type_bien,nb_pieces,url,date_scraping,actif)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                """, vals)
            conn.commit()
        finally:
            conn.close()
        return existing is None

    def get_annonces(self, code_commune=None, source=None, type_bien=None,
                     surface_min=None, surface_max=None, prix_max=None,
                     sous_cote_only=False, limit=200) -> list[dict]:
        conn, pg = _get_conn()
        ph = "%s" if pg else "?"
        where = ["actif=TRUE"] if pg else ["actif=1"]
        params = []
        if code_commune: where.append(f"code_commune={ph}"); params.append(code_commune)
        if source:        where.append(f"source={ph}");        params.append(source)
        if type_bien:     where.append(f"type_bien={ph}");     params.append(type_bien)
        if surface_min:   where.append(f"surface>={ph}");      params.append(surface_min)
        if surface_max:   where.append(f"surface<={ph}");      params.append(surface_max)
        if prix_max:      where.append(f"prix<={ph}");         params.append(prix_max)
        if sous_cote_only: where.append("sous_cote=TRUE" if pg else "sous_cote=1")
        params.append(limit)
        sql = f"""SELECT id,source,code_commune,nom_commune,titre,prix,surface,prix_m2,
                         delta_dvf_pct,sous_cote,prix_dvf_ref,rendement_estime,
                         type_bien,nb_pieces,url,date_scraping
                  FROM annonces WHERE {' AND '.join(where)}
                  ORDER BY sous_cote DESC, delta_dvf_pct ASC LIMIT {ph}"""
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            conn.close()
        cols = ["id","source","code_commune","nom_commune","titre","prix","surface",
                "prix_m2","delta_dvf_pct","sous_cote","prix_dvf_ref","rendement_estime",
                "type_bien","nb_pieces","url","date_scraping"]
        return [dict(zip(cols, r)) for r in rows]

    def get_annonces_stats(self) -> dict:
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            t = "TRUE" if pg else "1"
            cur.execute(f"SELECT COUNT(*) FROM annonces WHERE actif={t}")
            total = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM annonces WHERE actif={t} AND sous_cote={t}")
            sous_cote = cur.fetchone()[0]
            cur.execute("SELECT MAX(date_scraping) FROM annonces")
            last = cur.fetchone()[0]
            cur.execute(f"SELECT source, COUNT(*) FROM annonces WHERE actif={t} GROUP BY source")
            by_source = cur.fetchall()
        finally:
            conn.close()
        return {"total": total, "sous_cote": sous_cote, "dernier_scraping": last,
                "par_source": {r[0]: r[1] for r in by_source}}

    def deactivate_old_annonces(self, hours=36) -> None:
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat(timespec="seconds")
        conn, pg = _get_conn()
        try:
            cur = conn.cursor()
            ph = "%s" if pg else "?"
            f = "FALSE" if pg else "0"
            cur.execute(f"UPDATE annonces SET actif={f} WHERE date_scraping < {ph}", (cutoff,))
            conn.commit()
        finally:
            conn.close()
