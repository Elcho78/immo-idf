"""
Storage — persistance SQLite : communes, IRIS, annonces
"""
from __future__ import annotations
import json, sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from .scoring import CommuneScore

DB_PATH = Path("data/immo_idf.db")


class Storage:

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS communes (
                    code_insee  TEXT PRIMARY KEY,
                    nom         TEXT NOT NULL,
                    data        TEXT NOT NULL,
                    updated_at  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS iris (
                    code_iris     TEXT PRIMARY KEY,
                    code_commune  TEXT NOT NULL,
                    data          TEXT NOT NULL,
                    updated_at    TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_iris_commune ON iris (code_commune);

                CREATE TABLE IF NOT EXISTS annonces (
                    id              TEXT PRIMARY KEY,
                    source          TEXT NOT NULL,
                    code_commune    TEXT NOT NULL,
                    nom_commune     TEXT,
                    titre           TEXT,
                    prix            INTEGER,
                    surface         REAL,
                    prix_m2         REAL,
                    delta_dvf_pct   REAL,
                    sous_cote       INTEGER DEFAULT 0,
                    prix_dvf_ref    REAL,
                    rendement_estime REAL,
                    type_bien       TEXT,
                    nb_pieces       INTEGER,
                    url             TEXT,
                    date_scraping   TEXT,
                    actif           INTEGER DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_ann_commune ON annonces (code_commune);
                CREATE INDEX IF NOT EXISTS idx_ann_actif   ON annonces (actif);
                CREATE INDEX IF NOT EXISTS idx_ann_source  ON annonces (source);
            """)

    # ── Communes ─────────────────────────────────────────────────────────────

    def save_commune(self, score: CommuneScore) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO communes (code_insee,nom,data,updated_at) VALUES (?,?,?,?)",
                (score.code_insee, score.nom, json.dumps(score.to_dict()), _now()),
            )

    def get_all_communes(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT data FROM communes ORDER BY json_extract(data,'$.score_final') DESC"
            ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def get_commune(self, code_insee: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT data FROM communes WHERE code_insee=?", (code_insee,)).fetchone()
        return json.loads(row[0]) if row else None

    def delete_commune(self, code_insee: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM communes WHERE code_insee=?", (code_insee,))
            conn.execute("DELETE FROM iris WHERE code_commune=?", (code_insee,))

    def get_dvf_prices(self) -> dict[str, float]:
        """Retourne {code_insee: prix_m2_median} pour toutes les communes."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT code_insee, json_extract(data,'$.prix_m2') FROM communes"
            ).fetchall()
        return {r[0]: r[1] for r in rows if r[1]}

    # ── IRIS ─────────────────────────────────────────────────────────────────

    def save_iris(self, code_commune: str, iris_list: list[dict]) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM iris WHERE code_commune=?", (code_commune,))
            conn.executemany(
                "INSERT INTO iris (code_iris,code_commune,data,updated_at) VALUES (?,?,?,?)",
                [(i["code_iris"], code_commune, json.dumps(i), _now())
                 for i in iris_list if i.get("code_iris")],
            )

    def get_iris_by_commune(self, code_commune: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT data FROM iris WHERE code_commune=? "
                "ORDER BY json_extract(data,'$.revenu_median') DESC", (code_commune,)
            ).fetchall()
        return [json.loads(r[0]) for r in rows]

    # ── Annonces ─────────────────────────────────────────────────────────────

    def save_annonce(self, ann: dict) -> bool:
        """Insère ou met à jour une annonce. Retourne True si nouvelle."""
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT id FROM annonces WHERE id=?", (ann["id"],)
            ).fetchone()
            conn.execute("""
                INSERT OR REPLACE INTO annonces
                (id,source,code_commune,nom_commune,titre,prix,surface,prix_m2,
                 delta_dvf_pct,sous_cote,prix_dvf_ref,rendement_estime,
                 type_bien,nb_pieces,url,date_scraping,actif)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
            """, (
                ann["id"], ann["source"], ann["code_commune"],
                ann.get("nom_commune"), ann.get("titre"),
                ann.get("prix"), ann.get("surface"), ann.get("prix_m2"),
                ann.get("delta_dvf_pct"), 1 if ann.get("sous_cote") else 0,
                ann.get("prix_dvf_ref"), ann.get("rendement_estime"),
                ann.get("type_bien"), ann.get("nb_pieces"),
                ann.get("url"), ann.get("date_scraping"),
            ))
        return existing is None

    def get_annonces(
        self,
        code_commune: str = None,
        source: str = None,
        type_bien: str = None,
        surface_min: float = None,
        surface_max: float = None,
        prix_max: int = None,
        sous_cote_only: bool = False,
        limit: int = 200,
    ) -> list[dict]:
        where = ["actif=1"]
        params = []
        if code_commune:
            where.append("code_commune=?"); params.append(code_commune)
        if source:
            where.append("source=?"); params.append(source)
        if type_bien:
            where.append("type_bien=?"); params.append(type_bien)
        if surface_min:
            where.append("surface>=?"); params.append(surface_min)
        if surface_max:
            where.append("surface<=?"); params.append(surface_max)
        if prix_max:
            where.append("prix<=?"); params.append(prix_max)
        if sous_cote_only:
            where.append("sous_cote=1")

        sql = f"""
            SELECT id,source,code_commune,nom_commune,titre,prix,surface,prix_m2,
                   delta_dvf_pct,sous_cote,prix_dvf_ref,rendement_estime,
                   type_bien,nb_pieces,url,date_scraping
            FROM annonces
            WHERE {' AND '.join(where)}
            ORDER BY sous_cote DESC, delta_dvf_pct ASC
            LIMIT ?
        """
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()

        cols = ["id","source","code_commune","nom_commune","titre","prix","surface",
                "prix_m2","delta_dvf_pct","sous_cote","prix_dvf_ref","rendement_estime",
                "type_bien","nb_pieces","url","date_scraping"]
        return [dict(zip(cols, r)) for r in rows]

    def get_annonces_stats(self) -> dict:
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM annonces WHERE actif=1").fetchone()[0]
            sous_cote = conn.execute("SELECT COUNT(*) FROM annonces WHERE actif=1 AND sous_cote=1").fetchone()[0]
            last = conn.execute("SELECT MAX(date_scraping) FROM annonces").fetchone()[0]
            by_source = conn.execute(
                "SELECT source, COUNT(*) FROM annonces WHERE actif=1 GROUP BY source"
            ).fetchall()
        return {
            "total": total,
            "sous_cote": sous_cote,
            "dernier_scraping": last,
            "par_source": {r[0]: r[1] for r in by_source},
        }

    def deactivate_old_annonces(self, hours: int = 36) -> None:
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat(timespec="seconds")
        with self._conn() as conn:
            conn.execute(
                "UPDATE annonces SET actif=0 WHERE date_scraping < ?", (cutoff,)
            )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")
