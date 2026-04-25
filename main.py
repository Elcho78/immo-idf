#!/usr/bin/env python3
"""
IMMO·IDF — Point d'entrée principal

Commandes disponibles :
  python main.py pipeline              Analyse toutes les communes actives
  python main.py pipeline --force      Force le re-téléchargement des données
  python main.py pipeline --commune 93001   Une seule commune

  python main.py api                   Lance l'API FastAPI (port 8000)
  python main.py api --port 9000

  python main.py add-commune --nom "Vincennes"   Recherche + ajoute une commune
  python main.py list-communes                   Affiche la config actuelle
  python main.py show-scores                     Affiche les scores calculés
"""
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("immo_idf")


def cmd_pipeline(args) -> None:
    from src.config import Config
    from src.pipeline import Pipeline

    config = Config.load(args.config)
    pipeline = Pipeline(config)
    results = pipeline.run(
        force_refresh=args.force,
        code_insee_filtre=args.commune,
    )

    print(f"\n{'─'*55}")
    print(f"{'Commune':<22} {'Score':>6}  {'Rendement':>10}  {'Prix m²':>10}")
    print(f"{'─'*55}")
    for r in sorted(results, key=lambda x: x.score_final or 0, reverse=True):
        rdt = f"{r.rendement_brut:.2f}%" if r.rendement_brut else "    N/A"
        prix = f"{r.prix_m2:,.0f} €" if r.prix_m2 else "    N/A"
        print(f"{r.nom:<22} {(r.score_final or 0):>5.1f}/10  {rdt:>10}  {prix:>10}")
    print(f"{'─'*55}")
    print(f"✓ {len(results)} communes traitées\n")


def cmd_api(args) -> None:
    import uvicorn
    uvicorn.run("src.api:app", host=args.host, port=args.port, reload=False)


def cmd_add_commune(args) -> None:
    import httpx
    from src.config import Config

    query = args.nom
    print(f"Recherche de communes correspondant à « {query} »…")
    resp = httpx.get(
        "https://geo.api.gouv.fr/communes",
        params={"nom": query, "fields": "nom,code,departement", "boost": "population", "limit": 8},
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json()

    if not results:
        print("Aucune commune trouvée.")
        return

    print("\nRésultats :")
    for i, c in enumerate(results):
        dept = c.get("departement", {})
        print(f"  [{i+1}] {c['nom']:<25} code={c['code']}  dept={dept.get('nom','')}")

    choice = input("\nNuméro à ajouter (ou Entrée pour annuler) : ").strip()
    if not choice.isdigit() or not (1 <= int(choice) <= len(results)):
        print("Annulé.")
        return

    selected = results[int(choice) - 1]
    cfg = Config.load(args.config)
    try:
        cfg.add_commune(selected["code"], selected["nom"])
        print(f"\n✓ {selected['nom']} ({selected['code']}) ajoutée à {args.config}")
        print("  Relancez `python main.py pipeline` pour collecter les données.")
    except ValueError as e:
        print(f"⚠ {e}")


def cmd_list_communes(args) -> None:
    from src.config import Config
    cfg = Config.load(args.config)
    print(f"\n{'Code INSEE':<12} {'Commune':<28} {'Actif'}")
    print("─" * 48)
    for c in cfg.communes:
        status = "✓" if c.actif else "○"
        print(f"{c.code_insee:<12} {c.nom:<28} {status}")
    print(f"\n{len(cfg.communes_actives)} active(s) sur {len(cfg.communes)} configurée(s)")


def cmd_show_scores(args) -> None:
    from src.storage import Storage
    st = Storage()
    communes = st.get_all_communes()
    if not communes:
        print("Aucune donnée. Lancez d'abord `python main.py pipeline`.")
        return
    print(f"\n{'Commune':<22} {'Score':>6}  {'Rdt brut':>9}  {'Prix m²':>10}  {'Rev. méd.':>10}")
    print("─" * 65)
    for c in communes:
        rdt = f"{c['rendement_brut']:.2f}%" if c.get("rendement_brut") else "  N/A"
        prix = f"{c['prix_m2']:,.0f} €" if c.get("prix_m2") else "  N/A"
        rev = f"{c['revenu_median']:,.0f} €" if c.get("revenu_median") else "  N/A"
        sc = c.get("score_final") or 0
        print(f"{c['nom']:<22} {sc:>5.1f}/10  {rdt:>9}  {prix:>10}  {rev:>10}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(prog="immo_idf", description="IMMO·IDF Pipeline & API")
    parser.add_argument("--config", default="config.yml", help="Chemin du fichier de configuration")
    sub = parser.add_subparsers(dest="cmd")

    # pipeline
    p_pipe = sub.add_parser("pipeline", help="Lancer le pipeline de données")
    p_pipe.add_argument("--force", action="store_true", help="Ignorer le cache")
    p_pipe.add_argument("--commune", metavar="CODE_INSEE", help="Une seule commune")

    # api
    p_api = sub.add_parser("api", help="Lancer l'API FastAPI")
    p_api.add_argument("--host", default="0.0.0.0")
    p_api.add_argument("--port", type=int, default=8000)

    # add-commune
    p_add = sub.add_parser("add-commune", help="Rechercher et ajouter une commune")
    p_add.add_argument("--nom", required=True, help="Nom de la commune à chercher")

    # list
    sub.add_parser("list-communes", help="Afficher les communes configurées")

    # scores
    sub.add_parser("show-scores", help="Afficher les scores calculés")

    args = parser.parse_args()

    dispatch = {
        "pipeline": cmd_pipeline,
        "api": cmd_api,
        "add-commune": cmd_add_commune,
        "list-communes": cmd_list_communes,
        "show-scores": cmd_show_scores,
    }

    if args.cmd in dispatch:
        dispatch[args.cmd](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
