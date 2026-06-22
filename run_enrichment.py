"""Orchestration runner: enrich -> SAVE -> score -> print grades."""
import argparse
import asyncio
import os
import sys
from pathlib import Path

from app.enrichment.schema import (
    init_enrichment_schema, save_enrichment_batch, get_enrichment)
from app.enrichment.scoring import score_domain

ADAPTERS = [
    ("archiveorg",       "app.enrichment.archiveorg",       "ArchiveOrgAdapter",      "free"),
    ("wikipedia",        "app.enrichment.wikipedia",        "WikipediaAdapter",       "free"),
    ("majestic_million", "app.enrichment.majestic_million", "MajesticMillionAdapter", "free"),
    ("openpagerank",     "app.enrichment.openpagerank",     "OpenPageRankAdapter",    "free_keyed"),
    ("whoxy",            "app.enrichment.whoxy",            "WhoxyAdapter",           "paid"),
    ("domscan",          "app.enrichment.domscan",          "DomscanAdapter",         "paid"),
    ("estibot",          "app.enrichment.estibot",          "EstibotAdapter",         "paid"),
    ("humbleworth",      "app.enrichment.humbleworth",      "HumbleworthAdapter",     "paid"),
    ("ahrefs",           "app.enrichment.ahrefs",           "AhrefsAdapter",          "paid"),
]

DEFAULT_DOMAINS = ["python.com", "glue.com", "bangyoulater.com",
                   "deepthroat.com", "naughty.com", "tempted.com"]


def _load_domains(csv_path, n):
    if not csv_path:
        return DEFAULT_DOMAINS[:n] if n else DEFAULT_DOMAINS
    p = Path(csv_path)
    if not p.exists():
        sys.exit(f"domain file not found: {p}")
    out = []
    for line in p.read_text(encoding="utf-8-sig", errors="replace").splitlines():
        d = line.strip().lstrip("\ufeff").strip().lower()
        if d and "." in d:
            out.append(d)
        if n and len(out) >= n:
            break
    return out or DEFAULT_DOMAINS


def _import(module_path, class_name):
    import importlib
    return getattr(importlib.import_module(module_path), class_name)


def _key_present(cls):
    env = getattr(cls, "API_KEY_ENV", "")
    return True if not env else bool(os.environ.get(env))


async def _enrich(domains, free_only):
    all_results = []
    for name, module_path, class_name, tier in ADAPTERS:
        if free_only and tier == "paid":
            print(f"  {name}: skipped (--free-only)")
            continue
        try:
            cls = _import(module_path, class_name)
        except Exception as e:
            print(f"  {name}: import failed ({type(e).__name__}: {e})")
            continue
        if not _key_present(cls):
            print(f"  {name}: skipped (no key {getattr(cls, 'API_KEY_ENV', '')})")
            continue
        try:
            results = await cls().enrich_many(domains)
            ok = sum(1 for r in results if r.get("ok"))
            print(f"  {name}: {ok}/{len(results)} ok")
            all_results.extend(results)
        except Exception as e:
            print(f"  {name}: run crashed ({type(e).__name__}: {e})")
    return all_results


def _fmt(x):
    return "  -  " if x is None else f"{x:5.1f}"


def _print_grades(domains):
    print("\n" + "=" * 78)
    print(f"{'domain':<20} {'tier':<5} {'comp':>5}  {'Val':>5} {'Auth':>5} {'Prov':>5} {'Use':>5}")
    print("-" * 78)
    for d in domains:
        enr = get_enrichment(d) or {}
        r = score_domain(d, enr)
        dims = r.dimensions
        print(f"{d:<20} {r.tier:<5} {_fmt(r.composite):>5}  "
              f"{_fmt(dims['value'])} {_fmt(dims['authority'])} "
              f"{_fmt(dims['provenance'])} {_fmt(dims['usage'])}")
        if r.reasons:
            print(f"{'':<20} -> {r.reasons[-1]}")
    print("=" * 78)
    print("tiers: A mission-critical | B strategic/hold | C review | D consolidate | E sell/drop")
    print("dims 0-100; '-' = no signal (neutral, not zero)")


async def main_async(args):
    domains = _load_domains(args.csv, args.n)
    print(f"Domains ({len(domains)}): {', '.join(domains)}")
    if not args.score_only:
        init_enrichment_schema()
        print("\nEnriching:")
        results = await _enrich(domains, args.free_only)
        if args.no_save:
            print(f"\n--no-save: {len(results)} results NOT persisted")
            return
        save_enrichment_batch(results)
        print(f"\nSaved {len(results)} rows to the enrichment table.")
    _print_grades(domains)


def main():
    ap = argparse.ArgumentParser(description="Enrich, save, and score domains.")
    ap.add_argument("--csv", default="", help="domain list file (default: built-in 6)")
    ap.add_argument("-n", type=int, default=0, help="cap number of domains")
    ap.add_argument("--free-only", action="store_true", help="zero-cost adapters only")
    ap.add_argument("--no-save", action="store_true", help="dry run, skip DB write")
    ap.add_argument("--score-only", action="store_true", help="score existing DB rows only")
    asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    main()
