"""Live adapter validation runner (DEV TOOL — not part of the app, not imported by it).

Purpose: turn "the adapter's code passes unit tests" into "the adapter writes correct rows
from the LIVE source." Every adapter docstring marks its field names "GUESSED until
validation" (except ahrefs, already confirmed live). This script runs each adapter against
its real API for a small domain sample and prints, per adapter:
    - how many domains came back ok=True vs ok=False (+ the error reasons)
    - for each FIELD, how many of the ok rows had it populated vs None
      -> a field that is None on EVERY ok row is the tell-tale of a wrong/guessed key name
    - one full sample row, so you can eyeball the actual values

It spends real credits/units on the paid adapters (domscan, estibot, ahrefs, openpagerank
is free, whoxy is cheap). On the default ~12 domains that is negligible. Use --free-only
to run just the zero-cost sources (archiveorg, wikipedia, majestic, openpagerank) and spend
nothing.

Usage:
    python validate_adapters.py                      # all keyed adapters, ~12 domains
    python validate_adapters.py -n 20                # 20 domains
    python validate_adapters.py --free-only          # zero-cost sources only
    python validate_adapters.py --only estibot,whoxy # just these
    python validate_adapters.py --csv /path/to.csv   # override the domain source

Reads keys from the environment (load your .env first, e.g. `export $(grep -v '^#' .env
| xargs)` or run under python-dotenv). Domains: one per line (the DTL test CSV shape),
header-less; lines are lowercased and stripped.
"""
import argparse
import asyncio
import os
import sys
from pathlib import Path

# --- adapter registry: name -> (import path, class, "free"|"paid") ----------
# openpagerank is keyed but free (no per-call cost); grouped with paid only in that it
# needs a key, so it runs in the default keyed set and also under --free-only.
ADAPTERS = [
    ("archiveorg",    "app.enrichment.archiveorg",    "ArchiveOrgAdapter",    "free"),
    ("wikipedia",     "app.enrichment.wikipedia",     "WikipediaAdapter",     "free"),
    ("majestic_million", "app.enrichment.majestic_million", "MajesticMillionAdapter", "free"),
    ("openpagerank",  "app.enrichment.openpagerank",  "OpenPageRankAdapter",  "free_keyed"),
    ("whoxy",         "app.enrichment.whoxy",         "WhoxyAdapter",         "paid"),
    ("domscan",       "app.enrichment.domscan",       "DomscanAdapter",       "paid"),
    ("estibot",       "app.enrichment.estibot",       "EstibotAdapter",       "paid"),
    ("ahrefs",        "app.enrichment.ahrefs",        "AhrefsAdapter",        "paid"),
]

DEFAULT_CSV = (Path.home() / "Desktop" / "GEC Media" / "Domain Options" /
               "Documents" / "DTL_Test_800_Domains.csv")


def _load_domains(csv_path, n):
    p = Path(csv_path)
    if not p.exists():
        sys.exit(f"domain file not found: {p}")
    out = []
    for line in p.read_text(encoding="utf-8-sig", errors="replace").splitlines():
        d = line.strip().lstrip("\ufeff").strip().lower()
        if d and "." in d:
            out.append(d)
        if len(out) >= n:
            break
    if not out:
        sys.exit(f"no usable domains in {p}")
    return out


def _import_adapter(module_path, class_name):
    import importlib
    mod = importlib.import_module(module_path)
    return getattr(mod, class_name)


def _key_present(cls):
    """True if the adapter needs no key, or its key env var is set."""
    env = getattr(cls, "API_KEY_ENV", "")
    if not env:
        return True
    return bool(os.environ.get(env))


def _summarize(name, results):
    """Print the per-adapter validation summary."""
    total = len(results)
    ok_rows = [r for r in results if r.get("ok")]
    bad_rows = [r for r in results if not r.get("ok")]
    print(f"\n=== {name} ===")
    print(f"  {len(ok_rows)}/{total} ok, {len(bad_rows)} failed")

    if bad_rows:
        # Tally distinct error reasons (first 60 chars) so patterns are visible.
        reasons = {}
        for r in bad_rows:
            key = (r.get("error") or "")[:60]
            reasons[key] = reasons.get(key, 0) + 1
        for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
            print(f"    fail x{count}: {reason}")

    if ok_rows:
        # Per-field population across ok rows. A field None on EVERY ok row = likely a
        # wrong/guessed field name -> the thing to fix.
        field_names = set()
        for r in ok_rows:
            field_names.update((r.get("data") or {}).keys())
        print(f"  field population (across {len(ok_rows)} ok rows):")
        for f in sorted(field_names):
            non_null = sum(1 for r in ok_rows if (r.get("data") or {}).get(f) is not None)
            flag = "  <-- ALWAYS NULL (check field name?)" if non_null == 0 else ""
            print(f"    {f:24} {non_null}/{len(ok_rows)} populated{flag}")
        # One full sample row for eyeballing real values.
        sample = ok_rows[0]
        print(f"  sample ok row [{sample.get('domain')}]: {sample.get('data')}")


async def _run_adapter(name, cls, domains):
    adapter = cls()
    return await adapter.enrich_many(domains)


async def main_async(args):
    domains = _load_domains(args.csv, args.n)
    print(f"Validating against {len(domains)} domains: {', '.join(domains[:5])}"
          f"{' ...' if len(domains) > 5 else ''}")

    only = set(s.strip() for s in args.only.split(",")) if args.only else None

    for name, module_path, class_name, tier in ADAPTERS:
        if only and name not in only:
            continue
        if args.free_only and tier == "paid":
            print(f"\n=== {name} === (skipped: --free-only)")
            continue
        try:
            cls = _import_adapter(module_path, class_name)
        except Exception as e:
            print(f"\n=== {name} === (import failed: {type(e).__name__}: {e})")
            continue
        if not _key_present(cls):
            env = getattr(cls, "API_KEY_ENV", "")
            print(f"\n=== {name} === (skipped: no key, set {env})")
            continue
        try:
            results = await _run_adapter(name, cls, domains)
            _summarize(name, results)
        except Exception as e:
            print(f"\n=== {name} === (run crashed: {type(e).__name__}: {e})")


def main():
    ap = argparse.ArgumentParser(description="Live-validate enrichment adapters.")
    ap.add_argument("-n", type=int, default=12, help="number of domains (default 12)")
    ap.add_argument("--csv", default=str(DEFAULT_CSV), help="domain list file")
    ap.add_argument("--free-only", action="store_true", help="zero-cost adapters only")
    ap.add_argument("--only", default="", help="comma-separated adapter names to run")
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
