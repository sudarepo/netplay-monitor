"""Majestic Million (global backlink rank) dataset enrichment adapter.

Dataset adapter (NOT per-domain) over the free Majestic Million CSV. Adapter #7 in
the v1 stack. Yields the AUTHORITY dimension via Majestic's GlobalRank -- the third
triangulated authority signal alongside SEOkicks domain pop (#6) and Wikipedia links,
where any one strong signal lifts the domain (BUILD_BLUEPRINT.md:157). Subclasses
DatasetAdapter (download -> local lookup -> discard), NOT EnrichmentAdapter: the whole
~1M-row corpus is downloaded once per run and queried locally, which is far cheaper than
a per-domain API and needs no key (the CSV is free, CC-BY licensed, refreshed daily).

Source (verified 2026-06-18; resolves the BUILD_BLUEPRINT.md:287 open URL item):
  https://downloads.majestic.com/majestic_million.csv
  Free, no auth, Creative Commons Attribution. ~1,000,000 data rows + header. Columns:
    GlobalRank, TldRank, Domain, TLD, RefSubNets, RefIPs, IDN_Domain, IDN_TLD,
    PrevGlobalRank, PrevTldRank, PrevRefSubNets, PrevRefIPs
  GlobalRank is 1..~1,000,000 where 1 = strongest backlink profile (lower is better).

Fields extracted (thin -- Authority magnitude only; Prev* and IDN_* skipped in v1):
    global_rank  <- GlobalRank   (1 = strongest; lower is better)
    tld_rank     <- TldRank       (rank within the domain's TLD)
    ref_subnets  <- RefSubNets    (distinct referring subnets -- Majestic's core metric)
    ref_ips      <- RefIPs        (distinct referring IPs)
  ref_subnets/ref_ips are kept because they corroborate SEOkicks' netpop/ippop -- the
  scorer can cross-check two independent sources of the same authority shape.

KEY MATCHING -- exact match on the normalized domain (v1), a DOCUMENTED limitation:
  The Majestic CSV is keyed by HOSTNAME, and subdomains appear as their OWN rows
  (en.wikipedia.org, maps.google.com, ...), distinct from the apex. Meanwhile the
  pipeline's db._normalize() strips scheme/www/path and hands adapters a registered
  domain like "wikipedia.org". v1 does an EXACT lookup of that normalized domain against
  the CSV Domain column (both lowercased).

  Consequence (accepted for v1): a domain whose APEX is individually ranked is found; a
  domain that appears in the CSV ONLY at subdomain granularity (apex not separately
  ranked) reads as a MISS -> present=False -> "no global authority rank". For the DTI
  portfolio -- apex commercial/adult domains the owner controls -- the apex is the right
  question and apex rows do exist, so exact match is correct for the common case. The
  www.* rows in the CSV are the minority.

  Phase 2.5 follow-up (deferred, NOT built before coverage validation): collapse the CSV
  to registered-domain granularity keeping the BEST (lowest) GlobalRank per registered
  domain (the public Majestic-Million mirrors do exactly this), via PSL-aware extraction
  (tldextract). That pulls in a dependency and PSL handling (co.uk etc.) -- deliberately
  deferred until a validation run measures how many DTI domains are missed by exact match
  vs. recovered by best-rank collapse. Don't build the heavier matcher before confirming
  it's needed (same posture as estibot's deferred bracket map / domscan's PSL note).

None != 0 / None != "absent" discipline:
  A MISS (domain not in the corpus) is NOT an error and NOT a zero -- the base records it
  as a successful ok=True row with {"present": False}. For Majestic that is real,
  measured information: "this domain is not among the top ~1M by backlinks" = low global
  authority, distinct from "we failed to measure it". A HIT returns the field dict (base
  stamps present=True). Within a hit, an unparseable/blank numeric field -> None (never
  0): rank 0 is not a real Majestic rank, and a fabricated 0 would read as "rank #0 =
  infinitely strong," the exact inverse of the truth. _rank() and _count() enforce this.

Refresh / index model (why the parse happens in refresh, not lookup):
  refresh() downloads the CSV once and parses it into an in-memory dict
  {lowercased domain -> {fields}} held on self._index. lookup() is a pure dict.get on
  that index -- no network, no re-parse -- so the ~1M-row scan happens ONCE per run, not
  once per domain. _is_fresh() skips re-download if a same-day copy is already on disk
  (the CSV updates daily), so repeated runs within a day reuse the file. DISCARD_AFTER_RUN
  stays at the base default (True): the file is deleted after the run, the in-memory index
  is dropped with the instance.

  Memory: a ~1M-row dict of small field dicts is tens of MB -- fine on the target machine.
  If this ever needs to shrink, the Phase 2.5 option is a sqlite index instead of a Python
  dict, but v1 keeps it simple.

Malformed-row tolerance: a corpus row with too few columns or a non-integer GlobalRank is
  SKIPPED (not fatal) -- one bad line in a 1M-row third-party CSV must not abort the index
  build. A row missing the Domain key contributes nothing. The header row is detected and
  skipped. We never let a single dirty row turn into a failed refresh (which would mark the
  whole batch unavailable).
"""
import csv
import io

from app.enrichment.dataset import DatasetAdapter, DatasetUnavailable

# Max age for an on-disk copy before re-download. The CSV refreshes daily, so a
# same-day file is reused; older than this triggers a fresh download.
_MAX_AGE_SECONDS = 12 * 60 * 60        # 12h: comfortably "today", re-pulls next run/day

# Canonical CSV column names we read. If Majestic renames a column the lookups below
# degrade to None for that field (never crash) -- the index still builds on Domain.
_COL_DOMAIN = "Domain"
_COL_GLOBAL_RANK = "GlobalRank"
_COL_TLD_RANK = "TldRank"
_COL_REF_SUBNETS = "RefSubNets"
_COL_REF_IPS = "RefIPs"


class MajesticMillionAdapter(DatasetAdapter):
    name = "majestic_million"
    SOURCE_URL = "https://downloads.majestic.com/majestic_million.csv"
    CSV_FILENAME = "majestic_million.csv"

    def __init__(self, scratch_dir=None):
        super().__init__(scratch_dir)
        # {lowercased domain -> {global_rank, tld_rank, ref_subnets, ref_ips}}.
        # Built by refresh(); read by lookup(). None until the first refresh.
        self._index = None

    async def refresh(self):
        """Download the CSV (skipping a fresh same-day copy) and build the in-memory
        index. Raises DatasetUnavailable via self._download on any reachability/HTTP
        problem -- the base catches it and degrades the whole batch to clean ok=False.
        A parse error here propagates to the base's generic 'refresh failed' path."""
        dest = self._scratch_path(self.CSV_FILENAME)
        if not self._is_fresh(dest, _MAX_AGE_SECONDS):
            await self._download(self.SOURCE_URL, dest)
        self._index = self._build_index(dest)

    def lookup(self, domain):
        """Pure local dict lookup -- no network. Returns the field dict on a hit, or
        None for a miss (base stamps present=False). Defensive: if lookup somehow runs
        before refresh built an index, treat as a miss rather than crash."""
        if not self._index:
            return None
        return self._index.get(self._key(domain))

    # --- index construction ---------------------------------------------

    def _build_index(self, path):
        """Parse the CSV at `path` into {lowercased domain -> field dict}. Tolerant of
        malformed rows (skipped, not fatal). Uses csv.DictReader so column ORDER changes
        don't break us -- we key on header names. Reads with utf-8 + errors='replace' so
        a stray non-UTF8 byte in a 1M-row third-party file can't abort the build."""
        index = {}
        # newline="" per the csv module contract; errors='replace' for robustness.
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                dom = (row.get(_COL_DOMAIN) or "").strip().lower()
                if not dom:
                    continue                       # row without a Domain -> skip
                gr = _rank(row.get(_COL_GLOBAL_RANK))
                if gr is None:
                    # No usable GlobalRank -> this row carries no authority signal worth
                    # indexing (also filters a stray re-header line). Skip it.
                    continue
                index[dom] = {
                    "global_rank": gr,
                    "tld_rank": _rank(row.get(_COL_TLD_RANK)),
                    "ref_subnets": _count(row.get(_COL_REF_SUBNETS)),
                    "ref_ips": _count(row.get(_COL_REF_IPS)),
                }
        if not index:
            # A parsed-but-empty index means the file was not the CSV we expected
            # (e.g. an HTML error page served with 200). Treat as unavailable so the
            # batch degrades cleanly rather than marking every domain present=False.
            raise DatasetUnavailable(
                f"{self.name}: parsed 0 usable rows from {path} (unexpected file format)")
        return index

    @staticmethod
    def _key(domain):
        """Normalize a domain to the index key: lowercased, stripped. The pipeline's
        db._normalize already removed scheme/www/path upstream; this is the final
        defensive lowercase/trim so matching is case-insensitive."""
        return (domain or "").strip().lower()

    # Test/inspection seam: build an index from an in-memory CSV string without a
    # download, so tests exercise the real parser (mirrors how the per-domain adapter
    # tests feed canned payloads). Not used in production.
    def _build_index_from_text(self, text):
        index = {}
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            dom = (row.get(_COL_DOMAIN) or "").strip().lower()
            if not dom:
                continue
            gr = _rank(row.get(_COL_GLOBAL_RANK))
            if gr is None:
                continue
            index[dom] = {
                "global_rank": gr,
                "tld_rank": _rank(row.get(_COL_TLD_RANK)),
                "ref_subnets": _count(row.get(_COL_REF_SUBNETS)),
                "ref_ips": _count(row.get(_COL_REF_IPS)),
            }
        return index


def _rank(v):
    """Majestic rank field -> positive int, or None. A rank is >= 1; 0/negative/blank/
    non-numeric -> None (NEVER 0). A fabricated rank 0 would read as 'stronger than #1',
    inverting the signal, so it must be None, not 0."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        i = int(s)
    except ValueError:
        try:
            i = int(float(s))
        except (TypeError, ValueError):
            return None
    return i if i >= 1 else None


def _count(v):
    """Majestic count field (RefSubNets/RefIPs) -> non-negative int, or None. Blank/
    negative/non-numeric -> None. A genuine 0 (a ranked domain with 0 of this sub-metric)
    is preserved, distinct from missing -- same discipline as the per-domain adapters."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        i = int(s)
    except ValueError:
        try:
            i = int(float(s))
        except (TypeError, ValueError):
            return None
    return i if i >= 0 else None
