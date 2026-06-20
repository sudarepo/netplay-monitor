"""Domain portfolio scoring — turns enrichment rows into per-domain sub-scores, a
weighted composite, and an A–E disposition tier.

Reads the `enrichment` table (one row per domain+adapter, via schema.get_enrichment)
and produces a ScoreResult per domain. This is the FIRST-PASS model from
BUILD_BLUEPRINT.md: the weights and every normalization threshold are tunable constants
at the top of this file, because calibrating them against Greg's judgment on real
domains IS the work (the blueprint budgets several "adjust → re-run → review" rounds).
Nothing here is meant to be final on the first run; it is meant to be EASY TO TUNE.

DIMENSIONS (four; Defensive/brand deferred — its data source, DomScan TLD-count, turned
out not to exist; see the build notes. Its 0.05 weight folded into Authority):
    Value       0.35   Estibot appraisal (log-scaled)
    Authority   0.30   Ahrefs DR (anchor) + Majestic rank + OpenPageRank + Wikipedia
    Provenance  0.20   WhoXY first-registered year + Archive.org first-seen/crawl history
    Usage       0.15   DomScan health + reputation

ASYMMETRIC-SIGNAL RULE (from PROJECT.md, load-bearing): for high-specificity /
low-recall signals — strong evidence of quality when PRESENT, but a MISSING value does
NOT imply low quality — absence scores NEUTRAL, never a penalty. Concretely: a domain not
in Majestic's top-million, or absent from OpenPageRank's index, contributes nothing (it
neither lifts nor drags Authority); it does not get a 0 that would falsely tank the
score. Each dimension's normalizer implements this by returning None for "no signal," and
the dimension aggregates over only the signals actually present. A dimension with NO
present signals is itself None (UNKNOWN), and the composite renormalizes over the
dimensions that DO have data — a domain we know little about gets an honest low-confidence
score, not a fake zero.

COMPOSITE: weighted mean over the dimensions that have a score, weights renormalized to
the present subset (so a missing dimension doesn't silently drag the composite toward 0).

TIER (A–E): composite PLUS decision rules, because disposition is a decision, not just a
number (blueprint). E.g. an actively-resolving healthy domain with high Value or Authority
is A regardless of a middling composite; a dead domain with no value/authority/provenance
is E. The rules are checked first; the composite bands are the fallback.

COST-GAP OVERLAY: independent of tier — flags a domain whose annual renewal cost exceeds
a threshold fraction of its estimated value ("paying $X/yr to hold a $Y domain"). Renewal
cost is not yet in the enrichment data, so this is a HOOK: pass a renewal_cost and it
computes; omit it and the flag is None (not fabricated). Wired now so the deliverable's
savings number works the moment cost data lands.
"""
import math
from dataclasses import dataclass, field
from typing import Optional

# ============================================================================
# TUNABLE CONSTANTS — this whole block is the calibration surface. Editing these
# numbers re-shapes the scores; no logic below needs to change. Every value is a
# first-pass guess from the blueprint's hints, to be tuned against real domains.
# ============================================================================

# --- dimension weights (must describe intent; renormalized at runtime over present dims)
WEIGHTS = {
    "value": 0.35,
    "authority": 0.30,
    "provenance": 0.20,
    "usage": 0.15,
}

# --- Value: log-scaled USD. Blueprint: "$0–$50 → low, $5k+ → high".
# Below FLOOR -> 0; at/above CEIL -> 100; log-interpolated between. Log because value is
# perceived multiplicatively ($100->$1k feels like $1k->$10k), not linearly.
VALUE_FLOOR_USD = 100.0       # at/below this, Value sub-score = 0
VALUE_CEIL_USD = 10_000_000.0  # at/above this -> 100 AND the off-scale asterisk flag
# Option B value blend: geometric mean of Estibot and HumbleWorth-marketplace. HumbleWorth
# is the conservative anchor; when it reads low, that LOW value stands (no Estibot rescue).
# Missing one source -> use the other; missing both -> None.

# --- Authority: each source normalized to 0–100, then COMBINED by "best signal lifts it"
# (triangulated: a domain strong on ANY one source is high). We take a soft-max-ish blend:
# the max present signal dominates, with a small lift from secondary agreement.
# Ahrefs Domain Rating is already 0–100 — use directly.
AHREFS_DR_IS_0_100 = True
# OpenPageRank is 0–10 -> *10 to reach 0–100.
OPR_SCALE = 10.0
# Majestic GlobalRank: rank 1 = strongest, ~1,000,000 = weakest. Log-scaled inverse:
# rank<=BEST -> 100, rank>=WORST -> ~0. (Only ~top-1M domains appear at all; absence is
# neutral per the asymmetric rule, NOT rank=worst.)
MAJESTIC_RANK_BEST = 1_000        # rank <= this -> 100
MAJESTIC_RANK_WORST = 1_000_000   # rank >= this -> ~0
# Wikipedia external-link count: presence is a strong positive (notability). Saturating:
# a handful of links is already meaningful; cap the contribution.
WIKI_LINKS_FULL = 10              # this many links -> 100 (saturates)
# Authority blend: composite = max_signal + SECONDARY_LIFT * (mean_of_rest)
AUTHORITY_SECONDARY_LIFT = 0.15

# --- Provenance: registration age (years) + archive depth.
# Older domain + longer/denser Wayback history = higher.
AGE_YEARS_FULL = 20.0       # >= this many years since first registration -> age component 100
ARCHIVE_DAYS_FULL = 1000    # >= this many distinct capture-days -> archive component 100
# Provenance blends age (primary) and archive depth (secondary).
PROVENANCE_AGE_WEIGHT = 0.65
PROVENANCE_ARCHIVE_WEIGHT = 0.35

# --- Usage: DomScan health_score + reputation_score, both already 0–100.
USAGE_HEALTH_WEIGHT = 0.55
USAGE_REPUTATION_WEIGHT = 0.45

# --- Tier decision-rule thresholds (the "high"/"low" bars the A–E rules reference).
HIGH_VALUE = 70.0
HIGH_AUTHORITY = 70.0
HIGH_PROVENANCE = 60.0
HEALTHY_USAGE = 60.0        # "actively resolving & healthy"
LOW_ALL = 25.0             # "low" bar for the E (dead-weight) rule
# Composite fallback bands (used only when no decision rule fires).
TIER_BANDS = [("A", 80.0), ("B", 65.0), ("C", 45.0), ("D", 25.0)]  # else "E"

# --- Cost-gap overlay: flag if renewal_cost > fraction * estimated_value.
COST_GAP_FRACTION = 0.10    # paying >10% of the domain's value per year to hold it


# ============================================================================
# Result type
# ============================================================================

@dataclass
class ScoreResult:
    domain: str
    composite: Optional[float]            # 0–100, or None if no dimension had data
    tier: str                            # "A".."E"
    dimensions: dict                     # {name: score|None}
    present_dimensions: list             # which dims contributed
    cost_gap_flag: Optional[bool] = None  # True/False, or None if no renewal cost given
    reasons: list = field(default_factory=list)  # human-readable tier justification


# ============================================================================
# Small normalization helpers (each returns 0–100, or None for "no signal")
# ============================================================================

def _clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def _log_scale(value, floor, ceil):
    """Log-interpolate `value` between floor->0 and ceil->100. None/<=0 -> None (no
    signal). A real measured value at/below floor -> 0 (measured-low, NOT absent)."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return 0.0                       # a measured zero is measured-low, not absent
    if v <= floor:
        return 0.0
    if v >= ceil:
        return 100.0
    # log interpolation
    return _clamp(100.0 * (math.log(v) - math.log(floor)) /
                  (math.log(ceil) - math.log(floor)))


def _inverse_rank(rank, best, worst):
    """A rank where LOWER is stronger -> 0–100 where higher is better. Log-scaled.
    rank<=best -> 100; rank>=worst -> 0; None -> None (absent, neutral)."""
    if rank is None:
        return None
    try:
        r = float(rank)
    except (TypeError, ValueError):
        return None
    if r <= 0:
        return None
    if r <= best:
        return 100.0
    if r >= worst:
        return 0.0
    return _clamp(100.0 * (math.log(worst) - math.log(r)) /
                  (math.log(worst) - math.log(best)))


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


# ============================================================================
# Per-dimension scorers. Each takes the per-domain enrichment dict
# {adapter_name: row} (row has row["ok"] and row["data"]) and returns 0–100 or None.
# ============================================================================

def _data(enrichment, adapter):
    """Return the data dict for an adapter IF it has a successful row, else None.
    A failed/absent adapter contributes no signal (asymmetric: absence is neutral)."""
    row = enrichment.get(adapter)
    if not row or not row.get("ok"):
        return None
    return row.get("data") or {}


def blended_value_usd(enrichment):
    """Option B: geometric mean of Estibot estimated_value and HumbleWorth marketplace.
    HumbleWorth's low readings STAND (no Estibot rescue). One source missing -> use the
    other; both missing -> None. Returns a USD float or None."""
    est = hw = None
    ed = _data(enrichment, "estibot")
    if ed is not None:
        v = ed.get("estimated_value")
        if v is not None and v > 0:
            est = float(v)
    hd = _data(enrichment, "humbleworth")
    if hd is not None:
        v = hd.get("marketplace")
        if v is not None and v > 0:
            hw = float(v)
    import math
    if est is not None and hw is not None:
        return math.sqrt(est * hw)
    return est if est is not None else hw


def score_value(enrichment):
    """Value = log-scaled blended (Estibot x HumbleWorth-marketplace) appraisal."""
    return _log_scale(blended_value_usd(enrichment), VALUE_FLOOR_USD, VALUE_CEIL_USD)


def score_authority(enrichment):
    """Authority = triangulated best-signal-lifts over Ahrefs DR, Majestic rank,
    OpenPageRank, Wikipedia links. Each source -> 0–100 or None; combine so the single
    strongest present signal dominates, with a small lift from the others. ALL absent ->
    None (asymmetric: not in any index is neutral, not zero)."""
    signals = []

    ah = _data(enrichment, "ahrefs")
    if ah is not None:
        dr = ah.get("domain_rating")
        if dr is not None:
            signals.append(_clamp(float(dr)))   # already 0–100

    opr = _data(enrichment, "openpagerank")
    if opr is not None:
        pr = opr.get("page_rank")
        if pr is not None:
            signals.append(_clamp(float(pr) * OPR_SCALE))

    mm = _data(enrichment, "majestic_million")
    if mm is not None and mm.get("present"):     # present=False is a real "not ranked"
        s = _inverse_rank(mm.get("global_rank"), MAJESTIC_RANK_BEST, MAJESTIC_RANK_WORST)
        if s is not None:
            signals.append(s)

    wk = _data(enrichment, "wikipedia")
    if wk is not None and wk.get("linked"):
        lc = wk.get("link_count")
        if lc is not None:
            signals.append(_clamp(100.0 * min(lc, WIKI_LINKS_FULL) / WIKI_LINKS_FULL))

    if not signals:
        return None                              # no authority signal present -> UNKNOWN
    top = max(signals)
    rest = [s for s in signals if s is not top] or [s for s in signals if s != top]
    lift = AUTHORITY_SECONDARY_LIFT * (_mean(rest) or 0.0)
    return _clamp(top + lift)


def score_provenance(enrichment):
    """Provenance = registration age (primary) + archive depth (secondary). Absent both
    -> None."""
    age_score = None
    wx = _data(enrichment, "whoxy")
    if wx is not None:
        yr = wx.get("first_registered")          # a year-int or 'YYYY' / ISO date
        year = _first_year(yr)
        if year is not None:
            from datetime import datetime
            age = datetime.utcnow().year - year
            if age >= 0:
                age_score = _clamp(100.0 * min(age, AGE_YEARS_FULL) / AGE_YEARS_FULL)

    archive_score = None
    ao = _data(enrichment, "archiveorg")
    if ao is not None and ao.get("archived"):
        days = ao.get("capture_days")
        if days is not None:
            archive_score = _clamp(100.0 * min(days, ARCHIVE_DAYS_FULL) / ARCHIVE_DAYS_FULL)

    if age_score is None and archive_score is None:
        return None
    # Blend over whichever are present, renormalizing the two sub-weights.
    parts, weights = [], []
    if age_score is not None:
        parts.append(age_score); weights.append(PROVENANCE_AGE_WEIGHT)
    if archive_score is not None:
        parts.append(archive_score); weights.append(PROVENANCE_ARCHIVE_WEIGHT)
    wsum = sum(weights)
    return _clamp(sum(p * w for p, w in zip(parts, weights)) / wsum)


def score_usage(enrichment):
    """Usage = DomScan health + reputation (both 0–100). Absent domscan -> None. A dead
    domain measured with low scores is measured-low (real), distinct from absent."""
    d = _data(enrichment, "domscan")
    if d is None:
        return None
    health = d.get("health_score")
    rep = d.get("reputation_score")
    parts, weights = [], []
    if health is not None:
        parts.append(_clamp(float(health))); weights.append(USAGE_HEALTH_WEIGHT)
    if rep is not None:
        parts.append(_clamp(float(rep))); weights.append(USAGE_REPUTATION_WEIGHT)
    if not parts:
        return None
    wsum = sum(weights)
    return _clamp(sum(p * w for p, w in zip(parts, weights)) / wsum)


def _first_year(v):
    """Coerce a 'first registered' value (int year, 'YYYY', or 'YYYY-..' date) to a
    4-digit year int, or None."""
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 4 and s[:4].isdigit():
        y = int(s[:4])
        if 1980 <= y <= 2100:
            return y
    return None


# ============================================================================
# Composite + tier
# ============================================================================

def _composite(dimensions):
    """Weighted mean over dimensions that have a score, weights renormalized to the
    present subset. None if no dimension has data."""
    present = {k: v for k, v in dimensions.items() if v is not None}
    if not present:
        return None
    wsum = sum(WEIGHTS[k] for k in present)
    return _clamp(sum(v * WEIGHTS[k] for k, v in present.items()) / wsum)


def _assign_tier(composite, dims, enrichment):
    """A–E via decision rules first, composite bands as fallback. Returns (tier, reasons)."""
    reasons = []
    value = dims.get("value")
    authority = dims.get("authority")
    provenance = dims.get("provenance")
    usage = dims.get("usage")

    resolving_healthy = usage is not None and usage >= HEALTHY_USAGE
    high_value = value is not None and value >= HIGH_VALUE
    high_authority = authority is not None and authority >= HIGH_AUTHORITY

    # A — Mission-critical: actively resolving + healthy + high Value or Authority.
    if resolving_healthy and (high_value or high_authority):
        reasons.append("healthy + high value/authority -> mission-critical")
        return "A", reasons

    # B — Strategic/defensive: high Value or strong Authority, even if not actively used.
    if high_value or high_authority:
        reasons.append("high value or authority (hold for strategic/future value)")
        return "B", reasons

    # E — dead weight: low value + low authority + not resolving + no provenance.
    # CRITICAL asymmetry guard: E requires at least one MEASURED low signal, not merely
    # absence. A domain with no data at all is UNKNOWN (-> C review below), never E:
    # grading an un-enriched domain "sell/drop" would act on absence as if it were
    # evidence, the exact opposite of the asymmetric rule. So each clause is "measured
    # AND low," E fires only if the domain was scoreable, and nothing is measured-high.
    low_value = value is not None and value <= LOW_ALL
    low_authority = authority is not None and authority <= LOW_ALL
    measured_not_resolving = usage is not None and usage < HEALTHY_USAGE
    low_provenance = provenance is not None and provenance <= LOW_ALL
    any_measured_low = low_value or low_authority or measured_not_resolving or low_provenance
    nothing_measured_high = not (high_value or high_authority
                                 or (provenance is not None and provenance > LOW_ALL)
                                 or (usage is not None and usage >= HEALTHY_USAGE))
    if composite is not None and any_measured_low and nothing_measured_high:
        reasons.append("measured low value/authority/usage, no offsetting signal -> sell/drop")
        return "E", reasons

    # Otherwise fall back to composite bands (C/D land here, plus edge cases).
    if composite is None:
        reasons.append("no scoreable signals -> review")
        return "C", reasons
    for tier, cut in TIER_BANDS:
        if composite >= cut:
            reasons.append(f"composite {composite:.0f} >= {cut:.0f} -> {tier}")
            return tier, reasons
    reasons.append(f"composite {composite:.0f} below all bands -> E")
    return "E", reasons


def _cost_gap(value_usd, renewal_cost):
    """True if renewal_cost > COST_GAP_FRACTION * estimated_value. None if either input
    is missing (never fabricate the flag)."""
    if renewal_cost is None or value_usd is None:
        return None
    try:
        v = float(value_usd); c = float(renewal_cost)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return c > 0                     # paying anything to hold a $0 domain is a gap
    return c > COST_GAP_FRACTION * v


# ============================================================================
# Public entry points
# ============================================================================

def score_domain(domain, enrichment, *, renewal_cost=None):
    """Score one domain from its enrichment dict {adapter: row}. renewal_cost (USD/yr)
    is optional; when given, the cost-gap flag is computed. Returns a ScoreResult."""
    dims = {
        "value": score_value(enrichment),
        "authority": score_authority(enrichment),
        "provenance": score_provenance(enrichment),
        "usage": score_usage(enrichment),
    }
    composite = _composite(dims)
    tier, reasons = _assign_tier(composite, dims, enrichment)

    # cost-gap needs the raw estimated value (USD), not the 0–100 Value sub-score.
    est_value = None
    ed = _data(enrichment, "estibot")
    if ed is not None:
        est_value = ed.get("estimated_value")
    cost_gap = _cost_gap(est_value, renewal_cost)

    present = [k for k, v in dims.items() if v is not None]
    return ScoreResult(
        domain=domain, composite=composite, tier=tier, dimensions=dims,
        present_dimensions=present, cost_gap_flag=cost_gap, reasons=reasons,
    )


def score_all(get_enrichment_fn, domains, *, renewal_costs=None):
    """Score many domains. `get_enrichment_fn` is schema.get_enrichment (domain ->
    {adapter: row}). renewal_costs is an optional {domain: usd} map. Returns a list of
    ScoreResult ordered as given."""
    renewal_costs = renewal_costs or {}
    out = []
    for d in domains:
        enr = get_enrichment_fn(d) or {}
        out.append(score_domain(d, enr, renewal_cost=renewal_costs.get(d)))
    return out
