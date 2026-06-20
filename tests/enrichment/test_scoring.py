"""Tests for the scoring module (app/enrichment/scoring.py).

No DB, no network: scoring is pure functions over an enrichment dict {adapter: row},
so synthetic rows exercise every path. Row shape mirrors what schema.get_enrichment
returns: {"ok": bool, "data": {...}} per adapter (the fields are the live-validated
ones from each adapter).

Coverage: each dimension normalizer (value log-scale, authority triangulation, provenance
age+archive, usage health+reputation); the asymmetric rule (absent signal -> None/neutral,
never a 0 penalty); composite renormalization over present dims; the A–E decision rules
(mission-critical, strategic, dead-weight, composite-band fallback); cost-gap hook
(present/absent/zero-value); and the score_all batch driver.
"""
from app.enrichment.scoring import (
    score_domain, score_all, score_value, score_authority, score_provenance,
    score_usage, _log_scale, _inverse_rank, _composite, _cost_gap,
    VALUE_FLOOR_USD, VALUE_CEIL_USD,
)


# --- row builders (mirror live adapter data shapes) ---------------------

def _ok(data):
    return {"ok": True, "data": data}


def _fail():
    return {"ok": False, "data": {}}


def _estibot(value):
    return _ok({"estimated_value": value, "wholesale_value": None, "price_range_retail": None})


def _ahrefs(dr):
    return _ok({"domain_rating": dr, "ahrefs_rank": 100, "live_backlinks": 1,
                "live_refdomains": 1, "all_time_refdomains": 1})


def _opr(page_rank):
    return _ok({"page_rank": page_rank, "page_rank_int": int(page_rank), "global_rank": 1000})


def _majestic(global_rank, present=True):
    if not present:
        return _ok({"present": False})
    return _ok({"global_rank": global_rank, "tld_rank": global_rank, "ref_subnets": 1,
                "ref_ips": 1, "present": True})


def _wikipedia(linked, link_count=0):
    return _ok({"linked": linked, "link_count": link_count, "link_count_capped": False})


def _whoxy(first_year):
    return _ok({"first_registered": str(first_year), "registered": True,
                "create_date": f"{first_year}-01-01"})


def _archiveorg(capture_days, archived=True):
    return _ok({"archived": archived, "first_capture": "2000-01-01",
                "last_capture": "2026-01-01", "capture_days": capture_days,
                "live_capture_days": capture_days, "capture_count_capped": False})


def _domscan(health, reputation):
    return _ok({"health_score": health, "health_grade": "B", "health_checks": [],
                "reputation_score": reputation, "reputation_grade": "B",
                "risk_level": "low", "reputation_factors": {},
                "registered": True, "lifecycle_phase": "active", "registry_status": []})


# --- normalizer unit checks --------------------------------------------

def test_log_scale_floor_ceil_and_absent():
    assert _log_scale(None, VALUE_FLOOR_USD, VALUE_CEIL_USD) is None     # absent -> None
    assert _log_scale(0, VALUE_FLOOR_USD, VALUE_CEIL_USD) == 0.0         # measured 0 -> 0
    assert _log_scale(100, VALUE_FLOOR_USD, VALUE_CEIL_USD) == 0.0       # at floor
    assert _log_scale(10_000_000, VALUE_FLOOR_USD, VALUE_CEIL_USD) == 100.0  # at ceil
    assert _log_scale(50_000_000, VALUE_FLOOR_USD, VALUE_CEIL_USD) == 100.0  # above ceil
    mid = _log_scale(50_000, VALUE_FLOOR_USD, VALUE_CEIL_USD)            # between
    assert 0.0 < mid < 100.0


def test_inverse_rank_absent_is_none():
    assert _inverse_rank(None, 1000, 1_000_000) is None                  # absent -> neutral
    assert _inverse_rank(1, 1000, 1_000_000) == 100.0                    # top rank
    assert _inverse_rank(1_000_000, 1000, 1_000_000) == 0.0              # worst rank
    assert 0.0 < _inverse_rank(50_000, 1000, 1_000_000) < 100.0


# --- Value dimension ----------------------------------------------------

def test_value_present_and_absent():
    assert score_value({"estibot": _estibot(10_000_000)}) == 100.0       # at ceiling
    assert score_value({"estibot": _estibot(50)}) == 0.0                 # below $100 floor
    assert score_value({}) is None                                       # absent -> None
    assert score_value({"estibot": _fail()}) is None                     # failed -> None
    blended = score_value({"estibot": _estibot(1_000_000),
                           "humbleworth": _ok({"marketplace": 4_000_000})})
    assert blended is not None and 70.0 < blended < 100.0


# --- Authority triangulation + asymmetric rule --------------------------

def test_authority_single_strong_signal_lifts():
    # Only Ahrefs present, DR 90 -> high authority from one signal (triangulation).
    s = score_authority({"ahrefs": _ahrefs(90)})
    assert s == 90.0


def test_authority_absent_all_is_none_not_zero():
    # No authority sources at all -> None (UNKNOWN), NOT 0 (the asymmetric rule).
    assert score_authority({}) is None
    # Majestic present=False (a real "not in top million") contributes no signal -> None.
    assert score_authority({"majestic_million": _majestic(0, present=False)}) is None


def test_authority_best_signal_dominates_with_small_lift():
    # Ahrefs DR 80 + OPR 5.0 (->50). Max (80) dominates, small lift from the 50.
    s = score_authority({"ahrefs": _ahrefs(80), "openpagerank": _opr(5.0)})
    assert 80.0 <= s <= 90.0            # top + small secondary lift, capped at 100


def test_authority_wikipedia_saturates():
    s = score_authority({"wikipedia": _wikipedia(True, link_count=10)})
    assert s == 100.0
    # not linked -> no signal
    assert score_authority({"wikipedia": _wikipedia(False)}) is None


# --- Provenance ---------------------------------------------------------

def test_provenance_age_and_archive():
    # 20+ year old domain with deep archive -> high.
    s = score_provenance({"whoxy": _whoxy(2000), "archiveorg": _archiveorg(1000)})
    assert s is not None and s >= 90.0


def test_provenance_absent_is_none():
    assert score_provenance({}) is None
    assert score_provenance({"whoxy": _fail()}) is None


def test_provenance_age_only_blends_over_present():
    # Only whoxy present -> provenance still scores (renormalized to the age sub-weight).
    s = score_provenance({"whoxy": _whoxy(2006)})   # 20 years -> ~100
    assert s is not None and s >= 90.0


# --- Usage --------------------------------------------------------------

def test_usage_health_and_reputation():
    s = score_usage({"domscan": _domscan(86, 97)})
    assert s is not None and 86.0 <= s <= 97.0
    assert score_usage({}) is None                  # absent -> None
    # measured-low (dead domain) is a real low score, not None
    assert score_usage({"domscan": _domscan(5, 5)}) == 5.0


# --- composite renormalization -----------------------------------------

def test_composite_renormalizes_over_present_dims():
    # Only value present -> composite == value (weight renormalized to 1.0), NOT dragged
    # toward 0 by the absent dimensions.
    assert _composite({"value": 80.0, "authority": None, "provenance": None,
                       "usage": None}) == 80.0
    # all None -> None
    assert _composite({"value": None, "authority": None, "provenance": None,
                       "usage": None}) is None


# --- tier decision rules ------------------------------------------------

def test_tier_A_mission_critical():
    # Healthy + high authority -> A regardless of other dims.
    enr = {"domscan": _domscan(90, 90), "ahrefs": _ahrefs(85)}
    r = score_domain("x.com", enr)
    assert r.tier == "A"
    assert "mission-critical" in " ".join(r.reasons)


def test_tier_B_strategic_high_value_not_used():
    # High value but not resolving (no domscan) -> B (hold for strategic value).
    enr = {"estibot": _estibot(5_000_000)}
    r = score_domain("x.com", enr)
    assert r.tier == "B"


def test_tier_E_dead_weight():
    # Low value, no authority, not resolving, no provenance -> E.
    enr = {"estibot": _estibot(10), "domscan": _domscan(10, 10)}
    r = score_domain("x.com", enr)
    assert r.tier == "E"
    assert "sell/drop" in " ".join(r.reasons)


def test_tier_C_for_no_signals():
    # Nothing scoreable -> C (human review), composite None.
    r = score_domain("x.com", {})
    assert r.tier == "C"
    assert r.composite is None
    assert r.present_dimensions == []


# --- cost-gap overlay ---------------------------------------------------

def test_cost_gap_present_absent_zero():
    # renewal 50 on a 1000-value domain: 50 > 10% of 1000 (100)? No -> False.
    assert _cost_gap(1000, 50) is False
    # renewal 200 on 1000: 200 > 100 -> True (paying too much to hold it).
    assert _cost_gap(1000, 200) is True
    # no renewal cost -> None (never fabricate).
    assert _cost_gap(1000, None) is None
    # zero-value domain with any renewal cost -> True (a gap by definition).
    assert _cost_gap(0, 10) is True


def test_score_domain_carries_cost_gap():
    enr = {"estibot": _estibot(1000)}
    r = score_domain("x.com", enr, renewal_cost=200)
    assert r.cost_gap_flag is True
    r2 = score_domain("x.com", enr)              # no renewal cost
    assert r2.cost_gap_flag is None


# --- batch driver -------------------------------------------------------

def test_score_all_batch():
    db = {
        "good.com": {"domscan": _domscan(90, 90), "ahrefs": _ahrefs(85)},
        "dead.com": {"estibot": _estibot(5), "domscan": _domscan(10, 10)},
    }
    results = score_all(lambda d: db.get(d, {}), ["good.com", "dead.com"],
                        renewal_costs={"dead.com": 12})
    assert [r.domain for r in results] == ["good.com", "dead.com"]
    assert results[0].tier == "A"
    assert results[1].tier == "E"
