"""Archive.org (Wayback) enrichment adapter.

Per-domain adapter over the Wayback CDX Server API. Yields Provenance and Usage
signals for a domain from its web-archive capture history. No API key required.

Endpoint:
    https://web.archive.org/cdx/search/cdx
    ?url=<domain>&matchType=domain&fl=timestamp,statuscode
    &collapse=timestamp:8&output=json&limit=10000

Signals extracted (and the scoring dimension each serves):
    first_capture      ISO date of the oldest capture        -> Provenance
    last_capture       ISO date of the newest capture        -> Usage (+span)
    capture_days       count of daily-collapsed capture rows -> Usage
    live_capture_days  capture rows with HTTP status 200      -> Usage
    archived           bool: any captures at all              -> discriminator

Why capture_days, not a literal "crawl count":
    PROJECT.md lists "crawl count," but raw capture count is dominated by bot
    crawl volume — a domain hit 50k times in one week is not more legitimately
    used than one captured steadily across 3,000 days. `collapse=timestamp:8`
    collapses to one row per UTC day, turning the figure into an activity-
    consistency signal that maps cleanly to Usage. (Caveat: with matchType=domain
    the server sorts by urlkey then timestamp, so a "row" is a (captured-URL, day)
    pair, not a strictly globally-distinct day — a reasonable activity proxy, and
    the right granularity for scoring. Validated live; see the 20-domain plan.)

CRITICAL for the scoring layer — two different "negatives":
    * ok=False                  -> the lookup FAILED. Knowledge is absent. Scoring
                                   must treat the domain as UNKNOWN here, never as
                                   "not archived." (Wayback was down / throttled /
                                   returned junk.)
    * ok=True, archived=False   -> the lookup SUCCEEDED and authoritatively found
                                   zero captures. A real Provenance signal (likely
                                   parked / never-deployed). Safe to score.
    Always check `ok` before reading `data["archived"]`.

Concurrency / rate limits:
    Wayback throttles under load with no documented limit, so MAX_CONCURRENCY is a
    conservative 4 and requests go through EnrichmentAdapter._get_with_retries
    (exponential backoff + jitter on 429/503 and transient transport errors).
    Exhausted retries map to a typed ok=False ("rate_limited ..."); a transport
    error on the final attempt propagates to _guarded for a clean ok=False row.
"""
import httpx

from app.enrichment.base import EnrichmentAdapter


class ArchiveOrgAdapter(EnrichmentAdapter):
    name = "archiveorg"
    BASE_URL = "https://web.archive.org/cdx/search/cdx"
    API_KEY_ENV = ""                       # keyless; no warning (guarded in base)
    MAX_CONCURRENCY = 4                     # Wayback throttles; stay a good citizen
    TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

    # Safety cap on returned rows. Hitting it sets capture_count_capped so scoring
    # knows capture_days is a floor and last_capture may be stale. Rare for the DTI
    # portfolio (parked/brandable names mostly have few or zero captures).
    LIMIT = 10000

    async def enrich_one(self, client, domain):
        # Punycode only at the wire boundary; the stored row keeps `domain`
        # (the normalized form) as its DB key.
        ascii_domain = self._punycode(domain)
        params = {
            "url": ascii_domain,
            "matchType": "domain",
            "fl": "timestamp,statuscode",
            "collapse": "timestamp:8",
            "output": "json",
            "limit": self.LIMIT,
        }
        # Transport errors from _get_with_retries (a timeout / connection reset that
        # persists past the last retry) propagate up to _guarded in enrich_many,
        # which catches them and produces a clean ok=False row. We deliberately do
        # NOT try/except here — adding one would be redundant with that backstop.
        resp = await self._get_with_retries(client, self.BASE_URL, params=params)

        # _get_with_retries returns the final response even if still throttled, so
        # the status-to-ok=False mapping lives here:
        #   200            -> parse and return data
        #   429/503        -> retries exhausted, typed rate_limited ok=False
        #   other >= 400   -> typed HTTP-status ok=False
        # All non-200 outcomes are UNKNOWN, never archived=False.
        if resp.status_code in (429, 503):
            return self._error(
                domain, f"archiveorg: rate_limited after retries (HTTP {resp.status_code})"
            )
        if resp.status_code >= 400:
            return self._error(domain, f"archiveorg: HTTP {resp.status_code}")

        # CDX returns 200 + EMPTY body for "no matches" — that's archived=False,
        # a successful finding, NOT an error.
        text = resp.text.strip()
        if not text:
            return self._result(domain, self._empty_payload())

        rows = resp.json()                  # malformed -> _guarded -> clean ok=False
        # output=json prepends a header row; <2 rows means header-only or empty.
        if not rows or len(rows) < 2:
            return self._result(domain, self._empty_payload())

        return self._result(domain, self._summarize(rows[1:]))

    # --- pure helpers (no network; unit-testable) -----------------------

    def _summarize(self, data_rows):
        """Reduce CDX [timestamp, statuscode] rows to the stored payload.

        `data_rows` is the response AFTER the header row has been sliced off by
        enrich_one, so its length is the data-record count. The CDX `limit` we send
        caps the number of RECORDS; output=json prepends the field-name header on
        top of those records rather than consuming a record slot, so a fully-capped
        response carries exactly LIMIT data rows — hence `capture_days >= self.LIMIT`.
        When it fires, capture_days is a floor and last_capture may be stale.
        (CDX's exact limit/header semantics are undocumented; confirm against the
        live API in the 20-domain validation.)
        """
        days = []
        live = 0
        for row in data_rows:
            ts = row[0] if row else ""
            status = row[1] if len(row) > 1 else ""
            if len(ts) >= 8 and ts[:8].isdigit():
                days.append(ts[:8])
            if status == "200":
                live += 1
        capture_days = len(data_rows)
        capture_count_capped = capture_days >= self.LIMIT
        return {
            "archived": True,
            "first_capture": self._fmt_date(min(days)) if days else None,
            "last_capture": self._fmt_date(max(days)) if days else None,
            "capture_days": capture_days,
            "live_capture_days": live,
            "capture_count_capped": capture_count_capped,
        }

    @staticmethod
    def _empty_payload():
        """The archived=False payload: a successful lookup that found nothing."""
        return {
            "archived": False,
            "first_capture": None,
            "last_capture": None,
            "capture_days": 0,
            "live_capture_days": 0,
            "capture_count_capped": False,
        }

    @staticmethod
    def _fmt_date(yyyymmdd):
        """'20040312' -> '2004-03-12'; None if too short to be a date."""
        if not yyyymmdd or len(yyyymmdd) < 8:
            return None
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
