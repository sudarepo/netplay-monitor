"""Base adapter for dataset enrichment sources.

Some sources publish a whole-corpus dataset that is far cheaper to download once
and query locally than to hit a per-domain API. This base implements the
download -> local lookup -> discard pattern described in PROJECT.md ("Two adapter
types").

Two confirmed consumers:
  - majestic_million  daily ~1M-row CSV  -> global-rank lookup
  - curlie            monthly ~200MB TSV -> boolean listed/not-listed lookup

(curlie.py is deferred while its download endpoint is unavailable — see the
Curlie note in PROJECT.md. majestic_million is the live test consumer for this
base.)

Dataset adapters produce the SAME result-dict shape as EnrichmentAdapter
(app/enrichment/base.py) and write to the SAME `enrichment` table via the same
save_enrichment helpers (app/enrichment/schema.py) — only the acquisition
differs. The result helpers below are intentionally a mirror of
EnrichmentAdapter's so the two adapter types stay decoupled (PROJECT.md frames
them as two distinct types); each module owning its own copy of small shared
shapes is the established repo convention (cf. USER_AGENT in checker.py / base.py).

Availability posture (mirrors the per-domain adapters' ok=False contract, at the
dataset level): a dataset's source may be unreachable at run time. refresh() must
not crash the run on a download failure. enrich_many() catches refresh failures,
records them on `self.refresh_error`, and returns a clean ok=False row for every
domain in the batch with a greppable error string — so the orchestration layer
(Phase 4) can inspect the outcome and DECIDE whether to halt the whole run or
skip just this source. Same idea as DomScan's 402 hard-stop signal, one level up.

Subclasses implement:
    name                       short identifier, used as the DB `adapter` key
    async refresh(self)        download/refresh the local dataset (idempotent)
    lookup(self, domain)       synchronous local lookup, no network; returns a
                               data dict, or None as shorthand for "not present"

enrich_many() is inherited: it calls refresh() once, then lookup() per domain.
Do not reimplement it.
"""
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import httpx

# A real-browser UA. Some hosts reject the default httpx UA. Each module owns its
# own copy by repo convention (see checker.py / base.py).
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Scratch root for downloaded corpora. Env-overridable, mirroring db.py's
# DOMAIN_MONITOR_DB pattern. The Mac has 2TB, so the download -> query -> discard
# cycle is fine; nothing here needs to persist between runs. Gitignored.
DEFAULT_SCRATCH_DIR = Path(
    os.environ.get(
        "DATASET_SCRATCH_DIR",
        # parent chain: dataset.py -> enrichment/ -> app/ -> repo root, then /data/datasets
        Path(__file__).resolve().parent.parent.parent / "data" / "datasets",
    )
)


class DatasetUnavailable(Exception):
    """Raised by refresh()/_download() when a dataset's source cannot be reached
    or returns an error. Signals an availability problem (not a code bug), so the
    orchestration layer can choose to halt or skip the source rather than crash."""


class DatasetAdapter:
    """Base class for a dataset-backed enrichment source.

    Subclass attributes to set:
        name              short identifier, used as the DB `adapter` key
        SOURCE_URL        the dataset download URL (informational; used by refresh)
        TIMEOUT           httpx.Timeout — generous read timeout for large downloads
        DISCARD_AFTER_RUN delete the downloaded corpus after enrich_many() finishes
                          (default True — strict download -> query -> discard). Set
                          env var DATASET_KEEP_CACHE ("1"/"true") to keep the local
                          corpus for iterative dev without changing this default.

    Subclass methods to implement:
        async refresh(self)        download/refresh the local dataset
        lookup(self, domain)       synchronous local lookup -> dict | None
    """

    # Empty by design: a subclass MUST override this. A non-empty placeholder would
    # let a forgotten override silently write every row under that name and only
    # surface at query time; "" fails fast in DB operations instead.
    name = ""
    SOURCE_URL = ""
    # Large downloads: a long read timeout, but a tight connect timeout so an
    # unreachable host fails fast rather than hanging the run.
    TIMEOUT = httpx.Timeout(connect=10.0, read=300.0, write=10.0, pool=10.0)
    # Strict discard by default (PROJECT.md "download -> query -> discard"): the
    # corpus is deleted after each run, so a corrupted local copy auto-recovers on
    # the next run. Developers iterating locally can keep the cache by setting the
    # DATASET_KEEP_CACHE env var (see _keep_cache) — runtime opt-out, no code
    # change, production default unchanged.
    DISCARD_AFTER_RUN = True

    def __init__(self, scratch_dir=None):
        self.scratch_dir = Path(scratch_dir) if scratch_dir else DEFAULT_SCRATCH_DIR
        # Set by enrich_many() when refresh() fails; None means the last refresh
        # succeeded (or has not run). Orchestration can inspect this to decide
        # halt-vs-skip without parsing per-row errors.
        self.refresh_error = None

    # --- subclass hooks -------------------------------------------------

    def _headers(self):
        """Return request headers for downloads. Override per provider if needed."""
        return {"User-Agent": USER_AGENT}

    async def refresh(self):
        """Download/refresh the local dataset into self.scratch_dir. Idempotent.

        Implementations should raise DatasetUnavailable on a reachability/HTTP
        problem (use self._download, which does this for you). Any other exception
        is also caught by enrich_many and surfaced as ok=False rows, but
        DatasetUnavailable is the intended, expected signal for "source is down".
        """
        raise NotImplementedError

    def lookup(self, domain):
        """Synchronous local lookup. No network.

        Return the adapter-specific data dict for `domain` (a hit), or None as
        shorthand for "not present in the dataset" (a miss). You do NOT need to set
        `present` yourself: the base stamps a uniform `present` flag onto the
        stored payload either way — present=True alongside your dict on a hit,
        {"present": False} on a miss — so the scoring layer can always read
        data["present"]. A miss is recorded as a successful ok=True row, so it
        stays distinguishable from an 'errored' row."""
        raise NotImplementedError

    # --- result helpers (mirror EnrichmentAdapter; see PROJECT.md) ------

    def _result(self, domain, data):
        """Build a success result dict (same shape as EnrichmentAdapter._result)."""
        return {
            "domain": domain,
            "adapter": self.name,
            "ok": True,
            "error": None,
            "fetched_at": datetime.utcnow().isoformat(),
            "data": data or {},
        }

    def _error(self, domain, message):
        """Build a failure result dict (same shape as EnrichmentAdapter._error)."""
        return {
            "domain": domain,
            "adapter": self.name,
            "ok": False,
            "error": str(message)[:500],
            "fetched_at": datetime.utcnow().isoformat(),
            "data": {},
        }

    # --- download / scratch helpers -------------------------------------

    def _scratch_path(self, filename):
        """Return scratch_dir/<name>/<filename>, creating the per-adapter dir."""
        d = self.scratch_dir / self.name
        d.mkdir(parents=True, exist_ok=True)
        return d / filename

    @staticmethod
    def _is_fresh(path, max_age_seconds):
        """True if `path` exists and was modified within max_age_seconds. Lets a
        subclass skip a re-download within the same day. Uses mtime; no clock
        helpers needed beyond stat()."""
        p = Path(path)
        if not p.exists():
            return False
        # time.time() is a true POSIX epoch, directly comparable to st_mtime.
        # (datetime.utcnow().timestamp() would treat the naive UTC datetime as
        # local time and be wrong by the local UTC offset.)
        age = time.time() - p.stat().st_mtime
        return age <= max_age_seconds

    async def _download(self, url, dest, headers=None):
        """Stream `url` to `dest`, returning the Path. Streams so a 200MB file
        never lands in memory.

        Raises DatasetUnavailable on any reachability/HTTP problem (connect/read
        timeout, DNS failure, connection refused, non-2xx status) — a clean,
        typed signal that the source is down, not a crash. A partial file from a
        failed download is removed so a later run does not read a truncated corpus.
        Also asserts downloaded bytes match Content-Length when the header is
        present and the body is not content-encoded; a mismatch raises
        DatasetUnavailable (see the integrity-check comment below).
        """
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            async with httpx.AsyncClient(
                headers=headers or self._headers(),
                timeout=self.TIMEOUT,
                follow_redirects=True,
                http2=False,
                # TLS verification stays ON for dataset downloads — deliberately
                # unlike checker.py (verify=False). Different threat model: a corpus
                # is an authoritative scoring input, so a MITM'd or forged corpus
                # would silently corrupt every audit. If a specific source (e.g.
                # Curlie's LRZ bucket) turns out to have a cert problem, add a
                # documented per-adapter verify override in Phase 2 — never disable
                # globally.
                verify=True,
            ) as client:
                async with client.stream("GET", url) as resp:
                    if resp.status_code >= 400:
                        raise DatasetUnavailable(
                            f"{self.name}: HTTP {resp.status_code} fetching {url}"
                        )
                    written = 0
                    with open(tmp, "wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=1 << 20):
                            written += len(chunk)
                            f.write(chunk)
                    # Integrity check: guard against a cleanly-but-early-closed
                    # connection that yields a silently truncated corpus. When the
                    # body is not content-encoded, the bytes on disk must equal the
                    # declared Content-Length; a mismatch raises DatasetUnavailable
                    # (partial file cleaned up by the except block) — same posture as
                    # the reachability failures. We cannot size-check gzip/chunked
                    # transfers (Content-Length, if present, describes the COMPRESSED
                    # wire bytes, not the decoded file), nor a response with no
                    # Content-Length: in those cases we WARN and proceed rather than
                    # assert. (httpx already raises on most premature closes; this
                    # covers the clean-early-close case it does not.)
                    encoding = resp.headers.get("content-encoding", "").strip().lower()
                    declared = resp.headers.get("content-length")
                    if declared is None:
                        print(f"dataset {self.name}: no Content-Length, skipping size check")
                    elif encoding not in ("", "identity"):
                        print(f"dataset {self.name}: Content-Length describes "
                              f"{encoding}-encoded bytes, skipping size check")
                    else:
                        try:
                            expected_n = int(declared)
                        except ValueError:
                            expected_n = None
                        if expected_n is None:
                            print(f"dataset {self.name}: unparseable Content-Length "
                                  f"{declared!r}, skipping size check")
                        elif written != expected_n:
                            raise DatasetUnavailable(
                                f"{self.name}: truncated download from {url}: "
                                f"got {written} of {expected_n} bytes"
                            )
            tmp.replace(dest)
            return dest
        except DatasetUnavailable:
            tmp.unlink(missing_ok=True)
            raise
        except (httpx.TimeoutException, httpx.TransportError) as e:
            # Connect/read timeout, DNS failure, connection refused, etc.
            tmp.unlink(missing_ok=True)
            raise DatasetUnavailable(f"{self.name}: cannot reach {url}: {e}") from e
        except httpx.HTTPError as e:
            tmp.unlink(missing_ok=True)
            raise DatasetUnavailable(f"{self.name}: http error fetching {url}: {e}") from e

    def _discard(self):
        """Delete this adapter's scratch subdir (the download -> query -> discard
        tail). Best-effort; never raises."""
        d = self.scratch_dir / self.name
        try:
            if d.exists():
                shutil.rmtree(d)
        except Exception as e:
            # Never raise — but a failed cleanup leaves stale data on disk, which
            # should be visible in logs, not silent.
            print(f"dataset {self.name}: discard failed (continuing): {e}")

    # --- orchestration (do not override) --------------------------------

    async def enrich_many(self, domains, on_progress=None):
        """Refresh the corpus once, then look up each domain locally.

        Returns a list of result dicts (same shape as EnrichmentAdapter). If
        refresh() fails, every domain gets a clean ok=False row and
        self.refresh_error is set — the run is not crashed, and orchestration
        decides halt-vs-skip.
        """
        domains = [d for d in domains if d]
        if not domains:
            # Nothing to look up — don't download a corpus for no reason.
            return []
        total = len(domains)
        results = []
        self.refresh_error = None

        # Refresh the corpus once per run. A download failure is expected-possible
        # (source may be momentarily down), so it produces clean ok=False rows
        # rather than propagating.
        try:
            await self.refresh()
        except DatasetUnavailable as e:
            self.refresh_error = str(e)
        except Exception as e:
            # An unexpected refresh error (parse bug, disk full, ...). Still no
            # crash — surface it the same way, but label it so it's distinguishable
            # from a plain availability problem.
            self.refresh_error = f"refresh failed: {type(e).__name__}: {e}"

        if self.refresh_error is not None:
            err = f"dataset unavailable: {self.refresh_error}"
            done = 0
            for domain in domains:
                row = self._error(domain, err)
                # Tag every row of a failed-refresh batch as non-persistable. These
                # rows carry no real per-domain data — they exist only to report the
                # outage — and writing them would clobber prior good rows under
                # save_enrichment's last-write-wins upsert. save_enrichment and
                # save_enrichment_batch refuse to persist any row with this flag set.
                row["refresh_error"] = self.refresh_error
                results.append(row)
                done += 1
                _fire(on_progress, done, total)
            return results

        # Local lookups: synchronous, no network — no semaphore needed.
        done = 0
        for domain in domains:
            try:
                data = self.lookup(domain)
                if data is None:
                    # Absent: a successful lookup that found nothing.
                    payload = {"present": False}
                else:
                    # Present: stamp present=True alongside the subclass payload so
                    # scoring has a uniform data["present"] interface across every
                    # dataset adapter. A non-None return always means present.
                    payload = {**data, "present": True}
                results.append(self._result(domain, payload))
            except Exception as e:
                results.append(self._error(domain, f"{type(e).__name__}: {e}"))
            done += 1
            _fire(on_progress, done, total)

        if self.DISCARD_AFTER_RUN and not _keep_cache():
            self._discard()
        return results


def _keep_cache():
    """True if DATASET_KEEP_CACHE opts out of strict discard (dev convenience).
    Checked at runtime so a developer can keep the local corpus for iteration
    without changing the production DISCARD_AFTER_RUN default."""
    return os.environ.get("DATASET_KEEP_CACHE", "").strip().lower() in (
        "1", "true", "yes", "on",
    )


def _fire(on_progress, done, total):
    """Call the progress callback, swallowing any error it raises. Mirrors
    base.py._fire so a callback bug never kills a run."""
    if on_progress:
        try:
            on_progress(done, total)
        except Exception:
            pass
