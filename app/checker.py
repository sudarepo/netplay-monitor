"""Domain checker.

For each domain we check both the apex (example.com) and www subdomain (www.example.com).
Each check returns a dict with:
  - status: 'operational' | 'down' | 'error'
  - http_code, final_url, response_ms, redirect_chain
  - dns_a_records (the IP(s) the hostname resolves to)
  - ssl_issuer, ssl_expires_at, ssl_days_left (when HTTPS works)
  - error (when something went wrong)
"""
import asyncio
import socket
import ssl
from datetime import datetime, timezone
from urllib.parse import urlparse

import dns.resolver
import dns.exception
import httpx


# How long we wait per phase. Keep these tight — with 650 checks even small
# timeouts add up if we let any single one stall the run.
HTTP_TIMEOUT = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)
DNS_TIMEOUT = 4.0
SSL_TIMEOUT = 5.0

# Pretend to be a real browser. Some shared hosting / WAFs block default httpx UA.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Send a full set of browser-equivalent request headers. Cloudflare and other
# bot managers fingerprint requests partly by which headers are present and in
# which order — sending ONLY a User-Agent is itself a giveaway. Adding the
# Accept / Accept-Language / Sec-Fetch-* headers makes us look like a real
# Chrome session and gets us past most challenge layers.
BROWSER_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# Cap concurrent checks. 650 checks at once will saturate your network and trigger
# rate limiting on shared infra. 50 in flight is a sweet spot for a Mac.
MAX_CONCURRENCY = 50


async def resolve_dns(hostname: str):
    """Return list of A record IPs for hostname, or [] if it doesn't resolve."""
    loop = asyncio.get_running_loop()
    try:
        # dnspython is sync; run in thread to keep event loop free.
        def _do_resolve():
            resolver = dns.resolver.Resolver()
            resolver.lifetime = DNS_TIMEOUT
            resolver.timeout = DNS_TIMEOUT
            try:
                ans = resolver.resolve(hostname, "A")
                return [r.address for r in ans]
            except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers):
                return []
            except dns.exception.Timeout:
                return None  # signal timeout vs empty
        return await loop.run_in_executor(None, _do_resolve)
    except Exception:
        return None


async def get_ssl_info(hostname: str):
    """Connect to hostname:443 and read the cert. Returns dict or None.

    Uses cryptography lib to parse the DER-encoded cert directly, which avoids
    Python's stdlib quirk of returning empty cert dicts when verify_mode is
    CERT_NONE. We want to read certs even when invalid/expired so we can report
    the expiry and issuer regardless.
    """
    loop = asyncio.get_running_loop()

    def _do_ssl():
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            with socket.create_connection((hostname, 443), timeout=SSL_TIMEOUT) as sock:
                with ctx.wrap_socket(sock, server_hostname=hostname) as ssock:
                    der = ssock.getpeercert(binary_form=True)
                    if not der:
                        return None
                    cert = x509.load_der_x509_certificate(der, default_backend())

                    # Issuer organization name (fall back to common name).
                    issuer = "Unknown"
                    try:
                        org = cert.issuer.get_attributes_for_oid(x509.NameOID.ORGANIZATION_NAME)
                        if org:
                            issuer = org[0].value
                        else:
                            cn = cert.issuer.get_attributes_for_oid(x509.NameOID.COMMON_NAME)
                            if cn:
                                issuer = cn[0].value
                    except Exception:
                        pass

                    # Expiry. Use UTC-aware variant when available (cryptography >= 42).
                    exp_dt = getattr(cert, "not_valid_after_utc", None) or cert.not_valid_after
                    if exp_dt.tzinfo is None:
                        exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                    days_left = (exp_dt - datetime.now(timezone.utc)).days

                    return {
                        "issuer": issuer,
                        "expires_at": exp_dt.isoformat(),
                        "days_left": days_left,
                    }
        except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError, ssl.SSLError):
            return None
        except Exception:
            return None

    try:
        return await loop.run_in_executor(None, _do_ssl)
    except Exception:
        return None


async def check_url(client: httpx.AsyncClient, url: str):
    """Issue a GET, follow redirects, capture the chain. Returns dict."""
    started = datetime.now(timezone.utc)
    t0 = asyncio.get_running_loop().time()
    redirect_chain = []
    try:
        # follow_redirects=True means httpx auto-follows; we read history afterwards.
        resp = await client.get(url, follow_redirects=True, timeout=HTTP_TIMEOUT)
        elapsed_ms = int((asyncio.get_running_loop().time() - t0) * 1000)

        for h in resp.history:
            redirect_chain.append({
                "from": str(h.url),
                "to": h.headers.get("location", ""),
                "code": h.status_code,
            })

        # Capture select response headers + a body snippet for challenge detection.
        # We only sniff the first ~4KB — challenge pages always declare themselves up top.
        server = resp.headers.get("server", "")
        cf_ray = resp.headers.get("cf-ray", "")
        cf_mitigated = resp.headers.get("cf-mitigated", "")
        body_snippet = ""
        try:
            body_snippet = resp.text[:4096] if resp.text else ""
        except Exception:
            body_snippet = ""

        return {
            "ok": True,
            "http_code": resp.status_code,
            "final_url": str(resp.url),
            "response_ms": elapsed_ms,
            "redirect_chain": redirect_chain,
            "started_at": started.isoformat(),
            "server_header": server,
            "cf_ray": cf_ray,
            "cf_mitigated": cf_mitigated,
            "body_snippet": body_snippet,
        }
    except httpx.TimeoutException:
        return {"ok": False, "error": "timeout", "started_at": started.isoformat()}
    except httpx.ConnectError as e:
        return {"ok": False, "error": f"connection refused: {e}", "started_at": started.isoformat()}
    except httpx.HTTPError as e:
        return {"ok": False, "error": f"http error: {e}", "started_at": started.isoformat()}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "started_at": started.isoformat()}


def _detect_protection(http_result):
    """Return a string describing the protection layer if the response looks like a
    bot challenge / WAF block from a real, working site. Otherwise return None.

    These pages legitimately serve a 4xx/5xx but the underlying domain is operational —
    a real browser with JavaScript would pass through and reach the site.
    """
    if not http_result.get("ok"):
        return None
    code = http_result.get("http_code")
    server = (http_result.get("server_header") or "").lower()
    cf_ray = http_result.get("cf_ray") or ""
    cf_mitigated = (http_result.get("cf_mitigated") or "").lower()
    body = (http_result.get("body_snippet") or "").lower()

    # Cloudflare specifically.
    if cf_ray or "cloudflare" in server:
        # Active mitigation header (set when CF is challenging).
        if "challenge" in cf_mitigated:
            return "Cloudflare challenge"
        # Body-based fingerprints — these phrases appear on CF interstitials.
        cf_phrases = (
            "just a moment",
            "checking your browser",
            "cf-browser-verification",
            "cf-challenge",
            "challenge-platform",
            "attention required",
            "ddos protection by cloudflare",
        )
        if any(p in body for p in cf_phrases) and code in (403, 503, 429):
            return "Cloudflare challenge"
        # 403 from Cloudflare without explicit challenge body — likely WAF block,
        # but the domain itself is still alive and serving via CF.
        if code == 403:
            return "Cloudflare WAF block"

    # Other common WAFs / bot managers.
    waf_signatures = [
        ("akamai", "Akamai"),
        ("incapsula", "Imperva"),
        ("imperva", "Imperva"),
        ("sucuri", "Sucuri"),
        ("perimeterx", "PerimeterX"),
        ("datadome", "DataDome"),
    ]
    for needle, label in waf_signatures:
        if needle in server or needle in body:
            if code in (403, 429, 503):
                return f"{label} block"

    # Generic bot-block phrases on a 403/429 from any server.
    if code in (403, 429):
        bot_phrases = (
            "access denied",
            "request blocked",
            "you have been blocked",
            "are you a robot",
            "verify you are human",
        )
        if any(p in body for p in bot_phrases):
            return "WAF block"

    return None


async def check_target(client: httpx.AsyncClient, domain: str, target: str):
    """Check one target (apex or www). Returns the full result dict ready for db.save_check."""
    hostname = domain if target == "apex" else f"www.{domain}"

    # Try HTTPS first, fall back to HTTP if HTTPS connection itself fails.
    https_url = f"https://{hostname}"
    http_url = f"http://{hostname}"

    # Run DNS and SSL in parallel with the HTTP request — they're independent.
    dns_task = asyncio.create_task(resolve_dns(hostname))
    ssl_task = asyncio.create_task(get_ssl_info(hostname))
    http_result = await check_url(client, https_url)

    # If HTTPS failed at the connection level, try plain HTTP — many old domains
    # are HTTP-only or have broken certs but still serve content.
    fell_back = False
    if not http_result["ok"] and "connection" in (http_result.get("error") or "").lower():
        http_result = await check_url(client, http_url)
        fell_back = True

    dns_ips = await dns_task
    ssl_info = await ssl_task

    # Build the unified result.
    result = {
        "domain": domain,
        "target": target,
        "checked_at": http_result.get("started_at") or datetime.now(timezone.utc).isoformat(),
        "dns_a_records": dns_ips if dns_ips is not None else [],
    }

    # Determine status.
    if dns_ips is None:
        result["status"] = "error"
        result["error"] = "DNS resolution timed out"
    elif not dns_ips and not http_result["ok"]:
        result["status"] = "down"
        result["error"] = "domain does not resolve (no A records)"
    elif http_result["ok"]:
        code = http_result["http_code"]
        protection = _detect_protection(http_result)
        # Inferred-protected fallback: if the site has a valid SSL cert AND
        # responds with a typical bot-block code (403, 429, 503), it's almost
        # certainly operational in a real browser — the server is up enough to
        # complete TLS and respond, it's just refusing non-browser clients.
        # We use this when the WAF doesn't identify itself in headers/body.
        ssl_ok = (
            ssl_info is not None
            and ssl_info.get("days_left") is not None
            and ssl_info.get("days_left") >= 0
        )
        looks_like_bot_block = code in (403, 429, 503)

        if 200 <= code < 400:
            result["status"] = "operational"
        elif protection:
            result["status"] = "protected"
            result["error"] = f"{protection} (HTTP {code}) — site likely operational in a real browser"
        elif looks_like_bot_block and ssl_ok:
            # SSL works, server is alive, just blocking non-browser clients.
            result["status"] = "protected"
            result["error"] = f"HTTP {code} with valid SSL — likely bot/WAF block, site is reachable"
        else:
            result["status"] = "down"
            result["error"] = f"HTTP {code}"
        result["http_code"] = code
        result["final_url"] = http_result["final_url"]
        result["response_ms"] = http_result["response_ms"]
        result["redirect_chain"] = http_result["redirect_chain"]
        if fell_back:
            result["error"] = (result.get("error") or "") + " (HTTPS failed, fell back to HTTP)"
            result["error"] = result["error"].strip()
    else:
        result["status"] = "down"
        result["error"] = http_result.get("error") or "unknown error"

    if ssl_info:
        result["ssl_issuer"] = ssl_info.get("issuer")
        result["ssl_expires_at"] = ssl_info.get("expires_at")
        result["ssl_days_left"] = ssl_info.get("days_left")

    return result


async def run_checks(domains, on_progress=None):
    """Check apex + www for every domain. Returns list of result dicts.

    on_progress(done, total) is called after each completion if provided.
    """
    targets = []
    for d in domains:
        targets.append((d, "apex"))
        targets.append((d, "www"))

    total = len(targets)
    done = 0
    results = []
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    headers = BROWSER_HEADERS
    limits = httpx.Limits(max_connections=MAX_CONCURRENCY * 2, max_keepalive_connections=MAX_CONCURRENCY)

    async with httpx.AsyncClient(headers=headers, limits=limits, http2=False, verify=False) as client:
        async def _one(domain, target):
            nonlocal done
            async with sem:
                r = await check_target(client, domain, target)
                done += 1
                if on_progress:
                    try:
                        on_progress(done, total)
                    except Exception:
                        pass
                return r

        tasks = [asyncio.create_task(_one(d, t)) for d, t in targets]
        for coro in asyncio.as_completed(tasks):
            results.append(await coro)

    return results
