import asyncio
import logging
import os
import random
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote, unquote, urljoin, urlparse

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, RedirectResponse
from playwright.async_api import Browser, Playwright, async_playwright

# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("vidsrc-resolver")


class Config:
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "7000"))
    CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "1800"))
    PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "15000"))
    STREAM_WAIT_TIMEOUT = float(os.getenv("STREAM_WAIT_TIMEOUT", "12.0"))
    GLOBAL_RESOLVE_TIMEOUT = float(os.getenv("GLOBAL_RESOLVE_TIMEOUT", "25.0"))
    MAX_CONCURRENT_PROVIDERS = int(os.getenv("MAX_CONCURRENT_PROVIDERS", "7"))
    SAVE_DEBUG_ARTIFACTS = os.getenv("SAVE_DEBUG_ARTIFACTS", "False").lower() in ("true", "1")
    DEBUG_DIR = os.getenv("DEBUG_DIR", "./debug_captures")
    PLAYLIST_CACHE_TTL = float(os.getenv("PLAYLIST_CACHE_TTL", "5.0"))

    ADDON_WAIT_TIMEOUT = float(os.getenv("ADDON_WAIT_TIMEOUT", "5.0"))  # seconds the add-on will wait for direct URL

    DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    ]

    ID_LIST_REFRESH_SEC = int(os.getenv("ID_LIST_REFRESH_SEC", "7200"))
    ID_LIST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "id_lists")
    ID_LIST_BASE = "https://vidsrcme.ru/ids"
    ID_LISTS = {
        "movie_imdb": f"{ID_LIST_BASE}/movie_imdb.txt",
        "tv_imdb":    f"{ID_LIST_BASE}/tv_imdb.txt",
        "eps_imdb":   f"{ID_LIST_BASE}/eps_imdb.txt",
    }


# ------------------------------------------------------------------------------
@dataclass
class Provider:
    name: str
    tv_template: str
    movie_template: str
    priority: int = 10
    enabled: bool = True
    success_count: int = 0
    failure_count: int = 0

    def get_url(self, imdb_id: str, season: Optional[str] = None, episode: Optional[str] = None) -> str:
        if season and episode:
            return self.tv_template.format(imdb=imdb_id, season=season, episode=episode)
        return self.movie_template.format(imdb=imdb_id)


# --- REORDERED PROVIDERS: reliable ones first ---
PROVIDERS: List[Provider] = [
    Provider(
        name="vidsrc.in",
        tv_template="https://vidsrc.in/embed/tv/{imdb}/{season}/{episode}",
        movie_template="https://vidsrc.in/embed/movie/{imdb}",
        priority=1,
    ),
    Provider(
        name="vidsrc.pm",
        tv_template="https://vidsrc.pm/embed/tv/{imdb}/{season}/{episode}",
        movie_template="https://vidsrc.pm/embed/movie/{imdb}",
        priority=2,
    ),
    Provider(
        name="vidsrc.net",
        tv_template="https://vidsrc.net/embed/tv/{imdb}/{season}/{episode}",
        movie_template="https://vidsrc.net/embed/movie/{imdb}",
        priority=3,
    ),
    Provider(
        name="vidsrc.me",
        tv_template="https://vidsrc.me/embed/tv?imdb={imdb}&season={season}&episode={episode}",
        movie_template="https://vidsrc.me/embed/movie?imdb={imdb}",
        priority=4,
    ),
    Provider(
        name="vidsrc.io",
        tv_template="https://vidsrc.io/embed/tv/{imdb}/{season}/{episode}",
        movie_template="https://vidsrc.io/embed/movie/{imdb}",
        priority=5,
    ),
    Provider(
        name="vidsrc.vip",
        tv_template="https://vidsrc.vip/embed/tv/{imdb}/{season}/{episode}",
        movie_template="https://vidsrc.vip/embed/movie/{imdb}",
        priority=6,
    ),
    Provider(
        name="vidsrc.sbs",
        tv_template="https://vidsrc.sbs/embed/tv/{imdb}/{season}/{episode}",
        movie_template="https://vidsrc.sbs/embed/movie/{imdb}",
        priority=7,
    ),
]


# ------------------------------------------------------------------------------
class AppState:
    playwright: Optional[Playwright] = None
    browser: Optional[Browser] = None
    client: Optional[httpx.AsyncClient] = None
    stream_cache: Dict[str, dict] = {}
    session_cache: Dict[str, dict] = {}
    playlist_cache: Dict[str, dict] = {}
    cache_lock = asyncio.Lock()
    metrics = {
        "total_requests": 0,
        "cache_hits": 0,
        "successful_resolves": 0,
        "failed_resolves": 0,
        "unsupported_rejects": 0,
        "token_refreshes": 0,
    }

    movie_ids: Set[str] = set()
    tv_ids: Set[str] = set()
    episode_ids: Set[str] = set()

    in_flight: Dict[str, asyncio.Task] = {}   # prevent duplicate sniff sessions


state = AppState()


# ------------------------------------------------------------------------------
async def ensure_browser() -> Browser:
    if state.browser is None or not state.browser.is_connected():
        logger.warning("Launching persistent Chromium...")
        if state.playwright is None:
            state.playwright = await async_playwright().start()
        state.browser = await state.playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-first-run",
                "--no-zygote",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        logger.info("Chromium engine ready.")
    return state.browser


# ------------------------------------------------------------------------------
async def fetch_ids_file(name: str, remote_url: str) -> Set[str]:
    os.makedirs(Config.ID_LIST_DIR, exist_ok=True)
    local_path = os.path.join(Config.ID_LIST_DIR, f"{name}.txt")

    if os.path.exists(local_path):
        file_age = time.time() - os.path.getmtime(local_path)
        if file_age < Config.ID_LIST_REFRESH_SEC:
            try:
                with open(local_path, "r") as f:
                    lines = f.readlines()
                return {line.strip() for line in lines if line.strip() and not line.startswith("#")}
            except Exception:
                logger.warning(f"Failed to read local ID list {local_path}, re-downloading.")

    try:
        resp = await state.client.get(remote_url, headers={"User-Agent": Config.DEFAULT_UA})
        if resp.status_code == 200:
            text = resp.text
            with open(local_path, "w") as f:
                f.write(text)
            lines = text.strip().splitlines()
            return {line.strip() for line in lines if line.strip() and not line.startswith("#")}
        else:
            logger.warning(f"Failed to fetch ID list {remote_url} (status {resp.status_code})")
    except Exception as e:
        logger.warning(f"Error fetching ID list {remote_url}: {e}")

    if os.path.exists(local_path):
        try:
            with open(local_path, "r") as f:
                lines = f.readlines()
            return {line.strip() for line in lines if line.strip() and not line.startswith("#")}
        except Exception:
            pass

    return set()


async def refresh_id_lists():
    movie_set = await fetch_ids_file("movie_imdb", Config.ID_LISTS["movie_imdb"])
    tv_set = await fetch_ids_file("tv_imdb", Config.ID_LISTS["tv_imdb"])
    eps_set = await fetch_ids_file("eps_imdb", Config.ID_LISTS["eps_imdb"])
    async with state.cache_lock:
        state.movie_ids = movie_set
        state.tv_ids = tv_set
        state.episode_ids = eps_set
    logger.info(
        f"ID lists refreshed: movies={len(movie_set)}, tv={len(tv_set)}, episodes={len(eps_set)}"
    )


async def periodic_id_list_refresh():
    while True:
        await refresh_id_lists()
        await asyncio.sleep(Config.ID_LIST_REFRESH_SEC)


def is_content_supported(imdb_id: str, season: Optional[str], episode: Optional[str]) -> bool:
    if season and episode:
        ep_key = f"{imdb_id}_{season}x{episode}"
        return ep_key in state.episode_ids
    elif season:
        return imdb_id in state.tv_ids
    else:
        return imdb_id in state.movie_ids


# ------------------------------------------------------------------------------
async def periodic_cache_cleanup():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        async with state.cache_lock:
            for k in list(state.stream_cache):
                if now - state.stream_cache[k].get("timestamp", 0) > Config.CACHE_TTL_SECONDS:
                    del state.stream_cache[k]
            for k in list(state.session_cache):
                if k != "global_last" and now - state.session_cache[k].get("timestamp", 0) > Config.CACHE_TTL_SECONDS:
                    del state.session_cache[k]
            for k in list(state.playlist_cache):
                if now - state.playlist_cache[k].get("timestamp", 0) > Config.PLAYLIST_CACHE_TTL:
                    del state.playlist_cache[k]


@asynccontextmanager
async def lifespan(app: FastAPI):
    if Config.SAVE_DEBUG_ARTIFACTS and not os.path.exists(Config.DEBUG_DIR):
        os.makedirs(Config.DEBUG_DIR, exist_ok=True)
    timeout_cfg = httpx.Timeout(10.0, connect=10.0, read=30.0)
    state.client = httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout_cfg,
        verify=False,
        limits=httpx.Limits(max_keepalive_connections=100, max_connections=300, keepalive_expiry=15.0),
    )
    await ensure_browser()
    await refresh_id_lists()
    id_refresh_task = asyncio.create_task(periodic_id_list_refresh())
    cleanup = asyncio.create_task(periodic_cache_cleanup())
    logger.info("VidSrc Resolver API started.")
    yield
    id_refresh_task.cancel()
    cleanup.cancel()
    if state.client:
        await state.client.aclose()
    if state.browser:
        await state.browser.close()
    if state.playwright:
        await state.playwright.stop()
    logger.info("Server shut down.")


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------------------
#  IMPROVED SNIFFER – with early bailout for dead providers
# ------------------------------------------------------------------------------
FAKE_VIDEO_PATTERNS = [
    "canAutoplayInline", "sample", "test", "bumper", "advertisement"
]

def is_fake_stream(url: str) -> bool:
    url_lower = url.lower()
    return any(p in url_lower for p in FAKE_VIDEO_PATTERNS)


async def sniff_single_mirror(provider: Provider, embed_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    browser = await ensure_browser()
    ua = random.choice(Config.USER_AGENTS)
    context = None
    try:
        context = await browser.new_context(
            user_agent=ua,
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=1,
            is_mobile=False,
            has_touch=False,
            locale="en-US",
            timezone_id="America/New_York",
        )

        extracted_url = None
        stream_type = None
        stream_found = asyncio.Event()
        iframe_loaded = False

        async def detect_iframe(request):
            nonlocal iframe_loaded
            if request.resource_type == "document" and request.url != embed_url:
                iframe_loaded = True

        context.on("request", lambda req: asyncio.create_task(detect_iframe(req)))

        async def handle_request(request):
            nonlocal extracted_url, stream_type
            if stream_found.is_set():
                return
            full_url = request.url
            if is_fake_stream(full_url):
                return
            url = full_url.lower()
            domain = urlparse(request.url).netloc
            async with state.cache_lock:
                state.session_cache[domain] = {
                    "headers": dict(request.headers),
                    "user_agent": ua,
                    "embed_referer": embed_url,
                    "timestamp": time.time(),
                }
                state.session_cache["global_last"] = state.session_cache[domain]

            if any(
                x in url for x in [
                    ".m3u8", ".mpd", ".mp4", "/hls/", "/dash/", "master.m3u8",
                    "playlist.m3u8", "chunklist", "manifest", "index.m3u8",
                    "/stream/", "vidplay", "surrit", "streamtape"
                ]
            ) and ".js" not in url:
                if ".mp4" in url:
                    extracted_url = request.url
                    stream_type = "mp4"
                elif ".mpd" in url or "/dash/" in url:
                    extracted_url = request.url
                    stream_type = "mpd"
                else:
                    extracted_url = request.url
                    stream_type = "m3u8"
                logger.info(f"[{provider.name}] Intercepted {stream_type.upper()}: {request.url[:80]}...")
                stream_found.set()

        async def handle_response(response):
            nonlocal extracted_url, stream_type
            if stream_found.is_set():
                return
            try:
                full_url = response.url
                if is_fake_stream(full_url):
                    return
                url = full_url.lower()
                ct = (response.headers.get("content-type") or "").lower()
                if ("application/json" in ct or "text/plain" in ct or "api/" in url) and response.status == 200:
                    body = await response.text()
                    matches = re.findall(r'https?://[^\s"\'\\]+\.(?:m3u8|mpd|mp4)[^\s"\'\\]*', body)
                    if matches:
                        found_url = matches[0]
                        if not is_fake_stream(found_url):
                            stream_type = "mp4" if ".mp4" in found_url else ("mpd" if ".mpd" in found_url else "m3u8")
                            extracted_url = found_url
                            logger.info(f"[{provider.name}] Extracted {stream_type.upper()} from response body.")
                            stream_found.set()
            except Exception:
                pass

        context.on("request", lambda req: asyncio.create_task(handle_request(req)))
        context.on("response", lambda resp: asyncio.create_task(handle_response(resp)))

        page = await context.new_page()
        await page.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
            window.chrome = { runtime: {} };
            """
        )

        logger.info(f"[{provider.name}] Navigating to: {embed_url}")
        await page.goto(embed_url, wait_until="domcontentloaded", timeout=Config.PAGE_TIMEOUT_MS)

        start = time.time()
        while time.time() - start < 5.0:
            if iframe_loaded or stream_found.is_set():
                break
            await asyncio.sleep(0.5)
        if not iframe_loaded and not stream_found.is_set():
            logger.debug(f"[{provider.name}] No iframe loaded within 5s, aborting.")
            return None, None, None

        await asyncio.sleep(1.5)

        try:
            for frame in page.frames:
                try:
                    sources = await frame.evaluate("() => window.sources || window.source || window.videoUrl || null")
                    if sources:
                        url = sources if isinstance(sources, str) else (sources[0] if isinstance(sources, list) else None)
                        if url and not is_fake_stream(url):
                            extracted_url = url
                            stream_type = "m3u8" if ".m3u8" in url else "mpd" if ".mpd" in url else "mp4"
                            stream_found.set()
                            logger.info(f"[{provider.name}] Extracted from JS: {url[:80]}")
                except Exception:
                    continue
        except Exception:
            pass

        for _ in range(2):
            if stream_found.is_set():
                break
            for frame in page.frames:
                if stream_found.is_set():
                    break
                try:
                    for selector in ["#player", "#overlay", ".jw-video", "video", "#play", "body"]:
                        if stream_found.is_set():
                            break
                        element = await frame.query_selector(selector)
                        if element:
                            await element.click(force=True, timeout=300)
                            await asyncio.sleep(0.5)
                except Exception:
                    continue

        try:
            await asyncio.wait_for(stream_found.wait(), timeout=Config.STREAM_WAIT_TIMEOUT)
        except asyncio.TimeoutError:
            logger.debug(f"[{provider.name}] No stream found within {Config.STREAM_WAIT_TIMEOUT}s.")

        if extracted_url:
            provider.success_count += 1
            return extracted_url, stream_type, embed_url
    except Exception as err:
        logger.debug(f"[{provider.name}] Sniffer error: {err}")
    finally:
        if context:
            await context.close()

    provider.failure_count += 1
    return None, None, None


# ------------------------------------------------------------------------------
async def resolve_stream(
    imdb_id: str, season: Optional[str] = None, episode: Optional[str] = None
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    cache_key = f"{imdb_id}:{season or ''}:{episode or ''}"

    # 1. Return cached result if fresh
    async with state.cache_lock:
        cached = state.stream_cache.get(cache_key)
        if cached and (time.time() - cached["timestamp"] < Config.CACHE_TTL_SECONDS):
            state.metrics["cache_hits"] += 1
            return cached["stream_url"], cached["stype"], cached["referer"]

        # 2. If an in‑flight task exists, wait for it
        if cache_key in state.in_flight:
            task = state.in_flight[cache_key]
    if cache_key in state.in_flight:
        logger.info(f"Waiting for in‑flight resolution of {cache_key}")
        return await task

    # 3. Create a new resolution task
    async def do_resolve():
        active = [p for p in PROVIDERS if p.enabled]
        active.sort(key=lambda p: p.priority)
        sem = asyncio.Semaphore(Config.MAX_CONCURRENT_PROVIDERS)

        async def worker(p: Provider):
            async with sem:
                return await sniff_single_mirror(p, p.get_url(imdb_id, season, episode))

        tasks = [asyncio.create_task(worker(p)) for p in active]
        pending = set(tasks)
        result = (None, None, None)

        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    stream_url, stype, referer = task.result()
                    if stream_url:
                        result = (stream_url, stype, referer)
                        for t in pending:
                            t.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)
                        pending.clear()
                        break
                except Exception as e:
                    logger.debug(f"Task exception: {e}")

        stream_url, stype, referer = result
        async with state.cache_lock:
            if stream_url:
                state.stream_cache[cache_key] = {
                    "stream_url": stream_url,
                    "stype": stype,
                    "referer": referer,
                    "timestamp": time.time(),
                }
                state.metrics["successful_resolves"] += 1
            else:
                state.metrics["failed_resolves"] += 1
            # Remove from in‑flight after finishing
            state.in_flight.pop(cache_key, None)
        return result

    task = asyncio.create_task(do_resolve())
    async with state.cache_lock:
        state.in_flight[cache_key] = task

    return await task


# ------------------------------------------------------------------------------
#  TOKEN REFRESH – re‑extract fresh stream URL when 403 occurs
# ------------------------------------------------------------------------------
async def refresh_stream_url(original_referer: str) -> Optional[str]:
    parsed = urlparse(original_referer)
    provider_name = parsed.netloc.lower().replace("www.", "")
    provider = next((p for p in PROVIDERS if p.name == provider_name or provider_name.startswith(p.name)), None)
    if not provider:
        logger.warning(f"No provider found for {original_referer}")
        return None

    new_url, _, _ = await sniff_single_mirror(provider, original_referer)
    if new_url:
        logger.info(f"Token refreshed from {provider.name}")
        async with state.cache_lock:
            state.metrics["token_refreshes"] += 1
        return new_url
    return None


# ------------------------------------------------------------------------------
#  PROXY FUNCTIONS (with 403 token‑refresh handling)
# ------------------------------------------------------------------------------
def build_headers(target_url: str, referer: str, incoming_headers: Optional[dict] = None) -> dict:
    domain = urlparse(target_url).netloc
    session_info = (
        state.session_cache.get(domain)
        or state.session_cache.get("global_last")
        or {}
    )
    if "headers" in session_info and session_info["headers"]:
        headers = dict(session_info["headers"])
        headers["Host"] = domain
        if referer:
            headers["Referer"] = referer
    else:
        headers = {
            "User-Agent": Config.DEFAULT_UA,
            "Referer": referer or f"{urlparse(target_url).scheme}://{domain}/",
            "Origin": f"{urlparse(target_url).scheme}://{domain}",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
        }
    if incoming_headers:
        for k, v in incoming_headers.items():
            if k.lower() in ["range", "if-none-match"]:
                headers[k.capitalize()] = v
    return headers


async def fetch_upstream_m3u8(target_url: str, ref_header: str) -> str:
    async with state.cache_lock:
        cached = state.playlist_cache.get(target_url)
        if cached and (time.time() - cached["timestamp"] < Config.PLAYLIST_CACHE_TTL):
            return cached["content"]
    headers = build_headers(target_url, ref_header)
    for attempt in range(3):
        try:
            resp = await state.client.get(target_url, headers=headers)
            if resp.status_code == 200:
                text = resp.text
                async with state.cache_lock:
                    state.playlist_cache[target_url] = {"content": text, "timestamp": time.time()}
                return text
            elif resp.status_code == 403:
                logger.warning(f"Master playlist 403, attempting token refresh.")
                new_url = await refresh_stream_url(ref_header)
                if new_url:
                    raise HTTPException(status_code=302, detail=new_url)
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"M3U8 fetch attempt {attempt+1}: {e}")
            await asyncio.sleep(0.2 * (2**attempt))
    raise HTTPException(status_code=502, detail="Upstream M3U8 fetch failed")


async def proxy_stream_request(target_url: str, ref_header: str, req: Request):
    headers = build_headers(target_url, ref_header, dict(req.headers))
    try:
        req_ctx = state.client.stream("GET", target_url, headers=headers, follow_redirects=True)
        res = await req_ctx.__aenter__()
        if res.status_code in [200, 206]:
            out_headers = {"Access-Control-Allow-Origin": "*", "Accept-Ranges": "bytes"}
            for h in ["content-type", "content-length", "content-range"]:
                if h in res.headers:
                    out_headers[h] = res.headers[h]
            media_type = res.headers.get("content-type")
            if not media_type or "text/plain" in media_type or "octet-stream" in media_type:
                media_type = "video/mp2t" if ".ts" in target_url else "video/mp4"
            async def stream_bytes():
                try:
                    async for chunk in res.aiter_bytes(chunk_size=65536):
                        yield chunk
                finally:
                    await req_ctx.__aexit__(None, None, None)
            return StreamingResponse(stream_bytes(), status_code=res.status_code, media_type=media_type, headers=out_headers)
        elif res.status_code == 403:
            logger.warning(f"Segment 403, refreshing token...")
            new_url = await refresh_stream_url(ref_header)
            if new_url:
                base = str(req.base_url)
                if ".mp4" in new_url:
                    new_proxy = f"{base}proxy/media?url={quote(new_url)}&referer={quote(ref_header)}"
                else:
                    new_proxy = f"{base}proxy/m3u8?url={quote(new_url)}&referer={quote(ref_header)}"
                return RedirectResponse(url=new_proxy, status_code=302)
        await req_ctx.__aexit__(None, None, None)
    except Exception as err:
        logger.error(f"Proxy stream error: {err}")
    raise HTTPException(status_code=502, detail="Upstream media fetch failed")


# ------------------------------------------------------------------------------
#  LAZY PROXY – resolves stream on demand, then redirects
# ------------------------------------------------------------------------------
@app.api_route("/lazy-proxy", methods=["GET", "HEAD"])
async def lazy_proxy(
    imdb: str,
    season: Optional[str] = None,
    episode: Optional[str] = None,
    req: Request = None,
):
    """
    Resolve the stream lazily and redirect to the correct proxy endpoint.
    """
    stream_url, resolved_type, referer = await resolve_stream(imdb, season, episode)
    if not stream_url or not referer:
        raise HTTPException(status_code=502, detail="No stream found for this content")

    base = str(req.base_url)
    if resolved_type == "mp4":
        proxy_target = f"{base}proxy/media?url={quote(stream_url)}&referer={quote(referer)}"
    elif resolved_type == "mpd":
        proxy_target = f"{base}proxy/media?url={quote(stream_url)}&referer={quote(referer)}"
    else:  # m3u8 or anything else
        proxy_target = f"{base}proxy/m3u8?url={quote(stream_url)}&referer={quote(referer)}"

    return RedirectResponse(url=proxy_target, status_code=302)


# ------------------------------------------------------------------------------
#  EXISTING PROXY ENDPOINTS
# ------------------------------------------------------------------------------
@app.api_route("/proxy/media", methods=["GET", "HEAD"])
async def proxy_media(url: str, referer: str, req: Request):
    return await proxy_stream_request(unquote(url), unquote(referer), req)


@app.api_route("/proxy/segment", methods=["GET", "HEAD"])
async def proxy_segment(url: str, referer: str, req: Request):
    return await proxy_stream_request(unquote(url), unquote(referer), req)


@app.api_route("/proxy/m3u8", methods=["GET", "HEAD"])
async def proxy_m3u8(url: str, referer: str, req: Request):
    target_url = unquote(url)
    ref_header = unquote(referer)
    base_proxy = str(req.base_url)

    try:
        content_text = await fetch_upstream_m3u8(target_url, ref_header)
    except HTTPException as e:
        if e.status_code == 302:
            new_url = e.detail
            return RedirectResponse(
                url=f"{base_proxy}proxy/m3u8?url={quote(new_url)}&referer={quote(ref_header)}",
                status_code=302
            )
        raise

    lines = content_text.splitlines()
    rewritten = []
    is_variant = False
    for line in lines:
        line_str = line.strip()
        if not line_str:
            continue
        if line_str.startswith("#"):
            if line_str.startswith("#EXT-X-STREAM-INF") or line_str.startswith("#EXT-I-FRAME-STREAM-INF"):
                is_variant = True
            def rewrite_uri(m):
                tag = m.group(1)
                raw = m.group(2)
                full = urljoin(target_url, raw)
                if "EXT-X-MEDIA" in line_str:
                    return f'{tag}="{base_proxy}proxy/m3u8?url={quote(full)}&referer={quote(ref_header)}"'
                return f'{tag}="{base_proxy}proxy/segment?url={quote(full)}&referer={quote(ref_header)}"'
            rewritten_line = re.sub(r'(URI)=["\']([^"\']+)["\']', rewrite_uri, line_str)
            rewritten.append(rewritten_line)
        else:
            full = urljoin(target_url, line_str)
            if is_variant or ".m3u8" in full.lower() or "playlist" in full.lower() or "master" in full.lower():
                proxy_line = f"{base_proxy}proxy/m3u8?url={quote(full)}&referer={quote(ref_header)}"
            else:
                proxy_line = f"{base_proxy}proxy/segment?url={quote(full)}&referer={quote(ref_header)}"
            rewritten.append(proxy_line)
            is_variant = False
    return Response(
        content="\n".join(rewritten),
        media_type="application/vnd.apple.mpegurl",
        headers={"Access-Control-Allow-Origin": "*", "Content-Type": "application/vnd.apple.mpegurl"},
    )


# ------------------------------------------------------------------------------
#  ROUTES
# ------------------------------------------------------------------------------
@app.get("/health")
async def health():
    browser_ok = state.browser is not None and state.browser.is_connected()
    client_ok = state.client is not None and not state.client.is_closed
    return {
        "status": "healthy" if (browser_ok and client_ok) else "degraded",
        "browser_connected": browser_ok,
        "http_client_active": client_ok,
        "stream_cache": len(state.stream_cache),
        "playlist_cache": len(state.playlist_cache),
    }


@app.get("/metrics")
async def metrics():
    async with state.cache_lock:
        m = dict(state.metrics)
    return {
        "metrics": m,
        "providers": [{"name": p.name, "successes": p.success_count, "failures": p.failure_count} for p in PROVIDERS],
        "stream_cache": len(state.stream_cache),
        "playlist_cache": len(state.playlist_cache),
    }


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.vidsrc.local.addon",
        "version": "6.0.0",
        "name": "VidSrc Resolver (Hybrid Fast‑Start)",
        "description": "Add‑on waits up to 5s for direct stream, else falls back to lazy proxy.",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
    }


@app.get("/stream/{type}/{id}.json")
async def get_stream(type: str, id: str, req: Request):
    async with state.cache_lock:
        state.metrics["total_requests"] += 1
    clean_id = id.replace(".json", "")
    parts = clean_id.split(":")
    imdb_id = parts[0]
    season = parts[1] if len(parts) > 1 else None
    episode = parts[2] if len(parts) > 2 else None

    if not is_content_supported(imdb_id, season, episode):
        async with state.cache_lock:
            state.metrics["unsupported_rejects"] += 1
        logger.info(f"ID {clean_id} not in VidSrc lists – rejecting instantly.")
        return {"streams": []}

    cache_key = f"{imdb_id}:{season or ''}:{episode or ''}"

    # Check cache first
    async with state.cache_lock:
        cached = state.stream_cache.get(cache_key)
    if cached and (time.time() - cached["timestamp"] < Config.CACHE_TTL_SECONDS):
        # Already resolved – return direct URL instantly
        stream_url, stype, referer = cached["stream_url"], cached["stype"], cached["referer"]
        base = str(req.base_url)
        if stype == "mp4":
            proxied = f"{base}proxy/media?url={quote(stream_url)}&referer={quote(referer)}"
            title = "⚡ Direct MP4"
        elif stype == "mpd":
            proxied = f"{base}proxy/media?url={quote(stream_url)}&referer={quote(referer)}"
            title = "⚡ MPEG-DASH"
        else:
            proxied = f"{base}proxy/m3u8?url={quote(stream_url)}&referer={quote(referer)}"
            title = "⚡ HLS (Proxied)"
        return {"streams": [{"name": "VidSrc Direct", "title": title, "url": proxied}]}

    # Start background resolution (will be deduplicated)
    resolve_task = asyncio.create_task(resolve_stream(imdb_id, season, episode))

    try:
        # Wait up to ADDON_WAIT_TIMEOUT seconds for the stream
        stream_url, stype, referer = await asyncio.wait_for(asyncio.shield(resolve_task), timeout=Config.ADDON_WAIT_TIMEOUT)
    except asyncio.TimeoutError:
        # Not ready yet – fall back to lazy URL
        base = str(req.base_url)
        lazy_url = f"{base}lazy-proxy?imdb={quote(imdb_id)}"
        if season:
            lazy_url += f"&season={season}"
        if episode:
            lazy_url += f"&episode={episode}"
        return {"streams": [{
            "name": "VidSrc Direct",
            "title": "⚡ HLS (Lazy)",
            "url": lazy_url
        }]}

    # Resolution succeeded within the timeout – return the direct proxied URL
    if not stream_url or not referer:
        # All providers failed quickly, still return lazy (player will get 502 if all fail eventually)
        base = str(req.base_url)
        lazy_url = f"{base}lazy-proxy?imdb={quote(imdb_id)}"
        if season:
            lazy_url += f"&season={season}"
        if episode:
            lazy_url += f"&episode={episode}"
        return {"streams": [{
            "name": "VidSrc Direct",
            "title": "⚡ HLS (Lazy)",
            "url": lazy_url
        }]}

    base = str(req.base_url)
    if stype == "mp4":
        proxied = f"{base}proxy/media?url={quote(stream_url)}&referer={quote(referer)}"
        title = "⚡ Direct MP4"
    elif stype == "mpd":
        proxied = f"{base}proxy/media?url={quote(stream_url)}&referer={quote(referer)}"
        title = "⚡ MPEG-DASH"
    else:
        proxied = f"{base}proxy/m3u8?url={quote(stream_url)}&referer={quote(referer)}"
        title = "⚡ HLS (Proxied)"
    return {"streams": [{"name": "VidSrc Direct", "title": title, "url": proxied}]}


if __name__ == "__main__":
    uvicorn.run(app, host=Config.HOST, port=Config.PORT)
