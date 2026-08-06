import asyncio
from contextlib import asynccontextmanager
import json
import os
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from playwright.async_api import Browser, BrowserContext, Playwright, async_playwright

# --- Configuration & Globals ---
PORT = int(os.environ.get("PORT", 8085))
HOST = os.environ.get("HOST", "0.0.0.0")

ORIGIN_SITE = "https://timstreams.st"
API_ENDPOINTS = [
    "https://api.timstreams.st/api/channels",
    f"{ORIGIN_SITE}/api/channels",
    f"{ORIGIN_SITE}/api/live",
    "https://api.timstreams.st/channels",
    f"{ORIGIN_SITE}/channels.json",
]

CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
CHROME_HEADERS = {
    "User-Agent": CHROME_UA,
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-CH-UA": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
}

CACHE_REFRESH_INTERVAL = 900  # 15 minutes
HLS_CACHE_TTL = 300           # 5 minutes
client: Optional[httpx.AsyncClient] = None

# --- Persistent Browser Session Manager ---
class PersistentBrowserManager:
    """Manages a Chromium instance with auto-recovery and keep-alive."""
    def __init__(self):
        self.pw: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self._lock = asyncio.Lock()

    async def start(self):
        try:
            self.pw = await async_playwright().start()
            self.browser = await self.pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--no-first-run",
                    "--no-zygote",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            self.context = await self.browser.new_context(
                user_agent=CHROME_UA,
                extra_http_headers={
                    "Sec-CH-UA": CHROME_HEADERS["Sec-CH-UA"],
                    "Sec-CH-UA-Mobile": CHROME_HEADERS["Sec-CH-UA-Mobile"],
                    "Sec-CH-UA-Platform": CHROME_HEADERS["Sec-CH-UA-Platform"],
                }
            )
            await self.context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            print("[+] Persistent Chromium session initialized.")

            # --- Immediate warm‑up: load a blank page to fully activate the context ---
            try:
                page = await self.context.new_page()
                await page.goto("about:blank", wait_until="commit", timeout=3000)
                await page.close()
                print("[+] Browser warm‑up completed.")
            except Exception as e:
                print(f"[-] Warm‑up failed (non‑critical): {e}")

        except Exception as e:
            print(f"[-] Browser start failed: {e}")

    async def _keepalive(self):
        """Lightweight navigation every 60s to keep browser WebSocket alive."""
        while True:
            await asyncio.sleep(60)
            try:
                if self.context:
                    async with self._lock:
                        page = await self.context.new_page()
                        await page.goto("about:blank", wait_until="commit", timeout=2000)
                        await page.close()
            except Exception:
                pass

    async def ensure_browser(self):
        """Auto-recovers browser connection if dropped."""
        async with self._lock:
            if not self.browser or not self.browser.is_connected():
                print("[!] Browser connection lost. Restarting Playwright...")
                await self.close()
                await self.start()

    async def close(self):
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.pw:
                await self.pw.stop()
        except Exception:
            pass
        finally:
            self.context = None
            self.browser = None
            self.pw = None
            print("[+] Persistent Chromium session closed.")

    async def sniff_channel(self, embed_url: str, retry_on_failure: bool = True) -> Tuple[Optional[str], dict]:
        """
        Sniffs for an HLS URL on the embed page. If retry_on_failure is True and no URL is found,
        it will attempt one more time after a short delay (useful when the browser is cold).
        """
        for attempt in range(2 if retry_on_failure else 1):
            if attempt > 0:
                print(f"[!] Retrying sniff for {embed_url} (attempt {attempt+1})...")
                await asyncio.sleep(1.5)

            await self.ensure_browser()
            if not self.context:
                return None, {}

            page = None
            extracted_data = {"url": None, "headers": {}}
            url_found_event = asyncio.Event()

            try:
                page = await self.context.new_page()

                await page.route(
                    "**/*",
                    lambda route: (
                        route.abort()
                        if route.request.resource_type in ["image", "font", "stylesheet", "media"]
                        else route.continue_()
                    ),
                )

                async def handle_request(request):
                    url = request.url
                    if (".m3u8" in url or "/hls/" in url or "/stream/" in url) and not extracted_data["url"] and ".js" not in url:
                        extracted_data["url"] = request.url
                        extracted_data["headers"] = dict(request.headers)
                        url_found_event.set()

                page.on("request", handle_request)

                await page.goto(embed_url, wait_until="commit", timeout=4000)
                try:
                    await asyncio.wait_for(url_found_event.wait(), timeout=2.5)
                except asyncio.TimeoutError:
                    pass

                if self.context:
                    cookies = await self.context.cookies(embed_url)
                    if cookies:
                        extracted_data["headers"]["cookie"] = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            except Exception as e:
                print(f"[-] Sniffer error on {embed_url} (attempt {attempt+1}): {e}")
            finally:
                if page:
                    try:
                        await page.close()
                    except Exception:
                        pass

            if extracted_data["url"]:
                return extracted_data["url"], extracted_data["headers"]

        return None, {}


# --- Channel State & Session Objects ---
class ChannelSession:
    def __init__(self, channel_id: str):
        self.channel_id = channel_id
        self.hls_url: Optional[str] = None
        self.embed_url: Optional[str] = None
        self.headers: dict = {}
        self.lock = asyncio.Lock()
        self.last_updated: float = 0


browser_manager = PersistentBrowserManager()

CHANNEL_CACHE: List[dict] = []
CHANNEL_MAP: Dict[str, dict] = {}
CHANNEL_SESSIONS: Dict[str, ChannelSession] = {}

CACHE_LOCK = asyncio.Lock()
INITIAL_LOAD_EVENT = asyncio.Event()


def sanitize_stream_url(url: str) -> str:
    if not url:
        return ""
    url = str(url).replace("\\/", "/")
    url = url.replace("¡", "a").replace(" ", "").strip()
    return re.sub(r"[^\x21-\x7E]", "", url)


async def is_hls_valid(url: str, headers: dict) -> bool:
    if not url or not client:
        return False

    clean_headers = {k: v for k, v in headers.items() if not k.startswith(":")}
    clean_headers.setdefault("User-Agent", CHROME_UA)

    try:
        async with client.stream("GET", url, headers=clean_headers, timeout=3.0) as resp:
            if resp.status_code != 200:
                return False
            chunk = await resp.aread(512)
            return chunk.startswith(b"#EXTM3U")
    except Exception:
        return False


async def fetch_api_channels() -> tuple[List[dict], Dict[str, dict]]:
    channels = []
    ch_map = {}
    headers = {
        **CHROME_HEADERS,
        "Origin": ORIGIN_SITE,
        "Referer": f"{ORIGIN_SITE}/live-tv",
        "Accept": "application/json, text/plain, */*",
    }

    res = None
    success_endpoint = None

    for ep in API_ENDPOINTS:
        try:
            res = await client.get(ep, headers=headers, timeout=3.0)
            if res.status_code == 200:
                text_preview = res.text.strip()
                if text_preview.startswith("<"):
                    continue
                success_endpoint = ep
                break
        except Exception:
            pass

    if not success_endpoint or not res:
        return channels, ch_map

    try:
        data = res.json()
        raw_channels = data.get("channels", []) if isinstance(data, dict) else data

        if not raw_channels and isinstance(data, list):
            raw_channels = data

        for ch in raw_channels:
            ch_id = (
                ch.get("id")
                or ch.get("channel_id")
                or ch.get("slug")
                or ch.get("key")
                or ch.get("url")
            )
            ch_name = ch.get("name") or ch.get("title") or ch.get("channel_name")
            if ch_id and ch_name:
                ch_obj = {
                    "id": str(ch_id),
                    "name": str(ch_name).strip(),
                    "logo": ch.get("logo") or ch.get("img") or ch.get("icon") or ch.get("poster") or "",
                    "genre": ch.get("genre") or ch.get("category") or ch.get("group") or "",
                    "raw": ch,
                }
                channels.append(ch_obj)
                ch_map[str(ch_id)] = ch_obj
                ch_map[str(ch_id).lower()] = ch_obj

        print(f"[+] Loaded {len(channels)} channels from {success_endpoint}")
    except Exception as e:
        print(f"[-] Failed parsing channel JSON payload: {e}")

    return channels, ch_map


async def background_cache_worker():
    global CHANNEL_CACHE, CHANNEL_MAP
    while True:
        try:
            new_channels, new_map = await fetch_api_channels()
            if new_channels:
                async with CACHE_LOCK:
                    CHANNEL_CACHE = new_channels
                    CHANNEL_MAP = new_map
                print(f"[+] Channel cache synced ({len(new_channels)} channels).")

            INITIAL_LOAD_EVENT.set()
        except Exception as e:
            print(f"[-] Background worker error: {e}")
        
        await asyncio.sleep(CACHE_REFRESH_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    timeout_config = httpx.Timeout(10.0, connect=3.0)
    limits_config = httpx.Limits(
        max_keepalive_connections=30,
        max_connections=120,
        keepalive_expiry=30.0
    )
    
    client = httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout_config,
        limits=limits_config,
        verify=False,
    )

    await browser_manager.start()
    keepalive_task = asyncio.create_task(browser_manager._keepalive())
    worker_task = asyncio.create_task(background_cache_worker())
    print("[+] FastAPI FFmpeg Session Proxy initialized.")
    yield
    worker_task.cancel()
    keepalive_task.cancel()
    await browser_manager.close()
    if client:
        await client.aclose()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def try_http_scrape(embed_url: str) -> Optional[str]:
    try:
        headers = {**CHROME_HEADERS, "Referer": ORIGIN_SITE}
        res = await client.get(embed_url, headers=headers, timeout=3.0)
        if res.status_code == 200:
            matches = re.findall(r'https?://[^\s<>"\']+\.m3u8[^\s<>"\']*', res.text)
            for m in matches:
                clean = sanitize_stream_url(m)
                if clean and "icelanders.st" not in clean:
                    return clean
    except Exception:
        pass
    return None


async def resolve_channel_session(channel_id: str, force_refresh: bool = False) -> Tuple[Optional[str], dict]:
    """
    Resolves an HLS stream for the given channel. 
    Implements a two‑pass strategy: on first failure it forces a browser restart
    and retries the Playwright sniffing step once, which handles cold‑start scenarios.
    """
    clean_id = str(channel_id).strip()

    if clean_id not in CHANNEL_SESSIONS:
        CHANNEL_SESSIONS[clean_id] = ChannelSession(clean_id)

    session = CHANNEL_SESSIONS[clean_id]

    async with session.lock:
        # --- Cached valid URL check (with TTL) ---
        if not force_refresh and session.hls_url and (time.time() - session.last_updated) < HLS_CACHE_TTL:
            if await is_hls_valid(session.hls_url, session.headers):
                return session.hls_url, session.headers

        # --- Determine candidate embed URLs ---
        target_stream = None
        ch_info = CHANNEL_MAP.get(clean_id) or CHANNEL_MAP.get(clean_id.lower())
        if ch_info and "raw" in ch_info:
            raw = ch_info["raw"]
            for field in ["stream", "url", "stream_url", "link", "file", "embed", "embedUrl", "hls", "playlist"]:
                val = raw.get(field)
                if isinstance(val, str) and val.strip():
                    if any(val.lower().endswith(ext) for ext in [".jpg", ".png", ".jpeg", ".webp"]):
                        continue
                    if val.startswith("http"):
                        target_stream = val
                        break
                    elif val.startswith("/"):
                        target_stream = urljoin(ORIGIN_SITE, val)
                        break

        candidates = []
        if target_stream:
            candidates.append(target_stream)
        candidates.extend([
            f"{ORIGIN_SITE}/embed/{clean_id}",
            f"{ORIGIN_SITE}/watch/{clean_id}",
        ])

        # --------------- FIRST PASS ---------------
        print(f"[*] First pass for {clean_id}: {len(candidates)} candidates")
        for candidate in candidates:
            candidate = sanitize_stream_url(candidate)
            if not candidate or "icelanders.st" in candidate:
                continue

            # Direct .m3u8 URL
            if ".m3u8" in candidate.lower():
                headers = {**CHROME_HEADERS, "Referer": ORIGIN_SITE, "Origin": f"https://{urlparse(candidate).netloc}"}
                if await is_hls_valid(candidate, headers):
                    print(f"[+] Found valid direct HLS for {clean_id}")
                    session.hls_url = candidate
                    session.headers = headers
                    session.last_updated = time.time()
                    return candidate, headers

            # HTTP scrape
            try:
                scraped_url = await asyncio.wait_for(try_http_scrape(candidate), timeout=3.0)
            except asyncio.TimeoutError:
                scraped_url = None

            if scraped_url:
                headers = {**CHROME_HEADERS, "Referer": candidate, "Origin": f"https://{urlparse(scraped_url).netloc}"}
                if await is_hls_valid(scraped_url, headers):
                    print(f"[+] Found valid scraped HLS for {clean_id}")
                    session.hls_url = scraped_url
                    session.headers = headers
                    session.last_updated = time.time()
                    return scraped_url, headers

            # Playwright sniff (first try)
            try:
                sniffed_url, sniff_headers = await asyncio.wait_for(
                    browser_manager.sniff_channel(candidate, retry_on_failure=False),  # no internal retry here
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                sniffed_url = None

            if sniffed_url:
                sniffed_url = sanitize_stream_url(sniffed_url)
                session.hls_url = sniffed_url
                session.headers = sniff_headers
                session.last_updated = time.time()
                return sniffed_url, sniff_headers

        # --------------- SECOND PASS (forced browser restart + retry) ---------------
        print(f"[!] First pass failed for {clean_id}. Attempting second pass with browser reset...")
        try:
            await browser_manager.ensure_browser()  # will restart if needed
            # Retry only the Playwright sniff on the first candidate
            for candidate in candidates[:1]:  # only the first one to save time
                candidate = sanitize_stream_url(candidate)
                if not candidate or "icelanders.st" in candidate:
                    continue
                try:
                    sniffed_url, sniff_headers = await asyncio.wait_for(
                        browser_manager.sniff_channel(candidate, retry_on_failure=True),
                        timeout=7.0   # a little more time
                    )
                except asyncio.TimeoutError:
                    continue
                if sniffed_url:
                    sniffed_url = sanitize_stream_url(sniffed_url)
                    session.hls_url = sniffed_url
                    session.headers = sniff_headers
                    session.last_updated = time.time()
                    return sniffed_url, sniff_headers
        except Exception as e:
            print(f"[-] Second pass error: {e}")

    print(f"[-] Could not resolve active stream for channel: {clean_id}")
    return None, {}


@app.get("/playlist.m3u")
async def get_playlist(req: Request):
    try:
        await asyncio.wait_for(INITIAL_LOAD_EVENT.wait(), timeout=5)
    except asyncio.TimeoutError:
        pass

    base_proxy_url = str(req.base_url).rstrip("/") + "/"
    m3u_lines = ["#EXTM3U\n"]
    async with CACHE_LOCK:
        for ch in CHANNEL_CACHE:
            ch_id = ch["id"]
            ch_name = ch["name"]
            ch_logo = ch.get("logo", "")
            ch_genre = ch.get("genre", "")
            m3u_lines.append(
                f'#EXTINF:-1 tvg-id="{ch_id}" tvg-name="{ch_name}" tvg-logo="{ch_logo}" group-title="{ch_genre}", {ch_name}\n'
                f"{base_proxy_url}play?id={ch_id}\n"
            )

    return Response(
        content="".join(m3u_lines),
        media_type="audio/x-mpegurl",
        headers={"Access-Control-Allow-Origin": "*"},
    )


async def handle_stream_request(channel_id: str):
    clean_id = str(channel_id).strip()
    
    # Outer timeout: 15s (gives room for the second pass)
    try:
        stream_url, headers = await asyncio.wait_for(
            resolve_channel_session(clean_id), timeout=15.0
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail=f"Timeout resolving active stream for {clean_id}")

    if not stream_url:
        raise HTTPException(status_code=504, detail=f"Could not resolve active stream for {clean_id}")

    async def stream_generator():
        nonlocal stream_url, headers
        reconnect_attempts = 0
        max_reconnects = 3

        while reconnect_attempts < max_reconnects:
            ffmpeg_headers = [f"{k}: {v}" for k, v in headers.items() if not k.startswith(":")]
            headers_str = "\r\n".join(ffmpeg_headers) + "\r\n"

            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "warning",
                "-analyzeduration", "500000",
                "-probesize", "500000",
                "-fpsprobesize", "0",
                "-reconnect", "1",
                "-reconnect_streamed", "1",
                "-reconnect_delay_max", "2",
                "-rw_timeout", "4000000",
                "-user_agent", headers.get("user-agent", CHROME_UA),
                "-headers", headers_str,
                "-i", stream_url,
                "-map", "0:v:0?",
                "-map", "0:a:0?",
                "-c:v", "copy",
                "-c:a", "aac",
                "-ac", "2",
                "-b:a", "128k",
                "-ar", "48000",
                "-fflags", "+genpts+discardcorrupt+nobuffer",
                "-flush_packets", "1",
                "-avoid_negative_ts", "make_zero",
                "-muxdelay", "0",
                "-f", "mpegts",
                "pipe:1"
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            try:
                bytes_sent = 0
                chunk_size = 32768

                while True:
                    chunk = await process.stdout.read(chunk_size)
                    if not chunk:
                        break
                    bytes_sent += len(chunk)
                    yield chunk

                if bytes_sent < 131072:
                    reconnect_attempts += 1
                    print(f"[!] Stream died early for {clean_id}. Re-resolving HLS (Attempt {reconnect_attempts}/{max_reconnects})...")
                    try:
                        stream_url, headers = await asyncio.wait_for(
                            resolve_channel_session(clean_id, force_refresh=True), timeout=10.0
                        )
                    except asyncio.TimeoutError:
                        break
                    if not stream_url:
                        break
                else:
                    break

            except GeneratorExit:
                break
            finally:
                try:
                    process.terminate()
                    await process.wait()
                except Exception:
                    pass

    return StreamingResponse(
        stream_generator(),
        media_type="video/mp2t",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        },
    )


async def handle_head_request(channel_id: str):
    clean_id = str(channel_id).strip()
    try:
        stream_url, _ = await asyncio.wait_for(resolve_channel_session(clean_id), timeout=5.0)
    except asyncio.TimeoutError:
        stream_url = None

    if not stream_url:
        raise HTTPException(status_code=404, detail="Stream not found")
    return Response(status_code=200, headers={"Access-Control-Allow-Origin": "*"})

@app.head("/play/{channel_id}.m3u8")
async def head_channel_path(channel_id: str):
    return await handle_head_request(channel_id)

@app.head("/play")
async def head_channel_query(id: str):
    return await handle_head_request(id)

@app.head("/stream/{channel_id}")
async def head_stream_path(channel_id: str):
    return await handle_head_request(channel_id)

@app.head("/stream")
async def head_stream_query(id: str):
    return await handle_head_request(id)

@app.get("/play/{channel_id}.m3u8")
async def play_channel_path(channel_id: str):
    return await handle_stream_request(channel_id)

@app.get("/play")
async def play_channel_query(id: str):
    return await handle_stream_request(id)

@app.get("/stream/{channel_id}")
async def stream_channel_path(channel_id: str):
    return await handle_stream_request(channel_id)

@app.get("/stream")
async def stream_channel_query(id: str):
    return await handle_stream_request(id)

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
