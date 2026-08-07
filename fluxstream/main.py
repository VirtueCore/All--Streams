import http.server
import socketserver
import json
import urllib.parse
import re
import time
import threading
import requests
from requests.adapters import HTTPAdapter

# Configuration
PORT = 8085
API_BASE = "https://timstreams.st/api"
ORIGIN_SITE = "https://timstreams.st"

CACHE_REFRESH_INTERVAL = 900  # Refresh channel list every 15 minutes
STREAM_TTL = 300              # Cache stream URLs for 5 minutes

# Global cache & thread locks
CHANNEL_CACHE = []
CHANNEL_MAP = {}   # {channel_id: channel_dict}
STREAM_CACHE = {}  # {channel_id: (stream_url, embed_page, timestamp)}
CACHE_LOCK = threading.Lock()

DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0"

# Session with expanded connection pooling
session = requests.Session()
adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=2)
session.mount("http://", adapter)
session.mount("https://", adapter)

session.headers.update({
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Origin": ORIGIN_SITE,
    "Referer": f"{ORIGIN_SITE}/"
})


def fetch_api_channels():
    """Fetches structured channel list from the real API endpoint."""
    channels = []
    ch_map = {}
    try:
        url = f"{API_BASE}/channels"
        res = session.get(url, timeout=10)
        
        if res.status_code == 200:
            data = res.json()
            raw_channels = data.get("channels", []) if isinstance(data, dict) else data
            genres = data.get("genres", [])  # list of {"id": int, "name": str}

            # Build genre lookup
            genre_map = {g["id"]: g["name"] for g in genres}

            for ch in raw_channels:
                # "url" is actually the channel slug/id
                ch_id = ch.get("url") or ch.get("id") or ch.get("slug")
                ch_name = ch.get("name") or ch.get("title")

                if not ch_id or not ch_name:
                    continue

                # The first stream entry is usually the default embed link
                embed_url = None
                streams = ch.get("streams", [])
                if isinstance(streams, list) and streams:
                    first = streams[0]
                    if isinstance(first, dict):
                        embed_url = first.get("url")  # e.g., https://hux-giants.shop/embed/abc-usa

                # Map numeric genre id to its name
                genre_id = ch.get("genre")
                genre_name = genre_map.get(genre_id, "General") if isinstance(genre_id, int) else str(genre_id)

                ch_obj = {
                    "id": str(ch_id),
                    "name": str(ch_name).strip(),
                    "logo": ch.get("logo") or "",
                    "genre": genre_name,
                    "embed_url": embed_url,
                    "raw": ch
                }
                channels.append(ch_obj)
                ch_map[str(ch_id)] = ch_obj
                ch_map[str(ch_id).lower()] = ch_obj

            print(f"[API] Successfully loaded {len(channels)} live channels.", flush=True)
        else:
            print(f"[ERROR] Backend API returned HTTP {res.status_code}", flush=True)

    except Exception as e:
        print(f"[ERROR] Failed to communicate with backend API: {e}", flush=True)

    return channels, ch_map


def background_cache_worker():
    """Background thread to update channel metadata cleanly."""
    global CHANNEL_CACHE, CHANNEL_MAP
    while True:
        new_channels, new_map = fetch_api_channels()
        if new_channels:
            with CACHE_LOCK:
                CHANNEL_CACHE = new_channels
                CHANNEL_MAP = new_map
        time.sleep(CACHE_REFRESH_INTERVAL)


def resolve_stream_url(channel_id, bypass_cache=False):
    """
    Resolves active stream links by scraping embed pages, decrypting JS, and parsing payloads.
    Returns a tuple (stream_url, embed_page_url).
    """
    now = time.time()
    clean_query_id = str(channel_id).strip()

    # 1. Check Stream TTL Cache
    if not bypass_cache:
        with CACHE_LOCK:
            if clean_query_id in STREAM_CACHE:
                cached_url, embed_page, ts = STREAM_CACHE[clean_query_id]
                if now - ts < STREAM_TTL:
                    return cached_url, embed_page

    embed_page = None      # the embed page URL
    extracted_stream = None  # the final .m3u8 URL found on that page
    ch_info = None

    # 2. Get embed URL from cache
    with CACHE_LOCK:
        if clean_query_id in CHANNEL_MAP:
            ch_info = CHANNEL_MAP[clean_query_id]
        elif clean_query_id.lower() in CHANNEL_MAP:
            ch_info = CHANNEL_MAP[clean_query_id.lower()]
        else:
            for k, val in CHANNEL_MAP.items():
                if clean_query_id.lower() in k.lower():
                    ch_info = val
                    break

        if ch_info and ch_info.get("embed_url"):
            embed_page = ch_info["embed_url"]

    # 3. Fallback API lookup
    lookup_id = ch_info.get("id") if ch_info else clean_query_id
    if not embed_page:
        for endpoint_url in [f"{API_BASE}/watch/{lookup_id}", f"{API_BASE}/stream/{lookup_id}", f"{API_BASE}/channel/{lookup_id}"]:
            try:
                watch_res = session.get(endpoint_url, timeout=3)
                if watch_res.status_code == 200:
                    watch_data = watch_res.json()
                    if isinstance(watch_data, dict):
                        for k in ["url", "stream", "stream_url", "link", "file"]:
                            if watch_data.get(k):
                                embed_page = watch_data.get(k)
                                break
                    if embed_page:
                        break
            except Exception:
                pass

    if not embed_page:
        print(f"[DEBUG] resolve_stream_url: no embed page for {clean_query_id}", flush=True)
        return None, None

    embed_page = str(embed_page).replace("\\/", "/")

    # 4. If the embed_page is itself a .m3u8, return it directly
    if ".m3u8" in embed_page:
        with CACHE_LOCK:
            STREAM_CACHE[clean_query_id] = (embed_page, embed_page, now)
        return embed_page, embed_page

    # 5. Fetch the embed page to extract the real stream URL
    try:
        parsed_embed = urllib.parse.urlparse(embed_page)
        base_embed_origin = f"{parsed_embed.scheme}://{parsed_embed.netloc}"
        
        embed_headers = session.headers.copy()
        embed_headers["Referer"] = f"{base_embed_origin}/"
        embed_res = session.get(embed_page, headers=embed_headers, timeout=4)

        if embed_res.status_code == 200:
            html = embed_res.text
            
            # Fast check for direct .m3u8 in HTML source
            all_urls = re.findall(r'["\'](https?://[^"\']+\.m3u8[^"\']*)["\']', html)
            if all_urls:
                extracted_stream = all_urls[0].replace("\\/", "/")
                with CACHE_LOCK:
                    STREAM_CACHE[clean_query_id] = (extracted_stream, embed_page, now)
                return extracted_stream, embed_page

            # Optimized JS Deobfuscation Logic
            scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE)
            for script_content in scripts:
                if 'String.fromCharCode' in script_content:
                    arr_m = re.search(r'=\s*\[([0-9,\s]+)\]', script_content)
                    if not arr_m:
                        continue
                        
                    arr_vals = [int(x.strip()) for x in arr_m.group(1).split(',') if x.strip()]
                    if not arr_vals:
                        continue

                    decoded_js = ""
                    found_keys = False

                    algorithms = [
                        lambda v, k1, k2: ((v ^ k1) - k2),
                        lambda v, k1, k2: ((v - k1) ^ k2),
                        lambda v, k1, k2: (v ^ k1),
                        lambda v, k1, k2: (v - k1),
                        lambda v, k1, k2: ((v + k1) ^ k2)
                    ]

                    uk3_m = re.search(r'_uk3\s*=\s*(\d+)', script_content)
                    gc6_m = re.search(r'_gc6\s*=\s*(\d+)', script_content)
                    p1 = int(uk3_m.group(1)) if uk3_m else 80
                    p2 = int(gc6_m.group(1)) if gc6_m else 121

                    # Check hint parameters first
                    for alg in algorithms:
                        try:
                            candidate = "".join([chr(alg(val, p1, p2) % 256) for val in arr_vals])
                            if any(kw in candidate.lower() for kw in ['http://', 'https://', '.m3u8', 'playlist']):
                                decoded_js = candidate
                                found_keys = True
                                break
                        except Exception:
                            pass

                    # Fast-path brute-force using array sampling
                    if not found_keys:
                        sample_vals = arr_vals[:30]
                        for k1 in range(256):
                            for k2 in range(256):
                                for alg in algorithms:
                                    try:
                                        sample_str = "".join([chr(alg(v, k1, k2) % 256) for v in sample_vals])
                                        if any(kw in sample_str.lower() for kw in ['http', 'm3u8', 'play', 'var ', 'file']):
                                            full_candidate = "".join([chr(alg(v, k1, k2) % 256) for v in arr_vals])
                                            if any(kw in full_candidate.lower() for kw in ['http://', 'https://', '.m3u8', 'playlist']):
                                                decoded_js = full_candidate
                                                found_keys = True
                                                break
                                    except Exception:
                                        continue
                                if found_keys:
                                    break
                            if found_keys:
                                break

                    if not decoded_js:
                        decoded_js = "".join([chr(val % 256) for val in arr_vals])

                    # Extract stream URL candidate from decoded JS
                    for m in re.finditer(r'https?://', decoded_js):
                        sub = decoded_js[m.start():m.start()+500]
                        raw_url_candidate = ""
                        for char in sub:
                            if char in ('"', "'", '<', '>', '\\', '^', '`', '{', '}'):
                                break
                            if ord(char) >= 32:
                                raw_url_candidate += char
                        
                        clean_url = raw_url_candidate.replace(" ", "")
                        if '.m3u8' in clean_url.lower() or 'playlist' in clean_url.lower():
                            with CACHE_LOCK:
                                STREAM_CACHE[clean_query_id] = (clean_url, embed_page, now)
                            return clean_url, embed_page

    except Exception as e:
        print(f"[ERROR] Embed extraction failed: {e}", flush=True)

    return None, None


def fetch_media_manifest(stream_url, depth=0, referer=None):
    """
    Recursively unwraps Master Playlists (#EXT-X-STREAM-INF) down to actual Media Playlists.
    """
    if depth > 5:
        return None, None
    try:
        parsed = urllib.parse.urlparse(stream_url)
        base_origin = f"{parsed.scheme}://{parsed.netloc}"
        path_dir = parsed.path.rsplit("/", 1)[0]

        # Use the provided referer (embed page) or fallback to stream domain
        stream_headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            "Referer": referer if referer else f"{base_origin}/",
            "Origin": base_origin
        }

        print(f"[DEBUG] Fetching manifest: {stream_url} (depth {depth})", flush=True)
        res = session.get(stream_url, headers=stream_headers, timeout=5)
        print(f"[DEBUG] Manifest response: HTTP {res.status_code}", flush=True)

        if res.status_code != 200:
            print(f"[DEBUG] Response body (first 200 chars): {res.text[:200]}", flush=True)
            return None, None

        text = res.text

        if "#EXT-X-STREAM-INF" in text:
            lines = text.splitlines()
            for i, line in enumerate(lines):
                if "#EXT-X-STREAM-INF" in line and i + 1 < len(lines):
                    variant = lines[i + 1].strip()
                    if variant and not variant.startswith("#"):
                        if variant.startswith("http://") or variant.startswith("https://"):
                            sub_url = variant
                        elif variant.startswith("/"):
                            sub_url = f"{base_origin}{variant}"
                        else:
                            sub_url = f"{base_origin}{path_dir}/{variant}"
                        return fetch_media_manifest(sub_url, depth + 1, referer=referer)

        return text, stream_url
    except Exception as e:
        print(f"[ERROR] Failed fetching media manifest ({stream_url}): {e}", flush=True)
        return None, None


class ProxyRequestHandler(http.server.BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        """Handle CORS preflight requests for EngPlayer and web clients."""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed_path.query)
        host = self.headers.get("Host", f"localhost:{PORT}")
        print(f"[REQUEST] {self.command} {parsed_path.path}?{parsed_path.query}", flush=True)

        if parsed_path.path == "/playlist.m3u":
            self.send_response(200)
            self.send_header("Content-Type", "audio/x-mpegurl")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            m3u_lines = ["#EXTM3U\n"]
            with CACHE_LOCK:
                for ch in CHANNEL_CACHE:
                    ch_id = ch["id"]
                    ch_name = ch["name"]
                    ch_logo = ch.get("logo", "")
                    ch_group = ch.get("genre", "General")
                    
                    play_url = f"http://{host}/play?id={ch_id}"
                    
                    m3u_lines.append(
                        f'#EXTINF:-1 tvg-id="{ch_id}" tvg-name="{ch_name}" tvg-logo="{ch_logo}" group-title="{ch_group}", {ch_name}\n'
                        f'#EXTVLCOPT:http-user-agent={DEFAULT_USER_AGENT}\n'
                        f'#EXTVLCOPT:http-referrer={ORIGIN_SITE}/\n'
                        f'{play_url}\n'
                    )

            self.wfile.write("".join(m3u_lines).encode("utf-8"))

        elif parsed_path.path == "/play":
            channel_id = query.get("id", [None])[0]
            print(f"[INFO] Stream requested for channel: {channel_id}", flush=True)
            if not channel_id:
                self.send_error(400, "Missing channel id parameter")
                return

            stream_url, embed_page = resolve_stream_url(channel_id)
            if not stream_url:
                print(f"[ERROR] resolve_stream_url returned None for {channel_id}", flush=True)
                self.send_error(504, f"Could not resolve live stream for channel: {channel_id}")
                return
            print(f"[INFO] Resolved stream URL: {stream_url} (embed: {embed_page})", flush=True)

            # Use the embed page URL as Referer to keep the token valid
            manifest_text, final_stream_url = fetch_media_manifest(stream_url, referer=embed_page)

            if not manifest_text:
                stream_url, embed_page = resolve_stream_url(channel_id, bypass_cache=True)
                if stream_url:
                    manifest_text, final_stream_url = fetch_media_manifest(stream_url, referer=embed_page)

            if not manifest_text or not final_stream_url:
                self.send_error(504, f"Could not fetch valid stream manifest for channel: {channel_id}")
                return

            parsed_upstream_url = urllib.parse.urlparse(final_stream_url)
            upstream_base = f"{parsed_upstream_url.scheme}://{parsed_upstream_url.netloc}"
            path_dir = parsed_upstream_url.path.rsplit("/", 1)[0]

            manifest_lines = []
            for line in manifest_text.splitlines():
                stripped = line.strip()

                # Rewrite AES Key URLs if present
                if "#EXT-X-KEY" in line and 'URI="' in line:
                    def replace_key_uri(match):
                        raw_key = match.group(1)
                        if not (raw_key.startswith("http://") or raw_key.startswith("https://")):
                            raw_key = f"{upstream_base}{raw_key}" if raw_key.startswith("/") else f"{upstream_base}{path_dir}/{raw_key}"
                        encoded_key = urllib.parse.quote(raw_key, safe="")
                        return f'URI="http://{host}/proxy_seg?url={encoded_key}"'

                    line = re.sub(r'URI=["\']([^"\']+)["\']', replace_key_uri, line)

                if stripped and not stripped.startswith("#"):
                    if stripped.startswith("http://") or stripped.startswith("https://"):
                        seg_url = stripped
                    elif stripped.startswith("/"):
                        seg_url = f"{upstream_base}{stripped}"
                    else:
                        seg_url = f"{upstream_base}{path_dir}/{stripped}"

                    encoded_seg_url = urllib.parse.quote(seg_url, safe="")
                    # Pass the embed page URL to the segment proxy for correct Referer
                    encoded_embed = urllib.parse.quote(embed_page, safe="")
                    manifest_lines.append(f"http://{host}/proxy_seg?url={encoded_seg_url}&embed_url={encoded_embed}")
                else:
                    manifest_lines.append(line)

            rewritten_manifest = "\n".join(manifest_lines) + "\n"

            self.send_response(200)
            self.send_header("Content-Type", "application/x-mpegurl")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(rewritten_manifest.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(rewritten_manifest.encode("utf-8"))

        elif parsed_path.path == "/proxy_seg":
            seg_url = query.get("url", [None])[0]
            embed_url = query.get("embed_url", [None])[0]  # embed page to use as Referer

            if not seg_url:
                self.send_error(400, "Missing segment url")
                return

            try:
                parsed_seg = urllib.parse.urlparse(seg_url)
                seg_base = f"{parsed_seg.scheme}://{parsed_seg.netloc}"

                # Use the embed page as Referer/Origin to keep the token valid
                if embed_url:
                    parsed_embed = urllib.parse.urlparse(embed_url)
                    referer = embed_url                     # full embed page URL
                    origin = f"{parsed_embed.scheme}://{parsed_embed.netloc}"
                else:
                    referer = f"{seg_base}/"
                    origin = seg_base

                seg_headers = {
                    "User-Agent": DEFAULT_USER_AGENT,
                    "Referer": referer,
                    "Origin": origin
                }
                print(f"[DEBUG] Fetching segment: {seg_url} (Referer: {referer})", flush=True)
                seg_res = session.get(seg_url, headers=seg_headers, stream=True, timeout=8)
                
                self.send_response(seg_res.status_code)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Type", "video/mp2t")
                
                for header, value in seg_res.headers.items():
                    if header.lower() in ['transfer-encoding', 'connection', 'content-encoding', 'content-length', 'content-type']:
                        continue
                    self.send_header(header, value)
                self.end_headers()

                for chunk in seg_res.iter_content(chunk_size=65536):
                    if chunk:
                        try:
                            self.wfile.write(chunk)
                        except (BrokenPipeError, ConnectionResetError):
                            break
            except Exception as e:
                print(f"[ERROR] Failed segment download: {e}", flush=True)

        else:
            self.send_error(404, "Endpoint not found")

    def log_message(self, format, *args):
        if args:
            print(f"[{self.log_date_time_string()}] {format % args}", flush=True)
        else:
            print(f"[{self.log_date_time_string()}] {format}", flush=True)


def run_server():
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    cache_thread = threading.Thread(target=background_cache_worker, daemon=True)
    cache_thread.start()

    with socketserver.ThreadingTCPServer(("", PORT), ProxyRequestHandler) as httpd:
        print(f"[SERVER] Optimized Proxy running on port {PORT}...")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n[SERVER] Stopping proxy service...")


if __name__ == "__main__":
    run_server()
