# All-Streams

![Python](https://img.shields.io/badge/Python-3.11+-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?logo=fastapi)
![Playwright](https://img.shields.io/badge/Playwright-Chromium-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

> **A self-hosted streaming platform combining a custom IPTV proxy, automatic Live TV playlist generation, and a high-performance Stremio addon for movies and TV shows.**

All-Streams provides a unified Docker-based solution for streaming enthusiasts who want to self-host both Live TV and on-demand content.

It combines three independent services into a single deployment:

* 📺 **FluxStream** – Live TV proxy that automatically discovers streams, generates an M3U playlist, and relays HLS streams through FFmpeg.
* 🎬 **FluxResolver** – High-speed Stremio addon capable of resolving streams from multiple providers with automatic token refresh.
* ☁️ **Cloudflare WARP** – Secure outbound networking for the Live TV proxy.

---

# Features

## Live TV

* Automatic channel discovery
* Dynamic M3U playlist generation
* HLS stream validation
* Persistent Chromium browser
* Playwright-powered stream discovery
* Automatic stream recovery
* FFmpeg stream relay
* Channel caching
* Automatic cache refresh
* Multiple endpoint fallback
* HTTP stream scraping
* Browser packet sniffing
* HLS health checking

---

## Movies & TV Shows

* Stremio addon
* Parallel provider resolution
* Multiple VidSrc mirrors
* Automatic provider failover
* Token auto-refresh
* Playlist rewriting
* Segment proxying
* MP4 support
* MPEG-DASH support
* HLS proxying
* Automatic provider ranking
* ID validation
* Intelligent caching

---

## Docker

* Fully containerized
* Docker Compose deployment
* Persistent storage
* Restart policies
* Shared networking
* Cloudflare WARP integration
* Minimal host requirements

---

# Architecture

```text
                           +----------------------+
                           |     Stremio App      |
                           +----------+-----------+
                                      |
                                      |
                                      ▼
                        +---------------------------+
                        |       FluxResolver        |
                        |        Port 7000          |
                        +------------+--------------+
                                     |
               Parallel Resolution across Providers
                                     |
      +------------+------------+------------+------------+
      |            |            |            |            |
      ▼            ▼            ▼            ▼            ▼
   Mirror 1     Mirror 2     Mirror 3     Mirror 5     Mirror 5
                                     |
                                     ▼
                            Playlist Proxy
                            Segment Proxy
                            Token Refresh



                        IPTV Players
                (VLC, TiviMate, Kodi, etc.)
                           |
                           ▼
                +-------------------------+
                |       FluxStream        |
                |        Port 8085        |
                +------------+------------+
                             |
                   Channel Discovery
                   Browser Sniffing
                   HTTP Scraping
                   HLS Validation
                             |
                             ▼
                      FFmpeg Stream Relay
                             |
                             ▼
                      Generated M3U Playlist



                   +----------------------+
                   | Cloudflare WARP VPN  |
                   +----------------------+
```

---

# Repository Layout

```text
All--Streams/
│
├── docker-compose.yml
├── LICENSE
├── README.md
│
├── fluxstream/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
│
└── fluxresolver/
    ├── Dockerfile
    ├── main.py
    └── requirements.txt
```

---
# Components

| Service         | Container          | Port     | Purpose                    |
| --------------- | ------------------ | -------- | -------------------------- |
| Cloudflare WARP | `warp`             | Internal | Secure outbound networking |
| FluxStream      | `fluxStream`       | 8085     | IPTV proxy & M3U generator |
| FluxResolver    | `fluxResolver`     | 7000     | Stremio addon              |

---

# Requirements

* Docker Engine
* Docker Compose
* Internet connection
* Minimum 2 GB RAM (4 GB recommended)
* Modern CPU with virtualization support recommended

---

# Quick Start

Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/All--Streams.git

cd All--Streams
```

Build the containers

```bash
docker compose build
```

Start everything

```bash
docker compose up -d
```

View logs

```bash
docker compose logs -f
```

Stop

```bash
docker compose down
```

Update


## Example Usage:

FluxStream: http://YOUR-IP-ADDRESS:8085/playlist.m3u

Supported players include (others work too):

- VLC
- TiviMate
- Kodi
- IPTV Smarters
- Hypnotix
- Perfect Player

---

## Stremio Addon

Install the addon using:

FluxResolver: http://YOUR-IP-ADDRESS:7000/manifest.json

Once installed, FluxResolver will automatically resolve supported movie and TV streams directly within Stremio.

---

# Networking

The Docker stack is split into two independent networking models:

- **FluxStream** shares the Cloudflare WARP container's network namespace (`network_mode: service:warp`). This routes all outbound IPTV traffic through WARP while exposing only port **8085** on the host.

- **FluxResolver** runs independently and exposes port **7000** directly on the host, allowing Stremio clients to connect without passing through WARP.

This separation improves flexibility while keeping the Live TV proxy isolated from the resolver.

---

# Troubleshooting

## FluxStream won't start

Check the container logs:

```bash
docker compose logs fluxstream
```
## FluxResolver returns no streams

Verify the resolver is running:
```bash
docker compose logs fluxresolver
```
Then confirm the addon is reachable:

http://YOUR-SERVER-IP:7000/health

## Browser fails to launch

Ensure Docker has enough shared memory.

The provided Compose file already configures:

shm_size: "2gb"

## WARP is disconnected

Check the WARP container:
```bash
docker compose logs warp
```
Restart if necessary:
```bash
docker compose restart warp
```

---

## 8. Add FAQ

# FAQ

### Does this project include TV channels?

No. FluxStream generates and proxies playlists from configured sources. Users are responsible for providing their own compatible sources.

---

### Does this work on Linux?

Yes. Linux is the recommended deployment platform.

---

### Does this work on Windows?

Yes. Docker Desktop is supported.

---

### Does FluxResolver require Cloudflare WARP?

No. Only FluxStream routes traffic through WARP.

---

### Can I run the services separately?

Yes. FluxStream and FluxResolver are independent services and can be deployed individually if desired.

---

# Available Endpoints

## FluxStream

| Endpoint        | Description             |
| --------------- | ----------------------- |
| `/playlist.m3u` | Generated IPTV playlist |
| `/play?id=`     | Stream by channel ID    |
| `/stream?id=`   | Stream endpoint         |

---

## FluxResolver

| Endpoint                   | Description      |
| -------------------------- | ---------------- |
| `/manifest.json`           | Stremio Manifest |
| `/stream/movie/{id}.json`  | Movie streams    |
| `/stream/series/{id}.json` | TV streams       |
| `/proxy/m3u8`              | Playlist proxy   |
| `/proxy/segment`           | Segment proxy    |
| `/proxy/media`             | Media proxy      |
| `/health`                  | Health check     |
| `/metrics`                 | Resolver metrics |

---

# Docker Services

The stack consists of three containers:

## Cloudflare WARP

Provides a dedicated network namespace used by the Live TV proxy to route outbound traffic. The `timstreams-proxy` service shares WARP's network stack using `network_mode: service:warp`. 

## FluxStream

* Live TV discovery
* Dynamic M3U generation
* FFmpeg streaming
* Browser-based extraction
* Automatic cache refresh
* Exposed on port **8085**

## FluxResolver

* Stremio addon
* Multi-provider resolution
* Token refresh
* Playlist proxy
* Segment proxy
* Health API
* Metrics API
* Exposed on port **7000**

---

# How It Works

### Live TV

1. Fetch channel metadata.
2. Cache channel information.
3. Discover the underlying HLS stream using HTTP scraping or a persistent Playwright browser.
4. Validate the discovered stream.
5. Relay it through FFmpeg.
6. Serve a dynamic M3U playlist to IPTV clients.  

### Movies & TV Shows

1. Receive a Stremio request.
2. Validate the requested IMDb ID against cached lists.
3. Query multiple VidSrc providers in parallel.
4. Return the first working stream.
5. Proxy playlists and media segments.
6. Automatically refresh expired stream tokens when needed.  

---

# Performance Features

* Persistent Chromium browser
* Browser reuse
* Async FastAPI
* HTTP connection pooling
* Automatic cache cleanup
* Playlist caching
* Stream caching
* Parallel provider resolution
* Automatic browser recovery
* Automatic token renewal
* Automatic reconnection
* FFmpeg relay
* Low-latency streaming

---

# Roadmap

* [ ] Web dashboard
* [ ] EPG support
* [ ] Multiple Live TV providers
* [ ] Docker image publishing
* [ ] Prometheus metrics
* [ ] Grafana dashboard
* [ ] Authentication
* [ ] Automatic updates
* [ ] Kubernetes manifests

---

# Disclaimer

This project is intended for **self-hosting, development, and educational purposes**. Users are responsible for ensuring their use complies with applicable laws, regulations, and the terms of any content providers or services they access.

---

# Contributing

Pull requests, bug reports, feature requests, and performance improvements are welcome.

If you encounter an issue:

1. Open an issue.
2. Include logs.
3. Describe how to reproduce it.
4. Include your Docker version and operating system.

---
