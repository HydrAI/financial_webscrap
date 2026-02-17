# Ethical Scraping

financial-scraper is designed to be a responsible web citizen. This document explains the mechanisms built into the pipeline to minimize impact on target websites.

## robots.txt Compliance

By default, the scraper fetches and respects `robots.txt` for every domain before making requests. Pages disallowed by `robots.txt` are skipped with a log message. This can be disabled with `--no-robots` but is strongly discouraged.

## Adaptive Rate Limiting

Each domain gets its own rate limiter that adjusts dynamically based on server responses:

```mermaid
flowchart LR
    A["Request to\nexample.com"] --> B{"Response"}
    B -->|"200 OK"| C["✅ Delay ÷ 2\nmin 0.5s"]
    B -->|"429 / 503"| D["⚠️ Delay × 2\nmax 60s"]
    B -->|"Error"| E["❌ Delay × 2\nmax 60s"]
    C --> F["Next request"]
    D --> F
    E --> F

    style C fill:#e8f5e9,stroke:#50c878
    style D fill:#fff3e0,stroke:#f5a623
    style E fill:#fce4ec,stroke:#e74c3c
```

| Response | Action | Rationale |
|----------|--------|-----------|
| 200 OK | Halve delay (floor 0.5s) | Server is comfortable, speed up gradually |
| 429 Too Many Requests | Double delay (ceiling 60s) | Back off immediately |
| 503 Service Unavailable | Double delay (ceiling 60s) | Server is overloaded |
| Connection error | Double delay | Network issue, reduce pressure |

This means the scraper automatically finds the sustainable request rate for each domain without manual tuning.

## Fingerprint Rotation

The scraper rotates between 5 browser fingerprint profiles (different User-Agent strings, Accept headers, and TLS settings). This is not for evasion — it reduces the chance of a single fingerprint being flagged as automated, which would result in blocks that waste bandwidth for both the scraper and the server.

## Tor Usage

Tor support is provided for privacy-sensitive research. When enabled:

```mermaid
flowchart TD
    Q["Process Query"] --> COUNT{"Query count\nmod N = 0?"}
    COUNT -->|Yes| RENEW["🔄 Renew Tor circuit\n(new IP address)"]
    COUNT -->|No| FETCH["Fetch pages"]
    RENEW --> FETCH
    FETCH --> RESP{"Got 429?"}
    RESP -->|Yes| AUTO["🔄 Auto-renew circuit"]
    RESP -->|No| NEXT["Next query"]
    AUTO --> NEXT

    style RENEW fill:#ede7f6,stroke:#7b68ee
    style AUTO fill:#fff3e0,stroke:#f5a623
```

- Requests route through the Tor network via SOCKS5 proxy
- Circuits renew every N queries (default 20) to distribute load
- Automatic renewal on rate-limit responses

**Guidance:** Use Tor for legitimate privacy needs (e.g., competitive research where your IP should not be associated with queries). Do not use Tor to circumvent rate limits or access restrictions — the adaptive throttling handles rate limits properly.

## Concurrency Limits

```mermaid
flowchart TD
    subgraph Normal["Default Mode"]
        N_POOL["Global pool: 10 concurrent"]
        N_D1["reuters.com\nmax 3 concurrent\ndelay 3-6s"]
        N_D2["bloomberg.com\nmax 3 concurrent\ndelay 3-6s"]
        N_D3["wsj.com\nmax 3 concurrent\ndelay 3-6s"]
        N_POOL --> N_D1
        N_POOL --> N_D2
        N_POOL --> N_D3
    end

    subgraph Stealth["Stealth Mode"]
        S_POOL["Global pool: 4 concurrent"]
        S_D1["reuters.com\nmax 2 concurrent\ndelay 5-8s"]
        S_D2["bloomberg.com\nmax 2 concurrent\ndelay 5-8s"]
        S_D3["wsj.com\nmax 2 concurrent\ndelay 5-8s"]
        S_POOL --> S_D1
        S_POOL --> S_D2
        S_POOL --> S_D3
    end

    style Normal fill:#e8f4fd,stroke:#4a90d9
    style Stealth fill:#fff3e0,stroke:#f5a623
```

| Setting | Default | Stealth Mode |
|---------|---------|--------------|
| Global concurrent fetches | 10 | 4 |
| Per-domain concurrent fetches | 3 | 2 |
| Inter-request delay | 3-6s | 5-8s |

Stealth mode reduces pressure across the board for large-scale runs.

## Best Practices

- **Use `--search-type news`** for financial content — it returns more relevant results and faces fewer rate limits
- **Start with small query files** (10-20 queries) to test before scaling up
- **Use `--stealth` for 100+ queries** to reduce server impact
- **Enable `--resume`** so interrupted runs don't re-fetch already-processed queries
- **Respect domain exclusions** — the default `exclude_domains.txt` blocks sites known to aggressively block scrapers
- **Check output quality** before scaling — if a domain consistently returns empty extractions, add it to the exclusion list rather than hammering it
- **Run during off-peak hours** when scraping large volumes
