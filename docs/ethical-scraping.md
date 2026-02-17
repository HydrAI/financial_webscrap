# Ethical Scraping

financial-scraper is designed to be a responsible web citizen. This document explains the mechanisms built into the pipeline to minimize impact on target websites.

## robots.txt Compliance

By default, the scraper fetches and respects `robots.txt` for every domain before making requests. Pages disallowed by `robots.txt` are skipped with a log message. This can be disabled with `--no-robots` but is strongly discouraged.

## Adaptive Rate Limiting

Each domain gets its own rate limiter that adjusts dynamically based on server responses:

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

- Requests route through the Tor network via SOCKS5 proxy
- Circuits renew every N queries (default 20) to distribute load
- Automatic renewal on rate-limit responses

**Guidance:** Use Tor for legitimate privacy needs (e.g., competitive research where your IP should not be associated with queries). Do not use Tor to circumvent rate limits or access restrictions — the adaptive throttling handles rate limits properly.

## Concurrency Limits

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
