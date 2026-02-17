# Architecture

## Pipeline Flow

```mermaid
flowchart LR
    A["📄 Query File\nOne query per line"] --> B["🔍 DDG Search\ntext / news mode\nretry + cooldown"]
    B --> C["🌐 Async Fetch\naiohttp + fingerprints\nthrottle + robots.txt\noptional Tor"]
    C --> D["📝 Extract\ntrafilatura 2-pass\npdfplumber\ncleanup + date filter"]
    D --> E["💾 Dedup + Store\nURL + SHA256 dedup\nParquet + JSONL\ncheckpoint"]

    style A fill:#4a90d9,stroke:#2c5f8a,color:#fff
    style B fill:#f5a623,stroke:#c47d0e,color:#fff
    style C fill:#7b68ee,stroke:#5a4bc7,color:#fff
    style D fill:#50c878,stroke:#3a9a5c,color:#fff
    style E fill:#e74c3c,stroke:#c0392b,color:#fff
```

## Module Map

```mermaid
graph TB
    subgraph CLI["CLI Layer"]
        main["main.py\nargparse CLI"]
        dunder_main["__main__.py\npython -m entry"]
    end

    subgraph Core["Core"]
        config["config.py\nScraperConfig\nfrozen dataclass"]
        pipeline["pipeline.py\nOrchestrator"]
        checkpoint["checkpoint.py\nAtomic JSON saves"]
    end

    subgraph Search["search/"]
        ddg["duckduckgo.py\nDDG text + news\ntenacity retry\nTor circuit renewal"]
    end

    subgraph Fetch["fetch/"]
        client["client.py\nAsync HTTP client"]
        fingerprints["fingerprints.py\n5 browser profiles"]
        throttle["throttle.py\nPer-domain adaptive\nrate limiter"]
        robots["robots.py\nrobots.txt compliance"]
        tor["tor.py\nSOCKS5 proxy\ncircuit renewal"]
    end

    subgraph Extract["extract/"]
        html["html.py\ntrafilatura 2-pass"]
        pdf["pdf.py\npdfplumber"]
        clean["clean.py\n10 regex patterns"]
        date_filter["date_filter.py\nDate range filter"]
    end

    subgraph Store["store/"]
        dedup["dedup.py\nURL + SHA256 dedup"]
        output["output.py\nParquet + JSONL\nappend mode"]
    end

    dunder_main --> main
    main --> config
    main --> pipeline
    pipeline --> ddg
    pipeline --> client
    pipeline --> html
    pipeline --> pdf
    pipeline --> clean
    pipeline --> date_filter
    pipeline --> dedup
    pipeline --> output
    pipeline --> checkpoint
    client --> fingerprints
    client --> throttle
    client --> robots
    client --> tor

    style CLI fill:#f0f0f0,stroke:#999
    style Core fill:#e8f4fd,stroke:#4a90d9
    style Search fill:#fff3e0,stroke:#f5a623
    style Fetch fill:#ede7f6,stroke:#7b68ee
    style Extract fill:#e8f5e9,stroke:#50c878
    style Store fill:#fce4ec,stroke:#e74c3c
```

## Data Flow

Each query passes through four typed result stages:

```mermaid
flowchart LR
    Q["query string"] -->|"DDG Search"| SR["list of SearchResult\n─────────────\ntitle: str\nurl: str\nsnippet: str"]
    SR -->|"Async Fetch"| FR["FetchResult\n─────────────\nurl: str\nbody: bytes\nstatus_code: int\ncontent_type: str"]
    FR -->|"Extract"| ER["ExtractionResult\n─────────────\nurl: str\ntitle: str\nfull_text: str\ndate: datetime\nsource: str"]
    ER -->|"Dedup + Store"| PR["Parquet Row\n─────────────\ncompany: str\ntitle: str\nlink: str\nsnippet: str\ndate: timestamp\nsource: str\nfull_text: str\nsource_file: str"]

    style Q fill:#f5f5f5,stroke:#999
    style SR fill:#fff3e0,stroke:#f5a623
    style FR fill:#ede7f6,stroke:#7b68ee
    style ER fill:#e8f5e9,stroke:#50c878
    style PR fill:#fce4ec,stroke:#e74c3c
```

## Adaptive Throttle Strategy

Each domain maintains its own delay that adjusts based on server responses:

```mermaid
flowchart TD
    REQ["Send Request"] --> RESP{"Response?"}
    RESP -->|"200 OK"| GOOD["✅ Halve delay\nfloor: 0.5s"]
    RESP -->|"429 / 503"| RATE["⚠️ Double delay\nceiling: 60s"]
    RESP -->|"Connection Error"| ERR["❌ Double delay\nceiling: 60s"]
    GOOD --> NEXT["Next request\nfor this domain"]
    RATE --> NEXT
    ERR --> NEXT
    NEXT --> REQ

    style GOOD fill:#e8f5e9,stroke:#50c878
    style RATE fill:#fff3e0,stroke:#f5a623
    style ERR fill:#fce4ec,stroke:#e74c3c
```

**Convergence:** The halve-on-success / double-on-failure strategy means each domain converges to its maximum sustainable rate independently. A single slow or rate-limiting domain doesn't affect the others.

## Checkpoint & Resume Flow

```mermaid
flowchart TD
    START["Start Pipeline"] --> CHECK{"--resume\nflag set?"}
    CHECK -->|No| FRESH["Process all queries"]
    CHECK -->|Yes| LOAD["Load checkpoint JSON"]
    LOAD --> SKIP["Skip completed queries"]
    SKIP --> PROC["Process remaining queries"]
    FRESH --> LOOP
    PROC --> LOOP

    LOOP["For each query"] --> SEARCH["Search DDG"]
    SEARCH --> FETCH["Fetch + Extract"]
    FETCH --> SAVE["Save to Parquet"]
    SAVE --> CP["Write checkpoint\n(atomic: tmp → rename)"]
    CP --> DONE{"More queries?"}
    DONE -->|Yes| LOOP
    DONE -->|No| FIN["Done"]

    style CHECK fill:#fff3e0,stroke:#f5a623
    style CP fill:#e8f4fd,stroke:#4a90d9
    style FIN fill:#e8f5e9,stroke:#50c878
```

## Design Rationale

**DuckDuckGo only** — No API keys, no billing, works through Tor. News mode produces strong results for financial content without rate-limit pressure.

**trafilatura over BeautifulSoup** — Purpose-built for main content extraction with metadata (title, date, author). The 2-pass strategy (precision mode first, then relaxed fallback) maximizes recall without sacrificing quality.

**Adaptive per-domain throttling** — Each domain gets its own delay that adjusts dynamically: successful fetches halve the delay (floor 0.5s), 429/503 responses double it (ceiling 60s). This converges to the optimal rate per site without a single slow domain blocking the whole pipeline.

**Frozen dataclass config** — `ScraperConfig` is immutable after creation. Stealth mode creates a new instance with overrides rather than mutating state during async execution.

**Checkpoint per query** — Queries are the natural unit of work (5-20 pages each). Atomic JSON writes (write to temp file, then rename) prevent corruption on crash.

**Parquet output** — Columnar format with snappy compression. Schema matches the downstream `merged_by_year` pipeline for compatibility. Append mode allows incremental writes.
