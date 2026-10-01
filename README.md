# NAIC / SBS Multi-State Insurance Licensee Scraper

High-performance, automated extractor for insurance producer and licensee records from the **National Association of Insurance Commissioners (NAIC)** State Based Systems (SBS) platform.

---

## Two Available Scrapers

| Script | Engine Type | Features & Output |
| :--- | :--- | :--- |
| **`naic_scraper_fast.py`** *(Recommended)* | **Parallel Thread Pool (8 workers)** | **~3-4x faster**. Saves each state to its own file in `states/` (e.g. `states/NJ.csv`), then merges all completed states into `all_states_producers.csv`. |
| **`naic_scraper.py`** | **Sequential** *(legacy)* | Runs 1 request at a time. Streams to one CSV. **Does not** have the failure queue or parallel retries — use `naic_scraper_fast.py`. |

---

## The Failure Queue — your safety net for unreliable internet

Every query that fails **even after all retry rounds** is written to `failed_queries.csv`.
That file holds **only outstanding failures**: the moment a combo later returns data, its row
is **deleted**. So the file always tells you exactly what is still missing.

It is written after every state (and atomically, so an interrupted run can't corrupt it) and it
is the **source of truth** — it outlives `states_checkpoint.json`, so deleting or corrupting the
checkpoint can never lose track of what was never collected.

### The loop you actually run

```bash
# Pass 1 — scrape everything. Internet drops kill some combos; they get queued.
python3 naic_scraper_fast.py --all-states

# Pass 2, 3, 4 … — just run the SAME command again.
python3 naic_scraper_fast.py --all-states
```

On every re-run the scraper:
1. **Skips** states that are fully covered (instant, no requests).
2. **Retries only the outstanding combos** for states with failures — not all 676. A combo that
   returned HTTP 200 already has its data safely in `states/<ST>.csv`, so re-firing it is waste.
3. **Appends** recovered records to that state's CSV (deduplicated by license number).
4. **Deletes** those rows from `failed_queries.csv`.
5. **Rebuilds** `all_states_producers.csv` from scratch.
6. Prints a coverage report.

Repeat until the queue is empty. Nothing is ever silently dropped.

To retry only the queue explicitly (equivalent, but never runs a full grid):

```bash
python3 naic_scraper_fast.py --retry-failed
python3 naic_scraper_fast.py --retry-failed --states NJ,DE   # only these
```

### Coverage report & exit codes

Every run ends with a report of what is still outstanding, and a non-zero exit code while
anything remains — so you can gate on it:

```bash
python3 naic_scraper_fast.py --all-states && echo "ALL 34 STATES FULLY COVERED"
```

```
  COVERAGE REPORT
  [PENDING] 137 query/combo(s) still need data across 4 jurisdiction(s):

  STATE  NAME                    PENDING
  ------ ---------------------- --------
  NJ     New Jersey                   71
  DE     Delaware                     42

  Failure reasons:
    proxy_error             98
    http_403               25
    read_timeout           14

  Most-attempted (persistent problems, check these first):
    NJ v:b   attempts=30  last=2026-10-01 20:35:06  proxy_error
```

### Retry rounds

After the main pass, failures get `--retry-rounds` more parallel passes (default **3**) before
being parked in the queue. Each round runs across the whole worker pool with proxies rotating.
If a round recovers **nothing**, the scraper assumes the connection is down and stops early
instead of burning time.

```bash
python3 naic_scraper_fast.py --all-states --retry-rounds 5
```

So a single combo gets up to `5 attempts × (1 main pass + 5 rounds) = 30 requests` before it is
written to the queue for later.

### `failed_queries.csv` columns

| Column | Description |
| :--- | :--- |
| `jurisdiction` | 2-letter state code |
| `combo` | `last_letter:first_letter` |
| `last_letter` / `first_letter` | The individual letters |
| `reason` | The server's own message where available, e.g. `http_429 Rate limit exceeded`, `http_409 Conflict`, plus `proxy_error`, `connect_timeout`, `read_timeout`, `ssl_error`, `connection_error`, `bad_json` |
| `attempts_total` | Lifetime attempts — high values flag persistent problems |
| `first_failed_at` / `last_failed_at` | Timestamps |

### Reading the reasons

| Reason | Meaning |
| :--- | :--- |
| `http_429 Rate limit exceeded` | Too many requests from that IP. Transient — retried in-call, then rotated. Heavy counts mean you're over-driving the pool: lower `--workers`. |
| `http_409 Conflict` | Transient server-side conflict, usually a duplicate in-flight search. Retried in-call and rotated. |
| `http_403` | That egress IP is blocked by NAIC. Rotated; the proxy is benched. |
| `proxy_error` / `connect_timeout` | The proxy itself is dead or unreachable. Benched for 60s, then retried. |
| `read_timeout` / `ssl_error` | Flaky line or proxy. Usually self-heals on the next attempt. |

If `proxy_error` dominates, the proxies are the problem — re-check them. If `http_429` dominates,
you are going too fast — drop `--workers` (or delete it and let auto-tuning pick a lower number).

### Interruption safety

| Situation | What happens |
| :--- | :--- |
| `Ctrl+C` mid-run | State CSV flushed **and closed**, failure queue saved, checkpoint saved, exit code 1. |
| Checkpoint deleted/corrupt | Queue is unaffected. Affected states are re-scanned; dedup prevents duplicates. |
| Failure queue corrupt | The unreadable file is copied to `failed_queries.csv.corrupt` and you get a loud warning rather than a silent reset. |
| Run stopped part-way through a state | That state is **not** marked grid-complete, so the un-reached combos are picked up on the next run instead of being lost. |

---

## Key Features

- **Direct API Extraction**: Queries the underlying NAIC REST API directly (`api.prod.naic.org`), running ~50x faster than browser automation with zero browser overhead.
- **Single-Alphabet Combination Grid**: Iterates through single letters:
  $$\text{Last Name } [A \dots Z] \times \text{First Name } [A \dots Z] = \mathbf{676 \text{ queries per state}}$$
- **All 34 SBS States Supported**: Covers all 34 participating jurisdictions (30 states, DC, Guam, Puerto Rico, US Virgin Islands).
- **Dynamic Statutory Code Resolution**: Automatically queries each state's metadata to resolve statutory codes:
  - e.g., Delaware (`licenseType=01`, `loaType=1`), Wisconsin (`licenseType=INT`, `loaType=LI`), North Carolina (`loaType=1`), etc.
- **Direct Interactive Links**: Automatically generates verified, clickable links for each row:
  - **`name_link`**: Direct URL to the Licensee Summary page.
  - **`license_manager_link`**: Direct URL to the state's License Manager portal.
- **Automatic Deduplication**: Deduplicates records by `(jurisdiction, license_number)`.
- **Zero Data-Loss Failure Queue**: Every query that fails after all retry rounds is recorded in
  `failed_queries.csv` with its reason and attempt count. Rows are **deleted on success**, so the
  file always lists exactly what is still missing. Re-run the scraper as many times as you like —
  it retries just the outstanding combos, appends to the right state CSV, and rebuilds the master.
- **Multi-Round Parallel Retries**: `--retry-rounds` (default 3) parallel passes over any failures
  before they are queued, each rotating proxies. Stops early if a round recovers nothing.
- **Retryable Transient Blocks**: HTTP 403/407/409/418/429 rotate to the next proxy with backoff
  instead of being written off permanently. Only genuinely fatal `400/404/422` fail immediately.
- **Temporary Proxy Cooldowns**: A proxy that errors is benched for 60s (doubling to a 300s cap on
  repeat failures) and then returns to rotation automatically — a single blip no longer shrinks the
  pool for the rest of the run.
- **Honest Worker Auto-Tuning**: Concurrency is derived from **distinct exit IPs**, not the number
  of lines in `proxies.txt`. Several entries often share one IP on different ports, and rate limits
  apply per IP.
- **Live Checkpointing & Resume**: Can be stopped anytime (`Ctrl+C`) and resumed with `--resume`.
- **CSV Output Ready for Google Sheets / Excel**: Immediate real-time flushing into clean CSV files.

---

## Quick Start (High-Speed Parallel Scraper)

### 1. Requirements
Ensure Python 3.8+ and `requests` are installed:
```bash
pip install -r requirements.txt
```

### 2. Run for ALL 34 States (Fast Parallel Mode)
```bash
python3 naic_scraper_fast.py --all-states
```
**What happens**:
1. Creates a `states/` directory.
2. Scrapes each state concurrently (auto-scales to 3× your proxy count — 24 workers with 8 proxies).
3. Saves each state to its own clean spreadsheet: `states/AL.csv`, `states/NJ.csv`, etc.
4. Retries failures up to `--retry-rounds` times (default 3), in parallel.
5. Anything still failing is queued in `failed_queries.csv` — **just run this same command again**.
6. When finished, automatically merges all states into the master `all_states_producers.csv`.

### 3. Run for Specific States (Fast Mode)
```bash
python3 naic_scraper_fast.py --states NJ,DE,MD
```

### 4. Run for a Single State (Fast Mode)
```bash
python3 naic_scraper_fast.py --jurisdiction NJ
```

### 5. Resume if Stopped at Any Time
```bash
python3 naic_scraper_fast.py --resume
```
*(Resume is always on — the flag is accepted for clarity. Checkpoints and the failure queue are
always loaded.)*

### 6. Re-run Only Queued Failures
```bash
python3 naic_scraper_fast.py --retry-failed
python3 naic_scraper_fast.py --retry-failed --states NJ,DE   # limit to specific states
```

### 7. Re-merge Existing State CSVs into Master File
```bash
python3 naic_scraper_fast.py --merge-only
```

---

## High-Speed Proxy Pool & 10x Throughput Scaling

### Why Use Proxies?
When scraping from a single IP address, NAIC limits concurrent socket connections to around 8–10 concurrent threads (~2.8 requests/second). Pushing beyond that causes connection resets or dropped packets.

Adding a pool of **10 proxies removes this single-IP bottleneck**:
- Each proxy IP sends requests at a modest, safe rate (~2.5 req/s).
- 10 distinct IPs operate in parallel across 25–40 workers.
- **Combined throughput reaches ~25 to 30 requests per second** (a ~10x multiplier!).

### Performance Comparison

| Configuration | Concurrency | Request Rate | Time per State (676 queries) | Time for All 34 States (22,984 queries) |
| :--- | :---: | :---: | :---: | :---: |
| **Direct IP (No Proxies)** | 8 workers | ~2.8 req/sec | ~4 minutes | ~2.2 hours |
| **8 exit IPs** | 24 workers | ~6 req/sec | ~2 minutes | ~60 minutes |
| **25+ exit IPs** | 40–50 workers | ~12 req/sec | ~1 minute | ~30 minutes |

> [!IMPORTANT]
> These rates assume **low-latency** proxies (~100–300 ms per request). Residential proxies are
> typically **3–8 seconds** per request — measured on this pool, latency was
> **3.5 s min / 4.0 s median / 7.3 s max**. Throughput is then bounded by
> `workers ÷ latency`.

### Your pool has 28 entries but only 10 exit IPs

`proxies.txt` lists multiple ports on the same host. Those are **one exit IP each for rate-limit
purposes**, so the scraper auto-tunes on distinct hosts, not line count:

```
  Proxy Pool:        28 entries / 10 exit IPs (28 active)
  Workers:           30 concurrent threads
```

That is ~2 concurrent requests per real IP. Measured behaviour on a single IP: the first burst of
24 requests returned all 200s, but an immediate second burst returned **13 read timeouts and 2
connection errors** — NAIC throttles hard per IP. Shared datacenter IPs are worse still, because
the quota is shared with other customers, so bursts can collide from outside your run.

If the failure queue fills with `http_429`, you are over-driving the pool. Drop the worker count
explicitly — `--workers 10` is exactly 1 per exit IP, `--workers 5` is half:

```bash
python3 naic_scraper_fast.py --all-states --workers 10
```

Slower per state, but far fewer retries and far fewer permanently-parked combos. The failure queue
means a conservative run is never lossy — just run it again afterwards if anything is left.

### Maintaining Your Proxy Pool

Check proxies against the real API before a long run, one at a time, and keep a backup:

```bash
cp proxies.txt proxies.txt.bak
python3 naic_scraper_fast.py --jurisdiction NJ --test    # 3-combo live smoke test
```

Remove any that fail, then re-test. If a run suddenly fails wholesale, suspect provider-side rate
limiting before suspecting the scraper — wait a few minutes rather than adding retries. The
failure queue makes this safe: nothing is lost, you just retry later.

### How to Configure Proxies

#### Method 1: Using `proxies.txt` (Automatic Detection)
Create a file named `proxies.txt` in the scraping directory (see `proxies.example.txt` template). The scraper automatically detects and loads it!

Supported formats (one per line):
```text
# Format 1: IP:PORT:USER:PASS (Webshare, BrightData, Smartproxy, etc.)
192.168.1.100:8080:username:password

# Format 2: IP:PORT (IP-whitelisted proxies)
192.168.1.100:8080

# Format 3: HTTP URL
http://username:password@proxy.example.com:8080

# Format 4: SOCKS5
socks5://username:password@proxy.example.com:1080
```

#### Method 2: Command-Line Flags
```bash
# Provide a custom proxy file:
python3 naic_scraper_fast.py --all-states --proxy-file my_proxies.txt

# Provide a single proxy:
python3 naic_scraper_fast.py --all-states --proxy "http://user:pass@host:port"

# Scale concurrency manually with 10+ proxies:
python3 naic_scraper_fast.py --all-states --proxy-file proxies.txt --workers 30
```

> [!TIP]
> **Built-in Dead Proxy Failover**: If any proxy disconnects or times out, the scraper immediately catches the error, marks that specific proxy bad, and rotates the request to a working proxy in the pool without losing the query or delaying the run.

---

## Sequential Scraper (`naic_scraper.py`) — legacy

Kept for reference and single-state debugging. It streams into one CSV and supports targeting
individual letter combos, but it has **no failure queue** and no parallel retries, so interrupted
runs can lose track of what was never collected.

```bash
python3 naic_scraper.py --jurisdiction NJ --output nj_life_producers.csv
python3 naic_scraper.py --all-states --output all_states_producers.csv
python3 naic_scraper.py --resume
```

For anything long-running, use `naic_scraper_fast.py`.

---

## CSV Column Structure

| Column | Header | Description |
| :---: | :--- | :--- |
| 1 | **`name`** | Full producer name (`LAST, FIRST MIDDLE`) |
| 2 | **`license_number`** | State license number |
| 3 | **`effective_date`** | Established / effective date |
| 4 | **`expiration_date`** | License expiration date |
| 5 | **`business_address`** | City, State, and Zipcode |
| 6 | **`business_phone`** | Formatted phone number `(XXX) XXX-XXXX` |
| 7 | **`license_type_status`** | License type & status (e.g. `Insurance Producer-Active`) |
| 8 | **`lines_of_authority`** | Semicolon-separated lines of authority (e.g. `Life; Health; Property`) |
| 9 | **`npn`** | National Producer Number |
| 10 | **`residency`** | Resident status (`Yes` / `No`) |
| 11 | **`fein`** | Federal Employer Identification Number (if applicable) |
| 12 | **`dba_name`** | DBA / Trading name |
| 13 | **`home_state`** | Designated home state |
| 14 | **`jurisdiction`** | 2-letter state code (e.g. `NJ`, `DE`, `WI`) |
| 15 | **`name_link`** | Verified direct URL to the Licensee Summary page |
| 16 | **`license_manager_link`** | Verified direct URL to the License Manager portal |

> [!NOTE]
> The two link columns are placed last so the contact/licence data stays together on the left and the URLs never interrupt it.

---

## Importing into Google Sheets

1. Open [Google Sheets](https://sheets.new).
2. Go to **File** $\rightarrow$ **Import** $\rightarrow$ **Upload**.
3. Select your output CSV file (e.g. `all_states_producers.csv` or any file from `states/`).
4. Select **Replace current sheet** or **Insert new sheet**, and click **Import data**.
5. Columns 15 (`name_link`) and 16 (`license_manager_link`) will automatically be clickable direct links.
