#!/usr/bin/env python3
"""
NAIC / SBS High-Speed Parallel Multi-State Scraper
-------------------------------------------------
High-performance asynchronous/threaded scraper for the NAIC State Based Systems (SBS) API.

Failure Handling Model
----------------------
Because internet connections drop mid-run, every query that fails even after the
retry rounds is recorded in a durable work queue (`failed_queries.csv`). The file
holds ONLY outstanding failures: when a combo later succeeds its row is deleted.
Re-running the scraper retries just those pending combos, appends the recovered
records to the matching `states/<ST>.csv`, and rebuilds the master CSV. Repeat
runs until the queue is empty - nothing is ever silently dropped.

Features:
  - Parallel Worker Pool: Runs multiple queries concurrently (default 8 workers)
    to achieve ~3x to 4x faster execution while staying safely under server limits.
  - Durable Failure Queue: `failed_queries.csv` records every permanently failed
    query so no leads are lost. Survives checkpoint deletion.
  - Multi-Round Retries: `--retry-rounds` (default 3) passes over any failures
    before they are parked in the queue, each pass parallel across proxies.
  - Per-State CSV Storage: Saves each state to its own dedicated file in `states/`
    (e.g., `states/NJ.csv`, `states/DE.csv`).
  - Master CSV Merge: Automatically compiles all individual state files into
    one unified master CSV (`all_states_producers.csv`) once scraping is complete.
  - State Checkpoint: Can be stopped (`Ctrl+C`) and resumed anytime (`--resume`).
  - Gap Report: Prints remaining failures and exits non-zero while any remain.

Discovered API Endpoint:
https://api.prod.naic.org/sbs-external-lookup-api/licenseLookup/search
"""

import os
import sys
import time
import json
import csv
import string
import shutil
import argparse
import urllib.parse
import threading
import collections
import concurrent.futures
import requests
from typing import Dict, List, Set, Any, Optional, Tuple, Union

API_URL = "https://api.prod.naic.org/sbs-external-lookup-api/licenseLookup/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://external-lookup-web.prod.naic.org",
    "Referer": "https://external-lookup-web.prod.naic.org/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

# 34 official jurisdictions on the NAIC SBS platform
SBS_JURISDICTIONS = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "GU": "Guam",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IA": "Iowa",
    "KS": "Kansas",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PR": "Puerto Rico",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "VI": "US Virgin Islands",
    "VT": "Vermont",
    "WV": "West Virginia",
    "WI": "Wisconsin",
}

CSV_FIELDNAMES = [
    "name",
    "license_number",
    "effective_date",
    "expiration_date",
    "business_address",
    "business_phone",
    "license_type_status",
    "lines_of_authority",
    "npn",
    "residency",
    "fein",
    "dba_name",
    "home_state",
    "jurisdiction",
    "name_link",
    "license_manager_link",
]

# Durable failure queue. Holds ONLY outstanding failures - a row is deleted the
# moment that combo succeeds, so the file is always "what is still missing".
FAILURE_FIELDS = [
    "jurisdiction",
    "combo",
    "last_letter",
    "first_letter",
    "reason",
    "attempts_total",
    "first_failed_at",
    "last_failed_at",
]

# HTTP statuses worth retrying. 409/418 are transient server-side conflicts
# (duplicate in-flight search / rate-limit-adjacent), 403/407 mean this egress IP
# is blocked or unauthenticated, 429/5xx are rate limits and upstream errors.
# Genuinely fatal client errors (400/404/422) are deliberately absent.
RETRYABLE_STATUS = {403, 407, 408, 409, 418, 425, 429, 500, 502, 503, 504}

# 26 last-name letters x 26 first-name letters.
GRID_SIZE = 676


def classify_exception(exc: Exception) -> str:
    """Map a requests exception to a short, human-readable reason code."""
    if isinstance(exc, requests.exceptions.ProxyError):
        return "proxy_error"
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return "connect_timeout"
    if isinstance(exc, requests.exceptions.ReadTimeout):
        return "read_timeout"
    if isinstance(exc, requests.exceptions.SSLError):
        return "ssl_error"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "connection_error"
    if isinstance(exc, requests.exceptions.Timeout):
        return "timeout"
    if isinstance(exc, requests.exceptions.JSONDecodeError):
        return "bad_json"
    return type(exc).__name__.lower()


def http_reason(resp: Any) -> str:
    """
    Build a short reason label for a non-200 response, including whatever the
    server said. Keeps it CSV-safe and bounded: e.g. "http_429 Rate limit exceeded".
    """
    label = f"http_{resp.status_code}"
    msg = None
    try:
        data = resp.json()
        if isinstance(data, dict):
            msg = data.get("error") or data.get("message") or data.get("detail") or data.get("title")
    except Exception:
        msg = None
    if not msg:
        msg = (resp.text or "").strip()
    msg = " ".join(str(msg).split())[:60].strip()
    return f"{label} {msg}" if msg else label


class FailureLedger:
    """
    Durable, human-readable queue of queries that produced no data.

    This is the source of truth for outstanding work - it outlives the checkpoint
    JSON, so deleting or corrupting the checkpoint can never lose track of which
    letter combinations were never collected.

    Rows are upserted on failure and DELETED on success, so the file always
    reflects exactly what still needs to be fetched. Writes are atomic
    (temp file + os.replace) so an interrupted run cannot corrupt the queue.
    """

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self.entries: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.corrupt_backup: Optional[str] = None
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
            return
        try:
            with open(self.path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                missing = [c for c in FAILURE_FIELDS if c not in (reader.fieldnames or [])]
                if missing:
                    raise ValueError(f"missing columns: {', '.join(missing)}")
                for row in reader:
                    jur = (row.get("jurisdiction") or "").strip().upper()
                    combo = (row.get("combo") or "").strip()
                    if not jur or ":" not in combo:
                        continue
                    last, first = combo.split(":", 1)
                    try:
                        attempts = int(row.get("attempts_total") or 0)
                    except ValueError:
                        attempts = 0
                    self.entries[(jur, combo)] = {
                        "jurisdiction": jur,
                        "combo": combo,
                        "last_letter": (row.get("last_letter") or last).strip(),
                        "first_letter": (row.get("first_letter") or first).strip(),
                        "reason": (row.get("reason") or "").strip(),
                        "attempts_total": attempts,
                        "first_failed_at": (row.get("first_failed_at") or "").strip(),
                        "last_failed_at": (row.get("last_failed_at") or "").strip(),
                    }
            if self.entries:
                print(f"[*] Failure queue loaded: {len(self.entries)} outstanding query/combo(s) from '{self.path}'")
        except Exception as e:
            # Never silently discard the record of what is missing - quarantine
            # the unreadable file so it can be inspected/recovered by hand.
            self.corrupt_backup = f"{self.path}.corrupt"
            try:
                shutil.copy(self.path, self.corrupt_backup)
            except Exception:
                self.corrupt_backup = None
            print(f"[!] WARNING: Could not parse failure queue '{self.path}' ({e}).")
            print(f"    Preserved a copy at '{self.corrupt_backup}' and starting an empty queue.")
            print(f"    Re-check that file before trusting the output - entries in it may not be retried.")
            self.entries = {}

    def pending_for(self, jurisdiction: str) -> List[Tuple[str, str]]:
        """Return (last_letter, first_letter) pairs still outstanding for a state."""
        jur = jurisdiction.upper()
        with self._lock:
            combos = [k[1] for k in self.entries if k[0] == jur]
        pairs = []
        for combo in combos:
            last, first = combo.split(":", 1)
            pairs.append((last, first))
        return sorted(pairs)

    def jurisdictions(self) -> List[str]:
        with self._lock:
            return sorted({k[0] for k in self.entries})

    def count(self) -> int:
        with self._lock:
            return len(self.entries)

    def mark_failed(self, jurisdiction: str, combo: str, reason: str, attempts: int = 0) -> None:
        """Record (or refresh) an outstanding failure."""
        jur = jurisdiction.upper()
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            key = (jur, combo)
            row = self.entries.get(key)
            if row is None:
                last, first = combo.split(":", 1)
                row = {
                    "jurisdiction": jur,
                    "combo": combo,
                    "last_letter": last,
                    "first_letter": first,
                    "reason": reason,
                    "attempts_total": 0,
                    "first_failed_at": now,
                    "last_failed_at": now,
                }
                self.entries[key] = row
            row["reason"] = reason or row.get("reason") or ""
            row["last_failed_at"] = now
            row["attempts_total"] = int(row.get("attempts_total") or 0) + max(1, attempts)

    def mark_resolved(self, jurisdiction: str, combo: str) -> bool:
        """Drop an outstanding failure because the combo finally returned data."""
        with self._lock:
            return self.entries.pop((jurisdiction.upper(), combo), None) is not None

    def reasons(self) -> "collections.Counter[str]":
        with self._lock:
            return collections.Counter(r.get("reason") or "unknown" for r in self.entries.values())

    def save(self) -> None:
        """Atomically rewrite the queue (temp file + replace)."""
        with self._lock:
            rows = sorted(
                self.entries.values(),
                key=lambda r: (r["jurisdiction"], r["last_letter"], r["first_letter"]),
            )
            tmp = f"{self.path}.tmp"
            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
                with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=FAILURE_FIELDS)
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(row)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.path)
            except Exception as e:
                print(f"[!] WARNING: Could not save failure queue '{self.path}' ({e}).")
                try:
                    if os.path.exists(tmp):
                        os.remove(tmp)
                except Exception:
                    pass


def format_phone(phone_str: str) -> str:
    """Format a 10-digit phone number as (XXX) XXX-XXXX."""
    digits = "".join(filter(str.isdigit, str(phone_str or "")))
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return phone_str or ""


def clean_loas(loas_str: str) -> str:
    """Clean HTML breaks into readable semicolon-separated string."""
    if not loas_str:
        return ""
    cleaned = loas_str.replace("<br/>", "; ").replace("<br>", "; ")
    return " ".join(cleaned.split())


def format_eta(seconds: float) -> str:
    """Format seconds into human readable m:ss or h:mm:ss."""
    if seconds < 0 or seconds > 86400 * 7:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"


class ProxyManager:
    """
    Thread-safe proxy rotator with dead-proxy failover and multi-format support:
      - IP:PORT (e.g. 192.168.1.1:8080)
      - IP:PORT:USER:PASS (e.g. 192.168.1.1:8080:username:password)
      - http://user:pass@host:port
      - socks5://user:pass@host:port

    A failing proxy is benched for a cooldown rather than banned for the whole
    run, so a transient blip does not permanently shrink the pool. Repeat
    failures lengthen the cooldown exponentially up to `max_cooldown`.
    """
    def __init__(self, proxy_source: Optional[Union[str, List[str]]] = None,
                 cooldown: float = 60.0, max_cooldown: float = 300.0):
        self.raw_proxies: List[str] = []
        self._lock = threading.RLock()
        self._index = 0
        self.cooldown_until: Dict[str, float] = {}
        self.strikes: Dict[str, int] = {}
        self.cooldown = cooldown
        self.max_cooldown = max_cooldown

        if isinstance(proxy_source, list):
            for item in proxy_source:
                normalized = self.normalize_proxy(item)
                if normalized:
                    self.raw_proxies.append(normalized)
        elif isinstance(proxy_source, str):
            if os.path.isfile(proxy_source):
                try:
                    with open(proxy_source, "r", encoding="utf-8") as f:
                        for line in f:
                            normalized = self.normalize_proxy(line)
                            if normalized:
                                self.raw_proxies.append(normalized)
                except Exception as e:
                    print(f"[!] Warning: Could not read proxy file '{proxy_source}': {e}")
            else:
                normalized = self.normalize_proxy(proxy_source)
                if normalized:
                    self.raw_proxies.append(normalized)

    @staticmethod
    def normalize_proxy(raw: str) -> Optional[str]:
        """Convert various proxy strings into standard URL format."""
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            return None
        if raw.startswith("http://") or raw.startswith("https://") or raw.startswith("socks5://"):
            return raw
        parts = raw.split(":")
        if len(parts) == 4:
            ip, port, user, pwd = parts
            return f"http://{user}:{pwd}@{ip}:{port}"
        elif len(parts) == 2:
            return f"http://{raw}"
        return f"http://{raw}"

    @staticmethod
    def host_of(proxy_url: str) -> str:
        """Bare host of a proxy URL - the exit IP for a dedicated proxy."""
        return proxy_url.split("@")[-1].split(":")[0].strip()

    def distinct_hosts(self) -> int:
        """
        Number of distinct exit IPs. Several entries often share one host with
        different ports, and rate limits apply per IP - so this, not the entry
        count, is what concurrency should be based on.
        """
        return len({self.host_of(p) for p in self.raw_proxies})

    def get_requests_proxies(self) -> Optional[Dict[str, str]]:
        """Return requests-compatible proxies dict {'http': url, 'https': url} via round-robin."""
        with self._lock:
            if not self.raw_proxies:
                return None
            now = time.monotonic()
            active = [p for p in self.raw_proxies if self.cooldown_until.get(p, 0.0) <= now]
            if not active:
                # Everything is benched - use the one that frees up soonest
                # instead of piling onto a proxy already known to be down.
                active = [min(self.raw_proxies, key=lambda p: self.cooldown_until.get(p, 0.0))]
            proxy_url = active[self._index % len(active)]
            self._index += 1
            return {"http": proxy_url, "https": proxy_url}

    def mark_failed(self, proxy_url: str, seconds: Optional[float] = None) -> float:
        """Bench a proxy for a cooldown (longer on repeat failures). Returns the delay used."""
        with self._lock:
            self.strikes[proxy_url] = self.strikes.get(proxy_url, 0) + 1
            base = self.cooldown if seconds is None else seconds
            delay = min(base * (2 ** (self.strikes[proxy_url] - 1)), self.max_cooldown)
            self.cooldown_until[proxy_url] = time.monotonic() + delay
            return delay

    def mark_ok(self, proxy_url: Optional[str]) -> None:
        """Clear a proxy's strike once it responds successfully."""
        if not proxy_url:
            return
        with self._lock:
            self.strikes.pop(proxy_url, None)
            self.cooldown_until.pop(proxy_url, None)

    def total_count(self) -> int:
        return len(self.raw_proxies)

    def active_count(self) -> int:
        now = time.monotonic()
        with self._lock:
            return len([p for p in self.raw_proxies if self.cooldown_until.get(p, 0.0) <= now])


def resolve_jurisdiction_codes(
    state_code: str,
    session: requests.Session,
    proxy_mgr: Optional[ProxyManager] = None
) -> Tuple[Optional[str], Optional[str]]:
    """Dynamically query state license types metadata to find statutory Producer and Life codes."""
    url = f"https://api.prod.naic.org/sbs-external-lookup-api/licenseLookup/licenseTypes/{state_code}"
    proxies = proxy_mgr.get_requests_proxies() if proxy_mgr else None
    try:
        resp = session.get(url, headers=HEADERS, proxies=proxies, timeout=12)
        if resp.status_code != 200:
            return ("PRO", "L")
        types = resp.json()
        if not isinstance(types, list):
            return ("PRO", "L")
    except Exception:
        return ("PRO", "L")

    # Match Producer license type
    pro_item = None
    for t in types:
        tname = t.get("name", "").strip().lower()
        if tname in ["insurance producer", "intermediary (agent) individual"]:
            pro_item = t
            break
    if not pro_item:
        for t in types:
            tname = t.get("name", "").strip().lower()
            if "producer" in tname or "intermediary" in tname or "agent" in tname:
                pro_item = t
                break
    if not pro_item and types:
        pro_item = types[0]

    pro_code = pro_item.get("code") if pro_item else "PRO"

    # Match Life LOA code
    life_code = None
    if pro_item:
        loas = pro_item.get("loaTypes", [])
        for loa in loas:
            if loa.get("name", "").strip().lower() == "life":
                life_code = loa.get("code")
                break
        if not life_code:
            for loa in loas:
                lname = loa.get("name", "").strip().lower()
                if "life" in lname and "variable" not in lname:
                    life_code = loa.get("code")
                    break

    return (pro_code, life_code)


def process_record(raw: Dict[str, Any], jurisdiction: str) -> Dict[str, Any]:
    """Convert raw NAIC API item to structured CSV row with verified direct links."""
    name = (raw.get("name") or "").strip()
    lic_num = str(raw.get("licenseNumber") or "").strip()
    lic_type_code = raw.get("licenseTypeCode") or "PRO"

    last_name = name.split(",")[0].strip() if "," in name else name
    encoded_last = urllib.parse.quote(last_name)

    name_link = (
        f"https://external-lookup-web.prod.naic.org/solar-external-lookup/lookup/licensee/summary/{lic_num}?jurisdiction={jurisdiction}&entityType=IND&licenseType={lic_type_code}"
        if lic_num
        else ""
    )
    license_manager_link = (
        f"https://external-lookup-web.prod.naic.org/solar-external-lookup/license-manager?entityType=IND&licenseNumber={lic_num}&lastName={encoded_last}&jurisdiction={jurisdiction}"
        if lic_num
        else ""
    )

    return {
        "name": name,
        "license_number": lic_num,
        "effective_date": raw.get("licenseEffectiveDate") or "",
        "expiration_date": raw.get("licenseExpirationDate") or "",
        "business_address": (raw.get("businessAddress") or "").strip(),
        "business_phone": format_phone(raw.get("businessPhone") or ""),
        "license_type_status": (raw.get("licenseType") or "").strip(),
        "lines_of_authority": clean_loas(raw.get("loas") or ""),
        "npn": raw.get("npn") or "",
        "residency": raw.get("residency") or "",
        "fein": raw.get("fein") or "",
        "dba_name": (raw.get("dbaName") or "").strip(),
        "home_state": raw.get("designatedHomeState") or "",
        "jurisdiction": jurisdiction,
        "name_link": name_link,
        "license_manager_link": license_manager_link,
    }


class FastParallelScraper:
    def __init__(
        self,
        target_states: List[str],
        output_dir: str = "states",
        master_output: str = "all_states_producers.csv",
        checkpoint_file: str = "states_checkpoint.json",
        max_workers: int = 8,
        delay_per_worker: float = 0.15,
        max_retries: int = 5,
        proxy_manager: Optional[ProxyManager] = None,
        retry_rounds: int = 3,
        failure_log: str = "failed_queries.csv",
    ):
        self.target_states = [s.upper().strip() for s in target_states if s.strip()]
        self.output_dir = output_dir
        self.master_output = master_output
        self.checkpoint_file = checkpoint_file
        self.max_workers = max_workers
        self.delay_per_worker = delay_per_worker
        self.max_retries = max_retries
        self.proxy_mgr = proxy_manager
        self.retry_rounds = max(1, retry_rounds)
        self.failure_log = failure_log
        self.failure_ledger = FailureLedger(failure_log)

        # If proxies are active, reduce worker sleep because requests distribute across separate IPs
        if self.proxy_mgr and self.proxy_mgr.total_count() > 0:
            self.delay_per_worker = 0.02

        self.session = requests.Session()
        self.completed_states: Set[str] = set()
        self.grid_completed: Set[str] = set()
        self.failed_combos: Dict[str, Set[str]] = {}
        self.total_unique_all_states = 0
        self.lock = threading.RLock()

        os.makedirs(self.output_dir, exist_ok=True)

    def load_checkpoint(self) -> None:
        """Load checkpoint file to resume gracefully."""
        if not os.path.exists(self.checkpoint_file):
            return
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.completed_states = set(data.get("completed_states", []))
            self.grid_completed = set(data.get("grid_completed", []))
            # A checkpoint written by an older build has no grid_completed list.
            # Those states are known-complete, so trust completed_states for them.
            self.grid_completed |= set(self.completed_states)
            raw_failed = data.get("failed_combos", {})
            self.failed_combos = {st: set(combos) for st, combos in raw_failed.items()}
            print(f"[*] Resumed checkpoint: {len(self.completed_states)} states fully covered, "
                  f"{len(self.grid_completed)} grids attempted.")
        except Exception as e:
            # Do not silently forget which combos were collected - the failure
            # queue is the real source of truth, so just say so and continue.
            print(f"[!] Warning: Could not read checkpoint file ({e}).")
            print(f"    Continuing from '{self.failure_log}'; already-collected CSVs are untouched.")
            print(f"    Affected states will be re-scanned (dedup prevents duplicates).")

    def save_checkpoint(self) -> None:
        """Save progress to checkpoint JSON."""
        try:
            with self.lock:
                with open(self.checkpoint_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "completed_states": sorted(list(self.completed_states)),
                        "grid_completed": sorted(list(self.grid_completed)),
                        "failed_combos": {st: sorted(list(combos)) for st, combos in self.failed_combos.items() if combos},
                        "total_unique_saved": self.total_unique_all_states,
                        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
                    }, f, indent=2)
        except Exception as e:
            print(f"[!] Warning: Could not save checkpoint ({e}).")

    def fetch_single_combo(
        self,
        jurisdiction: str,
        pro_code: Optional[str],
        life_code: Optional[str],
        last_letter: str,
        first_letter: str,
    ) -> Tuple[str, Optional[List[Dict[str, Any]]], str]:
        """
        Worker function for concurrent execution with proxy rotation and failover.

        Returns (combo_key, records, reason):
          - records is a list  -> HTTP 200, success (possibly zero records)
          - records is None   -> permanently failed; reason is a short code such as
                                 http_403 / proxy_error / read_timeout / bad_json
        """
        combo_key = f"{last_letter}:{first_letter}"
        params = {
            "jurisdiction": jurisdiction,
            "searchType": "Licensee",
            "entityType": "IND",
            "licenseStatus": "A",
            "lastName": last_letter,
            "firstName": first_letter,
        }
        if pro_code:
            params["licenseType"] = pro_code
        if life_code:
            params["loaType"] = life_code

        last_reason = "unknown"

        for attempt in range(1, self.max_retries + 1):
            proxy_dict = self.proxy_mgr.get_requests_proxies() if self.proxy_mgr else None
            proxy_url = proxy_dict.get("http") if proxy_dict else None
            try:
                if self.delay_per_worker > 0:
                    time.sleep(self.delay_per_worker)
                resp = self.session.get(
                    API_URL,
                    params=params,
                    headers=HEADERS,
                    proxies=proxy_dict,
                    timeout=14,
                )
                if resp.status_code == 200:
                    if self.proxy_mgr and proxy_url:
                        self.proxy_mgr.mark_ok(proxy_url)
                    try:
                        data = resp.json()
                    except ValueError:
                        last_reason = "bad_json"
                        continue
                    return (combo_key, data if isinstance(data, list) else [], "")
                if resp.status_code in RETRYABLE_STATUS:
                    # 409/418 transient conflicts and 403/407 blocked or
                    # unauthenticated IPs rotate to the next proxy with backoff
                    # instead of being written off permanently.
                    last_reason = http_reason(resp)
                    if resp.status_code == 407 and self.proxy_mgr and proxy_url:
                        self.proxy_mgr.mark_failed(proxy_url)
                    time.sleep(min(15.0, 1.5 ** attempt))
                    continue
                # Non-retryable client error (400/404/422...) - fail fast.
                last_reason = http_reason(resp)
                return (combo_key, None, last_reason)
            except (requests.exceptions.ProxyError, requests.exceptions.ConnectTimeout) as exc:
                # Proxy failure: mark it bad, pause briefly, retry on a different IP.
                last_reason = classify_exception(exc)
                if self.proxy_mgr and proxy_url:
                    self.proxy_mgr.mark_failed(proxy_url)
                time.sleep(min(3.0, 0.25 * attempt))
                continue
            except requests.RequestException as exc:
                last_reason = classify_exception(exc)
                time.sleep(min(15.0, 1.5 ** attempt))

        return (combo_key, None, last_reason)

    def _run_combo_batch(
        self,
        state_code: str,
        pro_code: Optional[str],
        life_code: Optional[str],
        combos: List[Tuple[str, str]],
        writer: "csv.DictWriter",
        csv_f: Any,
        state_seen_licenses: Set[str],
        state_failed: Set[str],
        label: str,
    ) -> Dict[str, int]:
        """
        Run a batch of combos through the parallel pool, writing hits and keeping
        the failure ledger in sync. Returns stats for this batch.
        """
        stats = {"completed": 0, "new": 0, "recovered": 0, "failed": 0}
        if not combos:
            return stats

        workers = min(self.max_workers, max(1, len(combos)))
        total = len(combos)
        start = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_combo = {
                executor.submit(
                    self.fetch_single_combo,
                    state_code,
                    pro_code,
                    life_code,
                    last_ch,
                    first_ch,
                ): (last_ch, first_ch)
                for last_ch, first_ch in combos
            }

            for future in concurrent.futures.as_completed(future_to_combo):
                last_ch, first_ch = future_to_combo[future]
                combo_key, raw_records, reason = future.result()
                stats["completed"] += 1

                if raw_records is None:
                    # Permanently failed this round: park it in the durable queue.
                    with self.lock:
                        state_failed.add(combo_key)
                        self.failure_ledger.mark_failed(
                            state_code, combo_key, reason, self.max_retries
                        )
                    stats["failed"] += 1
                    elapsed = time.time() - start
                    speed = stats["completed"] / elapsed if elapsed > 0 else 1.0
                    eta = (total - stats["completed"]) / speed if speed > 0 else 0
                    print(
                        f"  [{state_code}:{label}] [{stats['completed']:3d}/{total}] ({stats['completed']/total*100:5.1f}%) "
                        f"Last='{last_ch.upper()}', First='{first_ch.upper()}' -> "
                        f"[FAILED ❌ {reason}] queued | Speed: {speed:.1f} q/s | ETA: {format_eta(eta)}"
                    )
                    continue

                # Success: drop it from the queue and the pending-failure set.
                with self.lock:
                    was_pending = combo_key in state_failed
                    state_failed.discard(combo_key)
                    if was_pending:
                        self.failure_ledger.mark_resolved(state_code, combo_key)

                combo_new = 0
                for item in raw_records:
                    lic = str(item.get("licenseNumber") or "").strip()
                    if not lic or lic in state_seen_licenses:
                        continue
                    state_seen_licenses.add(lic)
                    writer.writerow(process_record(item, state_code))
                    combo_new += 1
                stats["new"] += combo_new
                if was_pending:
                    stats["recovered"] += 1
                if combo_new > 0:
                    try:
                        csv_f.flush()
                    except Exception:
                        pass

                elapsed = time.time() - start
                speed = stats["completed"] / elapsed if elapsed > 0 else 1.0
                eta = (total - stats["completed"]) / speed if speed > 0 else 0
                tag = "RECOVERED ✓" if was_pending else "ok"
                print(
                    f"  [{state_code}:{label}] [{stats['completed']:3d}/{total}] ({stats['completed']/total*100:5.1f}%) "
                    f"Last='{last_ch.upper()}', First='{first_ch.upper()}' -> "
                    f"[{tag}] Hits: {len(raw_records):2d} | +{combo_new:2d} new | "
                    f"State Total: {len(state_seen_licenses):5d} | Speed: {speed:.1f} q/s | ETA: {format_eta(eta)}"
                )

        return stats

    def scrape_state(self, state_code: str, combos_to_run: List[Tuple[str, str]]) -> int:
        """Scrape a single state concurrently and save immediately to states/{state_code}.csv."""
        state_name = SBS_JURISDICTIONS.get(state_code, state_code)
        state_file = os.path.join(self.output_dir, f"{state_code}.csv")

        # Load existing state records if resuming mid-state
        state_seen_licenses: Set[str] = set()
        file_exists = os.path.exists(state_file) and os.path.getsize(state_file) > 0
        if file_exists:
            try:
                with open(state_file, "r", encoding="utf-8-sig") as f:
                    for row in csv.DictReader(f):
                        lic = str(row.get("license_number", "")).strip()
                        if lic:
                            state_seen_licenses.add(lic)
            except Exception:
                pass

        csv_f = open(state_file, "a", newline="", encoding="utf-8-sig")
        writer = csv.DictWriter(csv_f, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
            csv_f.flush()

        if state_code not in self.failed_combos:
            self.failed_combos[state_code] = set()
        state_failed = self.failed_combos[state_code]

        new_records_added = 0
        try:
            print(f"\n{'='*78}")
            print(f"  Scraping State: {state_name.upper()} ({state_code})")
            print(f"  Resolving statutory codes for {state_name}...")
            pro_code, life_code = resolve_jurisdiction_codes(state_code, self.session, self.proxy_mgr)
            print(f"  Codes resolved: Producer='{pro_code}', Life LOA='{life_code}'")
            print(f"  Concurrency:    {self.max_workers} parallel workers")
            print(f"  Queue:          {len(combos_to_run)} queries to run "
                  f"({len(state_seen_licenses)} existing records in {state_code}.csv)")
            print(f"{'='*78}")

            # Pass 1 - the main scrape (or the pending-failure retry queue).
            stats = self._run_combo_batch(
                state_code, pro_code, life_code, combos_to_run,
                writer, csv_f, state_seen_licenses, state_failed, "SCRAPE",
            )
            new_records_added += stats["new"]
            self.failure_ledger.save()

            # Passes 2..N - retry rounds over whatever is still failing.
            round_no = 0
            while state_failed and round_no < self.retry_rounds:
                round_no += 1
                batch = sorted(state_failed)
                print(f"\n  [🔄 RETRY ROUND {round_no}/{self.retry_rounds}] "
                      f"Re-running {len(batch)} failed queries for {state_name} in parallel...")
                stats = self._run_combo_batch(
                    state_code, pro_code, life_code,
                    [(c.split(":", 1)[0], c.split(":", 1)[1]) for c in batch],
                    writer, csv_f, state_seen_licenses, state_failed, f"RETRY{round_no}",
                )
                new_records_added += stats["new"]
                self.failure_ledger.save()
                if stats["recovered"] == 0:
                    print(f"  [🔄 RETRY ROUND {round_no}] Recovered nothing - "
                          f"connection looks down. Stopping early and leaving "
                          f"{len(state_failed)} query/combo(s) in the queue.")
                    break
        finally:
            # Always persist, even on Ctrl+C, so nothing is silently lost.
            self.failure_ledger.save()
            try:
                csv_f.flush()
                csv_f.close()
            except Exception:
                pass
            with self.lock:
                self.total_unique_all_states += new_records_added
                # Only claim the whole grid was attempted if this run really did
                # fire every combo - an interrupted pass must stay "incomplete" so
                # the un-reached combos get picked up next time.
                if len(combos_to_run) >= GRID_SIZE:
                    self.grid_completed.add(state_code)
                if state_code in self.grid_completed and not state_failed:
                    self.completed_states.add(state_code)
                else:
                    self.completed_states.discard(state_code)
                self.save_checkpoint()

        total_state_records = len(state_seen_licenses)
        if state_failed:
            print(f"\n[✓] State Finished: {state_name} ({state_code})! "
                  f"{total_state_records} records in '{state_file}'. "
                  f"[!] {len(state_failed)} query/combo(s) still queued in the failure file.")
        elif state_code in self.grid_completed:
            print(f"\n[✓] State Finished: {state_name} ({state_code})! "
                  f"{total_state_records} records saved to '{state_file}' (fully covered)")
        else:
            print(f"\n[✓] State Finished: {state_name} ({state_code})! "
                  f"{total_state_records} records in '{state_file}'. "
                  f"[!] Only {len(combos_to_run)}/{GRID_SIZE} combos attempted so far - "
                  f"re-run to cover the rest.")
        return total_state_records

    def merge_all_state_files(self) -> int:
        """Merge all individual state CSV files from states/ into master CSV."""
        print(f"\n{'='*78}")
        print(f"  Compiling Master CSV: {self.master_output}")
        print(f"{'='*78}")

        total_merged = 0
        seen_master_keys: Set[Tuple[str, str]] = set()

        with open(self.master_output, "w", newline="", encoding="utf-8-sig") as master_f:
            writer = csv.DictWriter(master_f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()

            for state_code in sorted(list(SBS_JURISDICTIONS.keys())):
                state_file = os.path.join(self.output_dir, f"{state_code}.csv")
                if not os.path.exists(state_file):
                    continue

                state_count = 0
                try:
                    with open(state_file, "r", encoding="utf-8-sig") as sf:
                        for row in csv.DictReader(sf):
                            lic = row.get("license_number", "").strip()
                            jur = row.get("jurisdiction", "").strip() or state_code
                            key = (jur, lic)
                            if not lic or key in seen_master_keys:
                                continue
                            seen_master_keys.add(key)
                            writer.writerow(row)
                            total_merged += 1
                            state_count += 1
                    print(f"  [+] Merged {SBS_JURISDICTIONS.get(state_code, state_code):20s} ({state_code}): {state_count:6d} records")
                except Exception as e:
                    print(f"  [!] Error reading {state_file}: {e}")

            master_f.flush()

        print(f"\n[✓] Master CSV complete: {total_merged} total unique licensees across all states saved to '{self.master_output}'!")
        return total_merged

    def reconcile_with_ledger(self) -> int:
        """
        Merge the durable failure queue into the in-memory pending sets.

        The ledger wins on conflicts: a combo listed there is outstanding until a
        run actually returns data for it. This is what makes the queue survive a
        deleted or corrupt checkpoint.
        """
        adopted = 0
        with self.lock:
            for jur in self.failure_ledger.jurisdictions():
                pending = self.failure_ledger.pending_for(jur)
                bucket = self.failed_combos.setdefault(jur, set())
                before = len(bucket)
                bucket.update(f"{last}:{first}" for last, first in pending)
                adopted += len(bucket) - before
        if adopted:
            print(f"[*] Adopted {adopted} outstanding query/combo(s) from '{self.failure_log}'")
        return adopted

    def print_gap_report(self) -> int:
        """Summarise outstanding failures. Returns process exit code."""
        total = self.failure_ledger.count()
        print(f"\n{'='*78}")
        print("  COVERAGE REPORT")
        print(f"{'='*78}")

        if total == 0:
            print("  [OK] No outstanding failures - every letter combination was collected.")
            print(f"{'='*78}\n")
            return 0

        by_state: Dict[str, List[Dict[str, Any]]] = {}
        for row in self.failure_ledger.entries.values():
            by_state.setdefault(row["jurisdiction"], []).append(row)

        print(f"  [PENDING] {total} query/combo(s) still need data across "
              f"{len(by_state)} jurisdiction(s):\n")
        print(f"  {'STATE':<6} {'NAME':<22} {'PENDING':>8} {'RETRY CMD'}")
        print(f"  {'-'*6} {'-'*22} {'-'*8} {'-'*26}")
        for jur in sorted(by_state):
            rows = by_state[jur]
            print(f"  {jur:<6} {SBS_JURISDICTIONS.get(jur, jur):<22} {len(rows):>8}   "
                  f"--jurisdiction {jur}")
        print()
        print("  Failure reasons:")
        for reason, count in self.failure_ledger.reasons().most_common():
            print(f"    {reason:<18} {count:>6}")

        worst = sorted(
            self.failure_ledger.entries.values(),
            key=lambda r: int(r.get("attempts_total") or 0),
            reverse=True,
        )[:5]
        if worst and int(worst[0].get("attempts_total") or 0) > 0:
            print("\n  Most-attempted (persistent problems, check these first):")
            for row in worst:
                print(f"    {row['jurisdiction']} {row['combo']:<5} "
                      f"attempts={row['attempts_total']:<5} last={row['last_failed_at']}  "
                      f"{row['reason']}")

        print(f"\n  Queue file: {os.path.abspath(self.failure_log)}")
        print("  Retry them with:  python3 naic_scraper_fast.py --retry-failed")
        print("  (or just re-run the same command - pending combos are picked up automatically)")
        print(f"{'='*78}\n")
        return 1

    def run(self, test_only: bool = False, retry_only: bool = False) -> int:
        """Run high-speed scraper across all target states. Returns exit code."""
        self.load_checkpoint()
        self.reconcile_with_ledger()

        alphabet = list(string.ascii_lowercase)
        base_combinations = [(last, first) for last in alphabet for first in alphabet]
        if test_only:
            base_combinations = base_combinations[:3]

        total_states = len(self.target_states)

        print("=" * 78)
        print(f"  NAIC / SBS High-Speed Multi-State Scraper (Parallel Engine)")
        print(f"  Target States:     {total_states} states")
        if self.proxy_mgr and self.proxy_mgr.total_count() > 0:
            hosts = self.proxy_mgr.distinct_hosts()
            host_note = f" / {hosts} exit IPs" if hosts != self.proxy_mgr.total_count() else ""
            print(f"  Proxy Pool:        {self.proxy_mgr.total_count()} entries{host_note} "
                  f"({self.proxy_mgr.active_count()} active)")
        else:
            print(f"  Proxy Pool:        Direct Connection (No proxies)")
        print(f"  Workers:           {self.max_workers} concurrent threads")
        print(f"  Retry Rounds:      {self.retry_rounds} (after the main pass)")
        print(f"  Per-State CSV Dir: {os.path.abspath(self.output_dir)}/")
        print(f"  Master CSV File:   {os.path.abspath(self.master_output)}")
        print(f"  Checkpoint File:   {os.path.abspath(self.checkpoint_file)}")
        print(f"  Failure Queue:     {os.path.abspath(self.failure_log)} "
              f"({self.failure_ledger.count()} outstanding)")
        print("=" * 78)

        exit_code = 0
        interrupted = False
        try:
            for idx, state_code in enumerate(self.target_states, 1):
                state_name = SBS_JURISDICTIONS.get(state_code, state_code)
                pending_pairs = sorted(
                    (c.split(":", 1)[0], c.split(":", 1)[1])
                    for c in self.failed_combos.get(state_code, set())
                )
                grid_done = state_code in self.grid_completed

                if retry_only:
                    if not pending_pairs:
                        print(f"[{idx}/{total_states}] {state_name} ({state_code}): "
                              f"nothing queued - skipping.")
                        continue
                    if not grid_done:
                        print(f"[!] {state_name} ({state_code}) was never fully scanned, so the "
                              f"queue alone does not cover it. Run without --retry-failed "
                              f"(or --jurisdiction {state_code}) to complete the grid.")
                    combos_to_run = pending_pairs
                elif grid_done:
                    if pending_pairs:
                        # The rest of the grid already returned data and is safely in
                        # states/<ST>.csv, so only the outstanding combos are re-fired.
                        print(f"[{idx}/{total_states}] {state_name} ({state_code}): "
                              f"{len(pending_pairs)} outstanding combo(s) queued - "
                              f"retrying only those.")
                        combos_to_run = pending_pairs
                    else:
                        print(f"[{idx}/{total_states}] Skipping {state_name} ({state_code}) - "
                              f"fully covered.")
                        continue
                else:
                    combos_to_run = base_combinations

                self.scrape_state(state_code, combos_to_run)

            # Once all requested states are scraped, compile into master CSV
            self.merge_all_state_files()

        except KeyboardInterrupt:
            interrupted = True
            print("\n\n[!] Interrupted by user (Ctrl+C). Progress saved safely.")
            print(f"    Individual state CSVs in '{self.output_dir}/' are preserved.")
            print(f"    Outstanding failures are queued in '{self.failure_log}'.")
            print("    Resume anytime with: python3 naic_scraper_fast.py --resume")

        exit_code = self.print_gap_report()
        return 1 if interrupted else exit_code


def main():
    parser = argparse.ArgumentParser(description="NAIC High-Speed Multi-State Parallel Scraper")
    parser.add_argument("--jurisdiction", "--state", help="Single state to scrape (e.g. NJ)")
    parser.add_argument("--states", help="Comma-separated list of states to scrape (e.g. NJ,DE,MD,NC)")
    parser.add_argument("--all-states", action="store_true", help="Scrape all 34 SBS states/territories")
    parser.add_argument("--workers", type=int, default=8, help="Number of concurrent workers (default: 8, optimal: 8-10 without proxy, 20-40 with proxy)")
    parser.add_argument("--proxy", help="Single proxy URL (e.g. http://user:pass@host:port or host:port:user:pass)")
    parser.add_argument("--proxy-file", help="Path to file containing proxies, one per line (defaults to proxies.txt if found)")
    parser.add_argument("--states-dir", default="states", help="Directory to save individual state CSV files (default: states)")
    parser.add_argument("--master-output", default="all_states_producers.csv", help="Master merged CSV path (default: all_states_producers.csv)")
    parser.add_argument("--checkpoint", default="states_checkpoint.json", help="Checkpoint file path (default: states_checkpoint.json)")
    parser.add_argument("--test", action="store_true", help="Quick test flag (3 combos per state)")
    parser.add_argument("--resume", action="store_true", help="Resume from existing checkpoint (always on; flag is accepted for clarity)")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Retry ONLY the combos listed in the failure queue, then re-merge")
    parser.add_argument("--retry-rounds", type=int, default=3,
                        help="Retry passes over still-failing queries before parking them "
                             "in the queue (default: 3)")
    parser.add_argument("--failure-log", default="failed_queries.csv",
                        help="Durable queue of outstanding failed queries (default: failed_queries.csv)")
    parser.add_argument("--merge-only", action="store_true", help="Only merge existing state CSVs into the master file without scraping")

    args = parser.parse_args()

    # Configure ProxyManager
    proxy_source = None
    if args.proxy:
        proxy_source = args.proxy
    elif args.proxy_file:
        proxy_source = args.proxy_file
    elif os.path.exists("proxies.txt"):
        proxy_source = "proxies.txt"

    proxy_mgr = ProxyManager(proxy_source) if proxy_source else None

    if args.merge_only:
        scraper = FastParallelScraper(
            target_states=list(SBS_JURISDICTIONS.keys()),
            output_dir=args.states_dir,
            master_output=args.master_output,
            checkpoint_file=args.checkpoint,
            proxy_manager=proxy_mgr,
        )
        scraper.merge_all_state_files()
        sys.exit(0)

    # The failure queue is the source of truth for what still needs fetching, so
    # it is read up front to decide which states a retry run has to touch.
    ledger_preview = FailureLedger(args.failure_log)
    queued_states = ledger_preview.jurisdictions()

    if args.retry_failed:
        checkpoint_states: List[str] = []
        if os.path.exists(args.checkpoint):
            try:
                with open(args.checkpoint, "r") as f:
                    checkpoint_states = list(json.load(f).get("failed_combos", {}).keys())
            except Exception:
                checkpoint_states = []
        target_states = sorted(set(queued_states) | set(checkpoint_states))

        if args.states:
            wanted = {s.strip().upper() for s in args.states.split(",") if s.strip()}
            target_states = [s for s in target_states if s in wanted]

        if not target_states:
            print("[OK] The failure queue is empty - nothing to retry.")
            print(f"     Queue file: {os.path.abspath(args.failure_log)}")
            sys.exit(0)
        print(f"[*] Retrying {ledger_preview.count()} outstanding query/combo(s) "
              f"across {len(target_states)} jurisdiction(s).")
    elif args.all_states:
        target_states = list(SBS_JURISDICTIONS.keys())
    elif args.states:
        target_states = [s.strip().upper() for s in args.states.split(",") if s.strip()]
    elif args.jurisdiction:
        target_states = [args.jurisdiction.strip().upper()]
    else:
        target_states = ["NJ"]

    # Concurrency auto-tuning. Rate limits apply per exit IP, several entries often
    # share one host on different ports, and measured throttling kicks in hard, so
    # scale conservatively at ~2 workers per exit IP. Override with --workers.
    workers = args.workers
    if proxy_mgr and proxy_mgr.total_count() > 0:
        hosts = proxy_mgr.distinct_hosts()
        if args.workers == 8:
            workers = min(40, max(8, hosts * 2))
            print(f"[*] Proxy Pool active ({proxy_mgr.total_count()} entries across "
                  f"{hosts} exit IPs): auto-scaled workers to {workers} "
                  f"(~{workers // max(1, hosts)} per IP). Override with --workers.")
        else:
            workers = args.workers

    scraper = FastParallelScraper(
        target_states=target_states,
        output_dir=args.states_dir,
        master_output=args.master_output,
        checkpoint_file=args.checkpoint,
        max_workers=workers,
        proxy_manager=proxy_mgr,
        retry_rounds=args.retry_rounds,
        failure_log=args.failure_log,
    )

    sys.exit(scraper.run(test_only=args.test, retry_only=args.retry_failed))


if __name__ == "__main__":
    main()
