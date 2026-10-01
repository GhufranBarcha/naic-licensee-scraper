#!/usr/bin/env python3
"""
NAIC / SBS Multi-State Insurance Producer Licensee Scraper
---------------------------------------------------------
Automates scraping insurance licensee records from the National Association of
Insurance Commissioners (NAIC) State Based Systems (SBS) API across all 34
participating states and territories.

Runs single-alphabet combinations:
  Last Name [a..z] x First Name [a..z] = exactly 676 queries per state.

Features:
  - Bulletproof Retry Engine with exponential backoff (2s, 4s, 8s, 16s, 32s...).
  - Never marks failed queries as completed: failed queries are tracked in `failed_combos`.
  - Automatic end-of-run retry pass over all failed combinations.
  - Dedicated `--retry-failed` mode to re-run only failed queries when network improves.
  - Specific combo targeting via `--combos a:d,b:c` or `--last-names S --first-names M`.
  - Direct links generated for every record (Name Summary Link & License Manager Link).
  - Deduplication across runs and states by (jurisdiction, license_number).
  - Multi-state support: run 1 state, specific states, or `--all-states`.

Discovered API Endpoint:
https://api.prod.naic.org/sbs-external-lookup-api/licenseLookup/search
"""

import os
import sys
import time
import json
import csv
import string
import argparse
import urllib.parse
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
    """
    def __init__(self, proxy_source: Optional[Union[str, List[str]]] = None):
        self.raw_proxies: List[str] = []
        self._index = 0
        self.failed_proxies: Set[str] = set()

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

    def get_requests_proxies(self) -> Optional[Dict[str, str]]:
        """Return requests-compatible proxies dict {'http': url, 'https': url} via round-robin."""
        if not self.raw_proxies:
            return None
        active = [p for p in self.raw_proxies if p not in self.failed_proxies]
        if not active:
            self.failed_proxies.clear()
            active = self.raw_proxies
        proxy_url = active[self._index % len(active)]
        self._index += 1
        return {"http": proxy_url, "https": proxy_url}

    def mark_failed(self, proxy_url: str):
        """Mark a proxy as temporarily failing to avoid using it for subsequent queries."""
        self.failed_proxies.add(proxy_url)

    def total_count(self) -> int:
        return len(self.raw_proxies)

    def active_count(self) -> int:
        return len([p for p in self.raw_proxies if p not in self.failed_proxies])


def resolve_jurisdiction_codes(
    state_code: str,
    session: requests.Session,
    proxy_mgr: Optional[ProxyManager] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Dynamically queries the state's license types metadata to find exact codes for:
      - Producer License Type (e.g. 'PRO', '01' in DE, 'INT' in WI)
      - Life Line of Authority (e.g. 'L', '1' in DE/NC, 'LIFE' in AZ, '16' in IL/MD/OR/WV, 'LI' in CT/MA/NE/SD/VT/WI)
    """
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

    # 1. Match Producer license type
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

    # 2. Match Life LOA code within this license type
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


class MultiStateScraper:
    def __init__(
        self,
        target_states: List[str],
        output_file: str = "all_states_producers.csv",
        checkpoint_file: str = "naic_checkpoint.json",
        delay: float = 0.35,
        max_retries: int = 5,
        proxy_manager: Optional[ProxyManager] = None,
    ):
        self.target_states = [s.upper().strip() for s in target_states if s.strip()]
        self.output_file = output_file
        self.checkpoint_file = checkpoint_file
        self.delay = delay
        self.max_retries = max_retries
        self.proxy_mgr = proxy_manager

        if self.proxy_mgr and self.proxy_mgr.total_count() > 0:
            self.delay = max(0.05, self.delay / 2)

        self.session = requests.Session()
        self.seen_licenses: Set[Tuple[str, str]] = set()  # (jurisdiction, license_number)
        self.completed_combos: Dict[str, Set[str]] = {}  # state -> set of "last:first"
        self.failed_combos: Dict[str, Set[str]] = {}  # state -> set of "last:first" that failed
        self.completed_states: Set[str] = set()
        self.total_saved = 0
        self.consecutive_failures = 0
        self.csv_file = None
        self.csv_writer = None

    def load_checkpoint(self) -> None:
        """Load checkpoint file and existing CSV records to resume seamlessly without losing anything."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.completed_states = set(data.get("completed_states", []))
                    raw_combos = data.get("completed_combos", {})
                    self.completed_combos = {st: set(combos) for st, combos in raw_combos.items()}
                    raw_failed = data.get("failed_combos", {})
                    self.failed_combos = {st: set(combos) for st, combos in raw_failed.items()}

                    total_done = sum(len(c) for c in self.completed_combos.values())
                    total_fail = sum(len(f_list) for f_list in self.failed_combos.values())
                    print(f"[*] Resumed checkpoint: {len(self.completed_states)} states completed | {total_done} combos done | {total_fail} combos flagged as failed.")
            except Exception as e:
                print(f"[!] Warning: Could not read checkpoint file ({e}).")

        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        lic = str(row.get("license_number", "")).strip()
                        jur = str(row.get("jurisdiction", "")).strip()
                        if lic and jur:
                            self.seen_licenses.add((jur, lic))
                self.total_saved = len(self.seen_licenses)
                print(f"[*] Loaded {self.total_saved} unique licensees from existing file '{self.output_file}'.")
            except Exception as e:
                print(f"[!] Warning: Could not read existing output file ({e}).")

    def save_checkpoint(self) -> None:
        """Save progress to checkpoint JSON."""
        try:
            with open(self.checkpoint_file, "w", encoding="utf-8") as f:
                json.dump({
                    "completed_states": sorted(list(self.completed_states)),
                    "completed_combos": {st: sorted(list(combos)) for st, combos in self.completed_combos.items()},
                    "failed_combos": {st: sorted(list(combos)) for st, combos in self.failed_combos.items() if combos},
                    "total_unique_saved": len(self.seen_licenses),
                    "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
                }, f, indent=2)
        except Exception as e:
            print(f"[!] Warning: Could not save checkpoint ({e}).")

    def open_csv(self) -> None:
        """Open CSV file for appending and write headers if new file."""
        file_exists = os.path.exists(self.output_file) and os.path.getsize(self.output_file) > 0
        self.csv_file = open(self.output_file, "a", newline="", encoding="utf-8-sig")
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            self.csv_writer.writeheader()
            self.csv_file.flush()

    def close_csv(self) -> None:
        """Safely flush and close CSV file."""
        if self.csv_file:
            self.csv_file.flush()
            self.csv_file.close()

    def fetch_records(
        self,
        jurisdiction: str,
        pro_code: Optional[str],
        life_code: Optional[str],
        last_letter: str,
        first_letter: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch search results for a state with exponential backoff retry and proxy rotation.
        Returns:
          - list of records (can be empty [] if 0 people exist) on SUCCESS
          - None if the query FAILED after all retries (so we never falsely mark it as completed)
        """
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

        for attempt in range(1, self.max_retries + 1):
            proxy_dict = self.proxy_mgr.get_requests_proxies() if self.proxy_mgr else None
            proxy_url = proxy_dict.get("http") if proxy_dict else None
            try:
                time.sleep(self.delay)
                resp = self.session.get(API_URL, params=params, headers=HEADERS, proxies=proxy_dict, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    self.consecutive_failures = 0
                    return data if isinstance(data, list) else []
                elif resp.status_code in (429, 500, 502, 503, 504):
                    sleep_time = min(30.0, 2.0 ** attempt)
                    print(f"    [!] HTTP {resp.status_code} on {jurisdiction} ({last_letter.upper()}:{first_letter.upper()}). Retrying in {sleep_time:.1f}s (attempt {attempt}/{self.max_retries})...")
                    time.sleep(sleep_time)
                else:
                    print(f"    [!] Unexpected HTTP {resp.status_code} on {jurisdiction}: {resp.text[:120]}")
                    return None
            except (requests.exceptions.ProxyError, requests.exceptions.ConnectTimeout):
                if self.proxy_mgr and proxy_url:
                    self.proxy_mgr.mark_failed(proxy_url)
                continue
            except requests.RequestException as e:
                sleep_time = min(30.0, 2.0 ** attempt)
                print(f"    [!] Network error on {jurisdiction} ({last_letter.upper()}:{first_letter.upper()}): {e}. Retrying in {sleep_time:.1f}s (attempt {attempt}/{self.max_retries})...")
                time.sleep(sleep_time)

        # If all retries failed:
        self.consecutive_failures += 1
        if self.consecutive_failures >= 3:
            print("\n  [⚠️ WARNING] Multiple queries failed consecutively. Your internet connection may be unstable.")
            print("  Pausing 10s before proceeding to give network time to recover...\n")
            time.sleep(10)

        return None

    def execute_query_combo(
        self,
        state_code: str,
        pro_code: Optional[str],
        life_code: Optional[str],
        last_ch: str,
        first_ch: str,
        state_done_combos: Set[str],
        state_failed_combos: Set[str],
        progress_prefix: str = "",
    ) -> bool:
        """Execute a single query combination, write data, and record status."""
        combo_key = f"{last_ch}:{first_ch}"
        raw_records = self.fetch_records(state_code, pro_code, life_code, last_ch, first_ch)

        if raw_records is None:
            # FAILED query: do NOT mark as completed! Track in failed_combos.
            state_failed_combos.add(combo_key)
            self.save_checkpoint()
            print(f"  {progress_prefix} Last='{last_ch.upper()}', First='{first_ch.upper()}' -> [FAILED ❌] Logged to retry queue.")
            return False

        # Query succeeded! Remove from failed if it was previously failed
        state_failed_combos.discard(combo_key)

        new_count = 0
        for item in raw_records:
            lic = str(item.get("licenseNumber") or "").strip()
            key = (state_code, lic)
            if not lic or key in self.seen_licenses:
                continue
            self.seen_licenses.add(key)
            row = process_record(item, state_code)
            self.csv_writer.writerow(row)
            new_count += 1
            self.total_saved += 1

        if new_count > 0:
            self.csv_file.flush()

        state_done_combos.add(combo_key)
        self.save_checkpoint()

        hit_count = len(raw_records)
        print(f"  {progress_prefix} Last='{last_ch.upper()}', First='{first_ch.upper()}' -> Hits: {hit_count:2d} | +{new_count:2d} new | All States: {self.total_saved:6d}")
        return True

    def run(
        self,
        test_only: bool = False,
        retry_only: bool = False,
        custom_combos: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        """Run multi-state automation through requested states."""
        self.load_checkpoint()
        self.open_csv()

        alphabet = list(string.ascii_lowercase)
        base_combinations = [(last, first) for last in alphabet for first in alphabet]

        if custom_combos:
            base_combinations = custom_combos
        elif test_only:
            base_combinations = base_combinations[:3]

        total_states = len(self.target_states)

        print("=" * 78)
        print(f"  NAIC / SBS Multi-State Automation Scraper")
        print(f"  Mode:              {'RETRY FAILED COMBOS ONLY' if retry_only else 'FULL SCRAPE'}")
        print(f"  States to process: {total_states} states")
        if self.proxy_mgr and self.proxy_mgr.total_count() > 0:
            print(f"  Proxy Pool:        {self.proxy_mgr.total_count()} rotating proxies ({self.proxy_mgr.active_count()} active)")
        else:
            print(f"  Proxy Pool:        Direct Connection (No proxies)")
        print(f"  Max Combos/State:  {len(base_combinations)} queries")
        print(f"  Output CSV:        {self.output_file}")
        print(f"  Checkpoint File:   {self.checkpoint_file}")
        print("=" * 78)

        try:
            for state_idx, state_code in enumerate(self.target_states, 1):
                state_name = SBS_JURISDICTIONS.get(state_code, state_code)

                # Initialize state sets
                if state_code not in self.completed_combos:
                    self.completed_combos[state_code] = set()
                if state_code not in self.failed_combos:
                    self.failed_combos[state_code] = set()

                state_done_combos = self.completed_combos[state_code]
                state_failed_combos = self.failed_combos[state_code]

                if retry_only:
                    if not state_failed_combos:
                        print(f"\n[{state_idx}/{total_states}] {state_name} ({state_code}): No failed combos to retry.")
                        continue
                    combos_to_run = [(c.split(":")[0], c.split(":")[1]) for c in list(state_failed_combos)]
                else:
                    if state_code in self.completed_states and not state_failed_combos:
                        print(f"\n[{state_idx}/{total_states}] Skipping {state_name} ({state_code}) - Already Completed.")
                        continue
                    combos_to_run = [c for c in base_combinations if f"{c[0]}:{c[1]}" not in state_done_combos]

                print(f"\n{'='*78}")
                print(f"  [{state_idx}/{total_states}] State: {state_name.upper()} ({state_code})")
                print(f"  Resolving statutory codes for {state_name}...")

                pro_code, life_code = resolve_jurisdiction_codes(state_code, self.session, self.proxy_mgr)
                print(f"  Codes resolved: Producer = '{pro_code}', Life LOA = '{life_code}'")
                print(f"  Queue: {len(combos_to_run)} queries to run ({len(state_done_combos)} already completed, {len(state_failed_combos)} failed)")
                print(f"{'='*78}")

                state_start_saved = self.total_saved

                for c_idx, (last_ch, first_ch) in enumerate(combos_to_run, 1):
                    progress_tag = f"[{state_code}] [{c_idx:3d}/{len(combos_to_run)}]"
                    self.execute_query_combo(
                        state_code,
                        pro_code,
                        life_code,
                        last_ch,
                        first_ch,
                        state_done_combos,
                        state_failed_combos,
                        progress_prefix=progress_tag,
                    )

                # End-of-state automated retry pass for any failed combos in this state:
                if state_failed_combos and not retry_only:
                    print(f"\n  [🔄 RETRY PASS] Attempting to retry {len(state_failed_combos)} failed combos for {state_name}...")
                    retry_list = list(state_failed_combos)
                    for r_idx, combo_str in enumerate(retry_list, 1):
                        last_ch, first_ch = combo_str.split(":", 1)
                        progress_tag = f"[{state_code}-RETRY] [{r_idx}/{len(retry_list)}]"
                        self.execute_query_combo(
                            state_code,
                            pro_code,
                            life_code,
                            last_ch,
                            first_ch,
                            state_done_combos,
                            state_failed_combos,
                            progress_prefix=progress_tag,
                        )

                # If no failed combos remain, declare state 100% complete
                if not state_failed_combos and len(state_done_combos) >= len(base_combinations):
                    self.completed_states.add(state_code)
                    self.save_checkpoint()
                    print(f"\n[✓] Completed State: {state_name} ({state_code}) with 100% data completeness! State records added: {self.total_saved - state_start_saved}")
                elif state_failed_combos:
                    print(f"\n[⚠️] State {state_name} ({state_code}) finished with {len(state_failed_combos)} uncompleted queries due to network drops.")
                    print(f"    You can re-run them later using: python3 naic_scraper.py --retry-failed")

        except KeyboardInterrupt:
            print("\n\n[!] Interrupted by user (Ctrl+C). Saving progress safely...")
        finally:
            self.save_checkpoint()
            self.close_csv()
            total_failed_all = sum(len(f_list) for f_list in self.failed_combos.values())
            print("\n" + "=" * 78)
            print(f"[✓] Automation Finished / Paused.")
            print(f"    States Fully Completed:      {len(self.completed_states)} / {total_states}")
            print(f"    Total Unique Licensees Saved: {self.total_saved}")
            print(f"    Queries Flagged as Failed:   {total_failed_all} (Re-run anytime with --retry-failed)")
            print(f"    CSV Output File:             {os.path.abspath(self.output_file)}")
            print(f"    Checkpoint File:             {os.path.abspath(self.checkpoint_file)}")
            print("=" * 78)


def parse_custom_combos(combo_arg: Optional[str]) -> Optional[List[Tuple[str, str]]]:
    """Parse combos like 'a:a,a:b,s:m' into list of tuples."""
    if not combo_arg:
        return None
    combos = []
    for item in combo_arg.split(","):
        item = item.strip().lower()
        if ":" in item:
            parts = item.split(":", 1)
            combos.append((parts[0].strip(), parts[1].strip()))
    return combos if combos else None


def main():
    parser = argparse.ArgumentParser(description="NAIC Multi-State Single-Alphabet Producer Scraper")
    parser.add_argument("--jurisdiction", "--state", help="Single state to scrape (e.g. NJ)")
    parser.add_argument("--states", help="Comma-separated list of states to scrape (e.g. NJ,DE,MD,NC)")
    parser.add_argument("--all-states", action="store_true", help="Scrape all 34 SBS states/territories one by one")
    parser.add_argument("--delay", type=float, default=0.35, help="Delay between API requests in seconds (default: 0.35)")
    parser.add_argument("--proxy", help="Single proxy URL (e.g. http://user:pass@host:port or host:port:user:pass)")
    parser.add_argument("--proxy-file", help="Path to file containing proxies, one per line (defaults to proxies.txt if found)")
    parser.add_argument("--max-retries", type=int, default=5, help="Max retry attempts for failed requests (default: 5)")
    parser.add_argument("--output", default="all_states_producers.csv", help="Output CSV path (default: all_states_producers.csv)")
    parser.add_argument("--checkpoint", default="naic_checkpoint.json", help="Checkpoint file path (default: naic_checkpoint.json)")
    parser.add_argument("--test", action="store_true", help="Quick test flag (runs 3 combos per state)")
    parser.add_argument("--resume", action="store_true", help="Resume from existing checkpoint")
    parser.add_argument("--retry-failed", action="store_true", help="Re-run only queries that previously failed due to network issues")
    parser.add_argument("--combos", help="Run specific letter combinations, e.g. 'a:a,a:d,s:m'")

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

    # Determine states to target
    if args.retry_failed:
        # Load states that have failed combos from checkpoint
        if os.path.exists(args.checkpoint):
            try:
                with open(args.checkpoint, "r") as f:
                    ckpt = json.load(f)
                    states_with_failures = list(ckpt.get("failed_combos", {}).keys())
                    target_states = states_with_failures if states_with_failures else list(SBS_JURISDICTIONS.keys())
            except Exception:
                target_states = list(SBS_JURISDICTIONS.keys())
        else:
            print("[!] No checkpoint file found to retry failed queries.")
            sys.exit(1)
    elif args.all_states:
        target_states = list(SBS_JURISDICTIONS.keys())
    elif args.states:
        target_states = [s.strip().upper() for s in args.states.split(",") if s.strip()]
    elif args.jurisdiction:
        target_states = [args.jurisdiction.strip().upper()]
    else:
        target_states = ["NJ"]

    custom_combos = parse_custom_combos(args.combos)

    scraper = MultiStateScraper(
        target_states=target_states,
        output_file=args.output,
        checkpoint_file=args.checkpoint,
        delay=args.delay,
        max_retries=args.max_retries,
        proxy_manager=proxy_mgr,
    )

    scraper.run(
        test_only=args.test,
        retry_only=args.retry_failed,
        custom_combos=custom_combos,
    )


if __name__ == "__main__":
    main()
