"""
Discogs listing DRY RUN (read-only).

Shows which Notion records WOULD be listed on the Discogs marketplace, and why
any records are blocked. This script contains NO code that writes to Notion or
creates anything on Discogs. It only reads.

A record is a candidate when:
  Status = Clean, Keep is unticked, and Marketplace ID is empty.
"""

import os
import time
import requests
from requests.exceptions import RequestException

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-listing-dryrun/1.0",
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ---------------------------------------------------
# CONFIG (edit these to suit)
# ---------------------------------------------------

MAX_PREVIEW = int(os.environ.get("MAX_PREVIEW", "25"))  # how many payloads to print in full

# Exact strings the Discogs listing API accepts
ALLOWED_CONDITIONS = [
    "Mint (M)",
    "Near Mint (NM or M-)",
    "Very Good Plus (VG+)",
    "Very Good (VG)",
    "Good Plus (G+)",
    "Good (G)",
    "Fair (F)",
    "Poor (P)",
]
ALLOWED_SLEEVE = ALLOWED_CONDITIONS + ["Generic", "Not Graded", "No Cover"]

# ESTIMATED weight in grams of ONE disc in its sleeve, excluding packaging.
# Please check and adjust these. Packaging is best added in your Discogs shipping policies.
WEIGHT_G_PER_DISC = {'7"': 60, '10"': 140, '12"': 250}

# Optional standard text added after the public Notes in the listing comments.
COMMENT_FOOTER = ""

REQUIRED_PROPERTIES = [
    "Title", "Artist", "Status", "Keep", "Marketplace ID", "Asking Price",
    "Media Condition", "Sleeve Condition", "Notes", "Instance ID",
    "Discogs ID", "FormatSize",
]

# ---------------------------------------------------
# REQUEST HELPERS
# ---------------------------------------------------

def notion_request(method, url, payload=None, retries=5):
    for attempt in range(retries):
        try:
            r = requests.request(method, url, headers=headers_notion, json=payload, timeout=30)

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 2))
                print(f"NOTION RATE LIMITED - sleeping {retry}s")
                time.sleep(retry)
                continue

            if r.status_code >= 500:
                wait = 2 ** attempt
                print(f"NOTION SERVER ERROR {r.status_code} - retrying in {wait}s")
                time.sleep(wait)
                continue

            if not r.ok:
                print(f"NOTION ERROR {r.status_code}: {r.text}")
                return None

            return r

        except RequestException as e:
            wait = 2 ** attempt
            print(f"NOTION REQUEST EXCEPTION: {e} - retrying in {wait}s")
            time.sleep(wait)

    print(f"NOTION REQUEST FAILED AFTER {retries} ATTEMPTS: {url}")
    return None


def discogs_get(url, retries=5):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers_discogs, timeout=30)
        except RequestException as e:
            wait = 2 ** attempt
            print(f"DISCOGS REQUEST EXCEPTION: {e} - retrying in {wait}s")
            time.sleep(wait)
            continue

        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 30))
            print(f"DISCOGS RATE LIMITED - sleeping {retry}s")
            time.sleep(retry)
            continue

        if r.status_code >= 500:
            wait = 2 ** attempt
            print(f"DISCOGS SERVER ERROR {r.status_code} - retrying in {wait}s")
            time.sleep(wait)
            continue

        if not r.ok:
            print(f"DISCOGS ERROR {r.status_code} for {url}")
            return None

        # Throttle only when close to the limit
        remaining = r.headers.get("X-Discogs-Ratelimit-Remaining")
        if remaining is not None and int(remaining) <= 2:
            time.sleep(10)

        return r

    return None

# ---------------------------------------------------
# NOTION HELPERS
# ---------------------------------------------------

def plain(rich_text):
    return "".join(part.get("plain_text", "") for part in (rich_text or []))


def get_select(prop):
    if not prop:
        return None
    value = prop.get("select")
    return value["name"] if value else None


def fetch_schema():
    r = notion_request("GET", f"{NOTION_BASE}/databases/{DATABASE_ID}")
    if not r:
        raise Exception("Could not read Notion database schema")
    return r.json().get("properties", {})


def fetch_clean_pages(status_type):
    pages = []
    has_more = True
    cursor = None

    while has_more:
        payload = {
            "page_size": 100,
            "filter": {"property": "Status", status_type: {"equals": "Clean"}},
        }
        if cursor:
            payload["start_cursor"] = cursor

        r = notion_request("POST", f"{NOTION_BASE}/databases/{DATABASE_ID}/query", payload)
        if not r:
            raise Exception("Failed fetching Notion pages")

        data = r.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more")
        cursor = data.get("next_cursor")

    return pages

# ---------------------------------------------------
# DISCOGS HELPERS
# ---------------------------------------------------

_release_cache = {}


def get_format_qty(release_id):
    """Number of discs, used for Discogs 'counts as' (shipping). Defaults to 1."""
    if release_id in _release_cache:
        return _release_cache[release_id]

    qty = 1
    r = discogs_get(f"{DISCOGS_BASE}/releases/{release_id}")
    if r:
        formats = r.json().get("formats") or []
        if formats:
            try:
                qty = max(1, int(formats[0].get("qty", 1)))
            except (TypeError, ValueError):
                qty = 1

    _release_cache[release_id] = qty
    return qty

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    print("=" * 60)
    print("DISCOGS LISTING DRY RUN - read-only, nothing will be created")
    print("=" * 60)

    schema = fetch_schema()

    missing = [p for p in REQUIRED_PROPERTIES if p not in schema]
    if missing:
        print("\nSTOPPING: these Notion properties were not found:")
        for p in missing:
            print(f"  - {p}")
        print("Please create/rename them (exact names) and run again.")
        raise SystemExit(1)

    status_type = schema["Status"]["type"]
    if status_type not in ("select", "status"):
        print(f"STOPPING: Status is type '{status_type}', expected select or status.")
        raise SystemExit(1)

    pages = fetch_clean_pages(status_type)
    print(f"\nRecords with Status = Clean: {len(pages)}")

    keepers = already_linked = 0
    ready = []
    blocked = []

    for page in pages:
        props = page["properties"]

        title = plain(props["Title"].get("title"))
        artist = plain(props["Artist"].get("rich_text"))
        label = f"{artist} - {title}".strip(" -") or page["id"]

        if props["Keep"].get("checkbox"):
            keepers += 1
            continue

        if props["Marketplace ID"].get("number") is not None:
            already_linked += 1
            continue

        problems = []

        instance_id = props["Instance ID"].get("number")
        release_id = props["Discogs ID"].get("number")
        if instance_id is None:
            problems.append("missing Instance ID")
        if release_id is None:
            problems.append("missing Discogs ID")

        condition = get_select(props["Media Condition"])
        if condition not in ALLOWED_CONDITIONS:
            problems.append(f"Media Condition not usable: {condition!r}")

        sleeve = get_select(props["Sleeve Condition"]) or "Not Graded"
        if sleeve not in ALLOWED_SLEEVE:
            problems.append(f"Sleeve Condition not usable: {sleeve!r}")

        price_raw = props["Asking Price"].get("formula", {}).get("number")
        if price_raw is None or price_raw <= 0:
            problems.append(f"Asking Price missing or zero: {price_raw!r}")

        if problems:
            blocked.append((label, problems))
            continue

        notes = plain(props["Notes"].get("rich_text")).strip()
        comments = "\n\n".join(x for x in (notes, COMMENT_FOOTER) if x)

        size = get_select(props["FormatSize"])
        qty = get_format_qty(release_id)
        per_disc = WEIGHT_G_PER_DISC.get(size)
        weight = per_disc * qty if per_disc else None

        warnings = []
        if weight is None:
            warnings.append(f"no weight estimate for FormatSize {size!r}")

        ready.append({
            "label": label,
            "warnings": warnings,
            "payload": {
                "release_id": release_id,
                "condition": condition,
                "sleeve_condition": sleeve,
                "price": round(float(price_raw), 2),
                "comments": comments,
                "status": "For Sale",
                "external_id": str(int(instance_id)),
                "weight": weight,
                "format_quantity": qty,
            },
        })

    print(f"Skipped (Keep ticked): {keepers}")
    print(f"Skipped (already has Marketplace ID): {already_linked}")
    print(f"READY to list: {len(ready)}")
    print(f"BLOCKED: {len(blocked)}")

    if ready:
        total = sum(item["payload"]["price"] for item in ready)
        print(f"Total asking price of ready records: GBP {total:,.2f}")

    if blocked:
        print("\n--- BLOCKED (would not be listed) ---")
        for label, problems in blocked:
            print(f"* {label}")
            for p in problems:
                print(f"    - {p}")

    if ready:
        print(f"\n--- READY (showing up to {MAX_PREVIEW}) ---")
        for item in ready[:MAX_PREVIEW]:
            pl = item["payload"]
            print(f"* {item['label']}")
            print(f"    release {pl['release_id']} | {pl['condition']} / sleeve {pl['sleeve_condition']}"
                  f" | GBP {pl['price']:.2f} | weight {pl['weight']}g | counts as {pl['format_quantity']}")
            print(f"    external_id (Instance ID): {pl['external_id']}")
            if pl["comments"]:
                print(f"    comments: {pl['comments'][:120]!r}")
            for w in item["warnings"]:
                print(f"    WARNING: {w}")

    print("\nDry run complete. Nothing was written to Notion or Discogs.")


if __name__ == "__main__":
    main()
