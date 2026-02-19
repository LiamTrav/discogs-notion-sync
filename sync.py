import os
import time
import re
import requests

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-sync/3.2"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ---------------------------------------------------
# RETRY HELPERS
# ---------------------------------------------------

def notion_request(method, url, payload=None, max_retries=5):
    for attempt in range(max_retries):
        try:
            if method == "GET":
                r = requests.get(url, headers=headers_notion)
            elif method == "POST":
                r = requests.post(url, headers=headers_notion, json=payload)
            elif method == "PATCH":
                r = requests.patch(url, headers=headers_notion, json=payload)
            else:
                raise ValueError("Unsupported method")

            if r.status_code >= 500 or r.status_code == 429:
                raise requests.exceptions.HTTPError(f"{r.status_code} error")

            r.raise_for_status()
            return r

        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            print(f"[Notion Retry] Attempt {attempt+1}/{max_retries} failed: {e}. Waiting {wait}s")
            time.sleep(wait)

    print("[Notion ERROR] Failed after retries.")
    return None


def discogs_request(url, max_retries=5):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers_discogs)

            if r.status_code >= 500 or r.status_code == 429:
                raise requests.exceptions.HTTPError(f"{r.status_code} error")

            r.raise_for_status()

            # Throttle to ~1.1 requests/sec
            time.sleep(1.1)

            return r

        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            print(f"[Discogs Retry] Attempt {attempt+1}/{max_retries} failed: {e}. Waiting {wait}s")
            time.sleep(wait)

    print(f"[Discogs ERROR] Failed after retries for URL: {url}")
    return None


# ---------------------------------------------------
# FORMAT PARSER
# ---------------------------------------------------

RPM_PATTERN = re.compile(r"\b(33\s?⅓|33\s?1/3|45|78)\s?RPM\b", re.IGNORECASE)
SIZE_PATTERN = re.compile(r'\b(7"|10"|12")')

def parse_formats(formats):
    size = None
    speed = None
    details = []

    if not formats:
        return None, None, None

    for fmt in formats:
        for desc in fmt.get("descriptions", []):
            if not size and SIZE_PATTERN.search(desc):
                size = desc
                continue

            if not speed and RPM_PATTERN.search(desc.replace("⅓", " 1/3")):
                speed = desc
                continue

            details.append(desc)

    return size, speed, ", ".join(details) if details else None


# ---------------------------------------------------
# DISCOGS
# ---------------------------------------------------

def get_folder_map():
    r = discogs_request(f"{DISCOGS_BASE}/users/{USERNAME}/collection/folders")
    if not r:
        return {}
    return {f["id"]: f["name"] for f in r.json().get("folders", [])}


def get_collection_fields():
    r = discogs_request(f"{DISCOGS_BASE}/users/{USERNAME}/collection/fields")
    if not r:
        return {}
    return {f["id"]: f["name"] for f in r.json().get("fields", [])}


def get_full_collection():
    releases = []
    page = 1

    while True:
        url = f"{DISCOGS_BASE}/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page=100"
        r = discogs_request(url)
        if not r:
            break

        data = r.json()
        releases.extend(data.get("releases", []))

        if page >= data.get("pagination", {}).get("pages", 1):
            break

        page += 1

    return releases


def get_release_details(release_id):
    r = discogs_request(f"{DISCOGS_BASE}/releases/{release_id}")
    return r.json() if r else {}


def get_market_stats(release_id):

    lowest = None
    median = None
    highest = None

    r_stats = discogs_request(f"{DISCOGS_BASE}/marketplace/stats/{release_id}")
    if r_stats:
        lowest_obj = r_stats.json().get("lowest_price")
        if lowest_obj:
            lowest = lowest_obj.get("value")

    r_price = discogs_request(f"{DISCOGS_BASE}/marketplace/price_suggestions/{release_id}")
    if r_price:
        price_data = r_price.json()

        vg_plus = price_data.get("Very Good Plus (VG+)")
        if vg_plus:
            median = vg_plus.get("value")

        mint = price_data.get("Mint (M)")
        if mint:
            highest = mint.get("value")

    return lowest, median, highest


# ---------------------------------------------------
# NOTION HELPERS
# ---------------------------------------------------

def fetch_select_options(property_name):
    r = notion_request("GET", f"{NOTION_BASE}/databases/{DATABASE_ID}")
    if not r:
        return set()

    db = r.json()
    return set(o["name"] for o in db["properties"][property_name]["select"]["options"])


def update_select_schema(property_name, new_value, existing_options):
    existing_options.ad_
