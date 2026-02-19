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
    "User-Agent": "discogs-notion-sync/3.3"
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

            # Throttle ~1.1/sec
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
    if not formats:
        return None, None, None

    size = None
    speed = None
    details = []

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
# DISCOGS HELPERS
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
    existing_options.add(new_value)

    payload = {
        "properties": {
            property_name: {
                "select": {
                    "options": [{"name": name} for name in existing_options]
                }
            }
        }
    }

    notion_request("PATCH", f"{NOTION_BASE}/databases/{DATABASE_ID}", payload)


def fetch_all_notion_pages():
    pages = {}
    has_more = True
    start_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        r = notion_request("POST", f"{NOTION_BASE}/databases/{DATABASE_ID}/query", payload)
        if not r:
            break

        data = r.json()

        for result in data.get("results", []):
            discogs_id = result["properties"]["Discogs ID"]["number"]
            if discogs_id:
                pages[discogs_id] = result

        has_more = data.get("has_more")
        start_cursor = data.get("next_cursor")

    return pages


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():

    print("Fetching Discogs collection...")
    collection = get_full_collection()
    print(f"Total releases identified in Discogs: {len(collection)}")

    folder_map = get_folder_map()
    field_map = get_collection_fields()
    notion_pages = fetch_all_notion_pages()

    folder_options = fetch_select_options("Folder")
    media_options = fetch_select_options("Media Condition")
    sleeve_options = fetch_select_options("Sleeve Condition")
    genre_options = fetch_select_options("Genre")
    style_options = fetch_select_options("Style")

    created = 0
    updated = 0

    for item in collection:
        try:
            release_id = item["basic_information"]["id"]
            folder_name = folder_map.get(item.get("folder_id"))
            date_added = item.get("date_added")

            # -----------------------------
            # CLEAN CONDITION SPLIT
            # -----------------------------
            media_condition = None
            sleeve_condition = None
            true_notes = []

            for n in item.get("notes", []):
                field_name = field_map.get(n.get("field_id"))
                value = n.get("value")

                if not value:
                    continue

                if field_name == "Media Condition":
                    media_condition = value.strip()
                elif field_name == "Sleeve Condition":
                    sleeve_condition = value.strip()
                else:
                    true_notes.append(value.strip())

            # IMPORTANT: Only non-condition notes stored
            notes = "\n".join(true_notes) if true_notes else None

            full_release = get_release_details(release_id)
            lowest, median, highest = get_market_stats(release_id)

            genres = full_release.get("genres") or []
            styles = full_release.get("styles") or []
            labels = full_release.get("labels") or []

            genre = genres[0] if genres else None
            style = styles[0] if styles else None
            catno = labels[0].get("catno") if labels else ""

            size, speed, details = parse_formats(full_release.get("formats"))

            artists = ", ".join(a["name"] for a in full_release.get("artists", []))
            label_names = ", ".join(l["name"] for l in labels)

            properties = {
                "Title": {"title": [{"text": {"content": full_release.get("title", "")}}]},
                "Artist": {"rich_text": [{"text": {"content": artists}}]},
                "Discogs ID": {"number": release_id},
                "Year": {"number": full_release.get("year")},
                "Country": {"rich_text": [{"text": {"content": full_release.get("country", "")}}]},
                "Label": {"rich_text": [{"text": {"content": label_names}}]},
                "CatNo": {"rich_text": [{"text": {"content": catno}}]},
                "FormatSize": {"rich_text": [{"text": {"content": size or ""}}]},
                "FormatSpeed": {"rich_text": [{"text": {"content": speed or ""}}]},
                "FormatDetails": {"rich_text": [{"text": {"content": details or ""}}]},
                "Folder": {"select": {"name": folder_name}} if folder_name else None,
                "Media Condition": {"select": {"name": media_condition}} if media_condition else None,
                "Sleeve Condition": {"select": {"name": sleeve_condition}} if sleeve_condition else None,
                "Genre": {"select": {"name": genre}} if genre else None,
                "Style": {"select": {"name": style}} if style else None,
                "Notes": {"rich_text": [{"text": {"content": notes}}]} if notes else None,
                "ValueLow": {"number": lowest},
                "ValueMed": {"number": median},
                "ValueHigh": {"number": highest},
            }

            properties = {k: v for k, v in properties.items() if v is not None}

            if release_id in notion_pages:
                page_id = notion_pages[release_id]["id"]
                notion_request("PATCH", f"{NOTION_BASE}/pages/{page_id}", {"properties": properties})
                updated += 1
            else:
                properties["Added"] = {"date": {"start": date_added}}
                notion_request(
                    "POST",
                    f"{NOTION_BASE}/pages",
                    {"parent": {"database_id": DATABASE_ID}, "properties": properties}
                )
                created += 1

        except Exception as e:
            print(f"[Release ERROR] ID {release_id}: {e}")

    print("Sync complete.")
    print(f"Created: {created}")
    print(f"Updated: {updated}")


if __name__ == "__main__":
    main()
