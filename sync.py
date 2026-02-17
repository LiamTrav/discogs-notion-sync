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
    "User-Agent": "discogs-notion-sync/2.1"
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
            return r

        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            print(f"[Discogs Retry] Attempt {attempt+1}/{max_retries} failed: {e}. Waiting {wait}s")
            time.sleep(wait)

    print(f"[Discogs ERROR] Failed after retries for URL: {url}")
    return None


# ---------------------------------------------------
# FORMAT PARSER (RESTORED + ROBUST)
# ---------------------------------------------------

RPM_PATTERN = re.compile(r"\b(33\s?⅓|33\s?1/3|45|78)\s?RPM\b", re.IGNORECASE)
SIZE_PATTERN = re.compile(r'(7"|10"|12")')

def parse_formats(formats):
    size = None
    speed = None
    details = []

    if not formats:
        return None, None, None

    for fmt in formats:
        for desc in fmt.get("descriptions", []):

            # Detect size (7", 10", 12")
            if not size and SIZE_PATTERN.search(desc):
                size = desc
                continue

            # Detect RPM (robust matching)
            if not speed and RPM_PATTERN.search(desc.replace("⅓", " 1/3")):
                speed = desc
                continue

            details.append(desc)

    details_text = ", ".join(details) if details else None
    return size, speed, details_text


# ---------------------------------------------------
# DISCOGS
# ---------------------------------------------------

def get_full_collection():
    releases = []
    page = 1
    per_page = 100

    while True:
        url = f"{DISCOGS_BASE}/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page={per_page}"
        r = discogs_request(url)
        if not r:
            break

        data = r.json()
        releases.extend(data["releases"])

        if page >= data["pagination"]["pages"]:
            break

        page += 1

    return releases


def get_release_details(release_id):
    url = f"{DISCOGS_BASE}/releases/{release_id}"
    r = discogs_request(url)
    if not r:
        return {}
    return r.json()


def get_market_stats(release_id):
    url = f"{DISCOGS_BASE}/marketplace/stats/{release_id}"
    r = discogs_request(url)
    if not r:
        return None, None, None

    data = r.json()
    lowest = data.get("lowest_price", {}).get("value") if data.get("lowest_price") else None
    median = data.get("median_price", {}).get("value") if data.get("median_price") else None
    highest = data.get("highest_price", {}).get("value") if data.get("highest_price") else None

    return lowest, median, highest


# ---------------------------------------------------
# NOTION
# ---------------------------------------------------

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

        for result in data["results"]:
            discogs_id = result["properties"]["Discogs ID"]["number"]
            if discogs_id:
                pages[discogs_id] = result

        has_more = data.get("has_more")
        start_cursor = data.get("next_cursor")

    return pages


def fetch_folder_options():
    r = notion_request("GET", f"{NOTION_BASE}/databases/{DATABASE_ID}")
    if not r:
        return set()

    db = r.json()
    options = db["properties"]["Folder"]["select"]["options"]
    return set(o["name"] for o in options)


def update_folder_schema(new_folder, existing_options):
    existing_options.add(new_folder)

    payload = {
        "properties": {
            "Folder": {
                "select": {
                    "options": [{"name": name} for name in existing_options]
                }
            }
        }
    }

    notion_request("PATCH", f"{NOTION_BASE}/databases/{DATABASE_ID}", payload)


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    print("Fetching Discogs collection...")
    collection = get_full_collection()
    print(f"Total releases identified in Discogs: {len(collection)}")

    print("Fetching existing Notion pages...")
    notion_pages = fetch_all_notion_pages()
    print(f"Existing pages in Notion: {len(notion_pages)}")

    folder_options = fetch_folder_options()

    created = 0
    updated = 0
    failed = 0

    for item in collection:
        try:
            release_id = item["basic_information"]["id"]
            folder_name = item.get("folder_name")
            date_added = item.get("date_added")

            full_release = get_release_details(release_id)
            lowest, median, highest = get_market_stats(release_id)

            if folder_name and folder_name not in folder_options:
                print(f"Adding new folder option: {folder_name}")
                update_folder_schema(folder_name, folder_options)

            size, speed, details = parse_formats(full_release.get("formats"))

            artists = ", ".join(a["name"] for a in full_release.get("artists", []))
            labels = ", ".join(l["name"] for l in full_release.get("labels", []))
            catno = full_release.get("labels", [{}])[0].get("catno", "")

            properties = {
                "Title": {"title": [{"text": {"content": full_release.get("title", "")}}]},
                "Artist": {"rich_text": [{"text": {"content": artists}}]},
                "Discogs ID": {"number": release_id},
                "Year": {"number": full_release.get("year")},
                "Country": {"rich_text": [{"text": {"content": full_release.get("country", "")}}]},
                "Label": {"rich_text": [{"text": {"content": labels}}]},
                "CatNo": {"rich_text": [{"text": {"content": catno}}]},
                "FormatSize": {"rich_text": [{"text": {"content": size or ""}}]},
                "FormatSpeed": {"rich_text": [{"text": {"content": speed or ""}}]},
                "FormatDetails": {"rich_text": [{"text": {"content": details or ""}}]},
                "Folder": {"select": {"name": folder_name}} if folder_name else None,
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
                    {
                        "parent": {"database_id": DATABASE_ID},
                        "properties": properties
                    }
                )
                created += 1

            time.sleep(1)

        except Exception as e:
            print(f"[Release ERROR] ID {release_id}: {e}")
            failed += 1
            continue

    print("Sync complete.")
    print(f"Created: {created}")
    print(f"Updated: {updated}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
