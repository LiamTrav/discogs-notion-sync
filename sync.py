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
    "User-Agent": "discogs-notion-sync/3.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ---------------------------------------------------
# REGEX
# ---------------------------------------------------

RPM_PATTERN = re.compile(
    r'\b(?:33\s?(?:⅓|1/3)|33|45|78)\s?RPM\b',
    re.IGNORECASE
)

SIZE_PATTERN = re.compile(r'(7"|10"|12")')


# ---------------------------------------------------
# REQUEST HELPERS
# ---------------------------------------------------

def notion_request(method, url, payload=None):
    r = requests.request(method, url, headers=headers_notion, json=payload)
    if r.status_code != 200:
        print(f"Notion error: {r.text}")
        return None
    return r


def discogs_request(url):
    r = requests.get(url, headers=headers_discogs)
    if r.status_code != 200:
        return None
    return r


# ---------------------------------------------------
# FORMAT PARSER
# ---------------------------------------------------

def normalise_speed(value):
    if not value:
        return None

    value = value.upper()

    if "33" in value:
        return "33 RPM"
    if "45" in value:
        return "45 RPM"
    if "78" in value:
        return "78 RPM"

    return value


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

            if not speed and RPM_PATTERN.search(desc):
                speed = normalise_speed(desc)
                continue

            details.append(desc)

    details_text = ", ".join(details) if details else None
    return size, speed, details_text


# ---------------------------------------------------
# DISCOGS API
# ---------------------------------------------------

def get_full_collection():
    releases = []
    page = 1

    while True:
        url = f"{DISCOGS_BASE}/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page=100"
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
    return r.json() if r else {}


def get_market_low(release_id):
    url = f"{DISCOGS_BASE}/marketplace/stats/{release_id}"
    r = discogs_request(url)
    if not r:
        return None

    data = r.json()
    if data.get("lowest_price"):
        return data["lowest_price"]["value"]

    return None


def get_price_suggestions(release_id):
    url = f"{DISCOGS_BASE}/marketplace/price_suggestions/{release_id}"
    r = discogs_request(url)
    if not r:
        return None, None

    data = r.json()

    median = None
    high = None

    if "Very Good Plus (VG+)" in data:
        median = data["Very Good Plus (VG+)"]["value"]

    if "Near Mint (NM or M-)" in data:
        high = data["Near Mint (NM or M-)"]["value"]

    return median, high


# ---------------------------------------------------
# NOTION HELPERS
# ---------------------------------------------------

def fetch_existing_pages():
    pages = {}
    payload = {"page_size": 100}

    r = notion_request("POST", f"{NOTION_BASE}/databases/{DATABASE_ID}/query", payload)
    if not r:
        return pages

    for result in r.json()["results"]:
        discogs_id = result["properties"]["Discogs ID"]["number"]
        if discogs_id:
            pages[discogs_id] = result

    return pages


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    print("Fetching Discogs collection...")
    collection = get_full_collection()

    print("Fetching existing Notion pages...")
    notion_pages = fetch_existing_pages()

    created = 0
    updated = 0

    for item in collection:
        release_id = item["basic_information"]["id"]
        folder_name = item.get("folder_name")
        date_added = item.get("date_added")

        release = get_release_details(release_id)

        lowest = get_market_low(release_id)
        median, high = get_price_suggestions(release_id)

        size, speed, details = parse_formats(release.get("formats"))

        artists = ", ".join(a["name"] for a in release.get("artists", []))
        labels = ", ".join(l["name"] for l in release.get("labels", []))
        catno = release.get("labels", [{}])[0].get("catno", "")

        properties = {
            "Title": {"title": [{"text": {"content": release.get("title", "")}}]},
            "Artist": {"rich_text": [{"text": {"content": artists}}]},
            "Discogs ID": {"number": release_id},
            "Year": {"number": release.get("year")},
            "Country": {"rich_text": [{"text": {"content": release.get("country", "")}}]},
            "Label": {"rich_text": [{"text": {"content": labels}}]},
            "CatNo": {"rich_text": [{"text": {"content": catno}}]},
            "FormatSize": {"rich_text": [{"text": {"content": size or ""}}]},
            "FormatSpeed": {"rich_text": [{"text": {"content": speed or ""}}]},
            "FormatDetails": {"rich_text": [{"text": {"content": details or ""}}]},
            "Folder": {"select": {"name": folder_name}} if folder_name else None,
            "ValueLow": {"number": lowest},
            "ValueMed": {"number": median},
            "ValueHigh": {"number": high},
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

        time.sleep(0.8)

    print("Sync complete.")
    print(f"Created: {created}")
    print(f"Updated: {updated}")


if __name__ == "__main__":
    main()
