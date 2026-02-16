import os
import time
import requests
from datetime import datetime

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-sync/1.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}


# -----------------------------
# Helper Functions
# -----------------------------

def get_all_discogs_collection():
    releases = []
    page = 1
    per_page = 100

    while True:
        url = f"{DISCOGS_BASE}/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page={per_page}"
        r = requests.get(url, headers=headers_discogs)
        r.raise_for_status()
        data = r.json()

        releases.extend(data["releases"])

        if page >= data["pagination"]["pages"]:
            break
        page += 1

    return releases


def get_market_stats(release_id):
    url = f"{DISCOGS_BASE}/marketplace/stats/{release_id}"
    r = requests.get(url, headers=headers_discogs)
    if r.status_code != 200:
        return None, None, None

    data = r.json()
    lowest = data.get("lowest_price", {}).get("value") if data.get("lowest_price") else None
    median = data.get("median_price", {}).get("value") if data.get("median_price") else None
    highest = data.get("highest_price", {}).get("value") if data.get("highest_price") else None

    return lowest, median, highest


def parse_formats(formats):
    size = None
    speed = None
    details = []

    if not formats:
        return None, None, None

    for fmt in formats:
        descriptions = fmt.get("descriptions", [])
        for d in descriptions:
            if '"' in d and not size:
                size = d
            elif "RPM" in d and not speed:
                speed = d
            else:
                details.append(d)

    details_str = ", ".join(details) if details else None
    return size, speed, details_str


def notion_query_by_discogs_id(discogs_id):
    url = f"{NOTION_BASE}/databases/{DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": "Discogs ID",
            "number": {
                "equals": discogs_id
            }
        }
    }
    r = requests.post(url, headers=headers_notion, json=payload)
    r.raise_for_status()
    results = r.json()["results"]
    return results[0] if results else None


def ensure_folder_option(folder_name):
    # Retrieve current DB schema
    db_url = f"{NOTION_BASE}/databases/{DATABASE_ID}"
    r = requests.get(db_url, headers=headers_notion)
    r.raise_for_status()
    db = r.json()

    options = db["properties"]["Folder"]["select"]["options"]
    existing_names = [o["name"] for o in options]

    if folder_name in existing_names:
        return

    # Add new option
    options.append({"name": folder_name})
    update_payload = {
        "properties": {
            "Folder": {
                "select": {
                    "options": options
                }
            }
        }
    }

    requests.patch(db_url, headers=headers_notion, json=update_payload).raise_for_status()


def build_notion_properties(release, stats, is_create):
    release_data = release["basic_information"]

    size, speed, details = parse_formats(release_data.get("formats"))
    lowest, median, highest = stats

    props = {
        "Title": {
            "title": [{"text": {"content": release_data.get("title", "")}}]
        },
        "Artist": {
            "rich_text": [{"text": {"content": ", ".join([a["name"] for a in release_data.get("artists", [])])}}]
        },
        "Discogs ID": {
            "number": release_data.get("id")
        },
        "Year": {
            "number": release_data.get("year")
        },
        "Label": {
            "rich_text": [{"text": {"content": ", ".join(release_data.get("labels", [{}])[0].get("name", ""))}}]
        },
        "Country": {
            "rich_text": [{"text": {"content": release_data.get("country", "")}}]
        },
        "FormatSize": {
            "rich_text": [{"text": {"content": size or ""}}]
        },
        "FormatSpeed": {
            "rich_text": [{"text": {"content": speed or ""}}]
        },
        "FormatDetails": {
            "rich_text": [{"text": {"content": details or ""}}]
        },
        "ValueLow": {"number": lowest},
        "ValueMed": {"number": median},
        "ValueHigh": {"number": highest},
        "CatNo": {
            "rich_text": [{"text": {"content": release_data.get("labels", [{}])[0].get("catno", "")}}]
        }
    }

    folder_name = release.get("folder_name")
    if folder_name:
        props["Folder"] = {"select": {"name": folder_name}}

    if is_create:
        props["Added"] = {
            "date": {
                "start": release.get("date_added")
            }
        }

    return props


# -----------------------------
# Main Sync Logic
# -----------------------------

def main():
    print("Fetching Discogs collection...")
    collection = get_all_discogs_collection()
    total_releases = len(collection)
    print(f"Total releases identified in Discogs: {total_releases}")

    created = 0
    updated = 0

    for release in collection:
        release_id = release["basic_information"]["id"]
        folder_name = release.get("folder_name")
        release["folder_name"] = folder_name

        stats = get_market_stats(release_id)
        time.sleep(1)  # rate limit safety

        existing_page = notion_query_by_discogs_id(release_id)

        if folder_name:
            ensure_folder_option(folder_name)

        if existing_page:
            page_id = existing_page["id"]
            props = build_notion_properties(release, stats, is_create=False)
            requests.patch(
                f"{NOTION_BASE}/pages/{page_id}",
                headers=headers_notion,
                json={"properties": props}
            ).raise_for_status()
            updated += 1
        else:
            props = build_notion_properties(release, stats, is_create=True)
            requests.post(
                f"{NOTION_BASE}/pages",
                headers=headers_notion,
                json={
                    "parent": {"database_id": DATABASE_ID},
                    "properties": props
                }
            ).raise_for_status()
            created += 1

    print("Sync complete.")
    print(f"Pages created in Notion: {created}")
    print(f"Pages updated in Notion: {updated}")


if __name__ == "__main__":
    main()
