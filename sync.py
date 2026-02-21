import os
import time
import requests
import hashlib
from collections import defaultdict

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-sync/6.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ---------------------------------------------------
# REQUEST HELPERS
# ---------------------------------------------------

def discogs_request(url):
    while True:
        r = requests.get(url, headers=headers_discogs)

        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 5))
            time.sleep(retry)
            continue

        remaining = r.headers.get("X-Discogs-Ratelimit-Remaining")
        if remaining and int(remaining) < 3:
            time.sleep(5)

        if not r.ok:
            print("DISCOGS ERROR:", r.status_code, r.text)
            return None

        return r


def notion_request(method, url, payload=None):
    r = requests.request(method, url, headers=headers_notion, json=payload)
    if not r.ok:
        print("NOTION ERROR:", r.status_code, r.text)
        return None
    return r


# ---------------------------------------------------
# UTIL
# ---------------------------------------------------

def compute_hash(data_dict):
    hash_input = "|".join(str(v or "") for v in data_dict.values())
    return hashlib.md5(hash_input.encode()).hexdigest()


def parse_formats(formats):
    size = None
    speed = None
    details = []

    for fmt in formats or []:
        descriptions = fmt.get("descriptions", [])

        for d in descriptions:
            if 'RPM' in d:
                speed = d
            elif '"' in d:
                size = d
            else:
                details.append(d)

    return size, speed, ", ".join(details)


# ---------------------------------------------------
# NOTION PAGINATION
# ---------------------------------------------------

def fetch_existing_pages():
    pages = {}
    has_more = True
    start_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        r = notion_request(
            "POST",
            f"{NOTION_BASE}/databases/{DATABASE_ID}/query",
            payload
        )

        if not r:
            break

        data = r.json()

        for result in data.get("results", []):
            props = result["properties"]
            instance_id = props["Instance ID"]["number"]

            existing_hash = ""
            if props.get("SyncHash") and props["SyncHash"]["rich_text"]:
                existing_hash = props["SyncHash"]["rich_text"][0]["text"]["content"]

            if instance_id:
                pages[instance_id] = {
                    "page_id": result["id"],
                    "hash": existing_hash
                }

        has_more = data.get("has_more")
        start_cursor = data.get("next_cursor")

    return pages


# ---------------------------------------------------
# DISCOGS HELPERS
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
        releases.extend(data.get("releases", []))

        if page >= data.get("pagination", {}).get("pages", 1):
            break

        page += 1

    return releases


def get_folder_map():
    r = discogs_request(f"{DISCOGS_BASE}/users/{USERNAME}/collection/folders")
    return {f["id"]: f["name"] for f in r.json().get("folders", [])} if r else {}


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():

    print("Fetching Discogs collection...")
    collection = get_full_collection()
    folder_map = get_folder_map()
    notion_pages = fetch_existing_pages()

    created = updated = skipped = 0

    for item in collection:

        basic = item["basic_information"]
        release_id = basic["id"]
        instance_id = item["instance_id"]

        title = basic.get("title")
        artist = ", ".join(a["name"] for a in basic.get("artists", []))
        year = basic.get("year")
        country = basic.get("country")

        label_data = basic.get("labels", [])
        label = label_data[0]["name"] if label_data else None
        catno = label_data[0]["catno"] if label_data else None

        genres = basic.get("genres") or []
        styles = basic.get("styles") or []

        size, speed, details = parse_formats(basic.get("formats"))

        folder_name = folder_map.get(item.get("folder_id"))
        notes = item.get("notes")
        media_condition = item.get("media_condition")
        sleeve_condition = item.get("sleeve_condition")
        date_added = item.get("date_added")

        hash_payload = {
            "title": title,
            "artist": artist,
            "year": year,
            "label": label,
            "catno": catno,
            "country": country,
            "folder": folder_name,
            "notes": notes,
            "media_condition": media_condition,
            "sleeve_condition": sleeve_condition,
            "date_added": date_added,
            "formatsize": size,
            "formatspeed": speed,
            "formatdetails": details,
            "genres": ",".join(genres),
            "styles": ",".join(styles),
        }

        new_hash = compute_hash(hash_payload)
        existing = notion_pages.get(instance_id)

        if existing and existing["hash"] == new_hash:
            skipped += 1
            continue

        properties = {
            "Title": {"title": [{"text": {"content": title or ""}}]},
            "Artist": {"rich_text": [{"text": {"content": artist or ""}}]},
            "Discogs ID": {"number": release_id},
            "Instance ID": {"number": instance_id},
            "Year": {"number": year},
            "Label": {"rich_text": [{"text": {"content": label or ""}}]},
            "CatNo": {"rich_text": [{"text": {"content": catno or ""}}]},
            "Country": {"select": {"name": country}} if country else None,
            "Folder": {"select": {"name": folder_name}} if folder_name else None,
            "Notes": {"rich_text": [{"text": {"content": notes or ""}}]},
            "Media Condition": {"select": {"name": media_condition}} if media_condition else None,
            "Sleeve Condition": {"select": {"name": sleeve_condition}} if sleeve_condition else None,
            "Added": {"date": {"start": date_added}} if date_added else None,
            "FormatSize": {"select": {"name": size}} if size else None,
            "FormatSpeed": {"select": {"name": speed}} if speed else None,
            "FormatDetails": {"rich_text": [{"text": {"content": details or ""}}]},
            "Genre": {"multi_select": [{"name": g} for g in genres]},
            "Style": {"multi_select": [{"name": s} for s in styles]},
            "SyncHash": {"rich_text": [{"text": {"content": new_hash}}]},
        }

        # Remove None properties
        properties = {k: v for k, v in properties.items() if v is not None}

        if existing:
            r = notion_request(
                "PATCH",
                f"{NOTION_BASE}/pages/{existing['page_id']}",
                {"properties": properties}
            )
            if r:
                updated += 1
        else:
            r = notion_request(
                "POST",
                f"{NOTION_BASE}/pages",
                {"parent": {"database_id": DATABASE_ID}, "properties": properties}
            )
            if r:
                created += 1

    print("Sync complete.")
    print("Created:", created)
    print("Updated:", updated)
    print("Skipped:", skipped)


if __name__ == "__main__":
    main()
