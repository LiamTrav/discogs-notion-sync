import os
import time
import re
import requests
from collections import defaultdict

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"

# ---------------------------------------------------
# HEADERS
# ---------------------------------------------------

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-sync/5.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ---------------------------------------------------
# SMART REQUEST HELPERS
# ---------------------------------------------------

def discogs_request(url):
    while True:
        r = requests.get(url, headers=headers_discogs)

        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 5))
            print(f"Rate limited. Sleeping {retry}s")
            time.sleep(retry)
            continue

        remaining = r.headers.get("X-Discogs-Ratelimit-Remaining")
        if remaining and int(remaining) < 3:
            print("Approaching rate limit. Sleeping 5s")
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
# FORMAT PARSER
# ---------------------------------------------------

RPM_PATTERN = re.compile(r"\b(33\s?⅓|33\s?1/3|33|45|78)\s?RPM\b", re.IGNORECASE)
SIZE_PATTERN = re.compile(r'\b(7"|10"|12")')

def parse_formats(formats):
    if not formats:
        return None, None, None

    size = None
    speed = None
    details = []

    for fmt in formats:
        for desc in fmt.get("descriptions", []):
            normalized = desc.replace("⅓", "1/3")

            if not size and SIZE_PATTERN.search(desc):
                size = desc
                continue

            if not speed and RPM_PATTERN.search(normalized):
                speed = desc
                continue

            details.append(desc)

    return size, speed, ", ".join(details) if details else None


# ---------------------------------------------------
# PHASE 1 — FETCH COLLECTION
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


def get_collection_fields():
    r = discogs_request(f"{DISCOGS_BASE}/users/{USERNAME}/collection/fields")
    return {f["id"]: f["name"] for f in r.json().get("fields", [])} if r else {}


# ---------------------------------------------------
# NOTION HELPERS
# ---------------------------------------------------

def fetch_existing_pages():
    pages = {}
    r = notion_request("POST", f"{NOTION_BASE}/databases/{DATABASE_ID}/query", {"page_size": 100})
    if not r:
        return pages

    for result in r.json().get("results", []):
        instance_id = result["properties"]["Instance ID"]["number"]
        if instance_id:
            pages[instance_id] = result["id"]

    return pages


def fetch_schema():
    r = notion_request("GET", f"{NOTION_BASE}/databases/{DATABASE_ID}")
    return r.json() if r else {}


def update_schema_multi_select(property_name, values):
    payload = {
        "properties": {
            property_name: {
                "multi_select": {
                    "options": [{"name": v} for v in sorted(values)]
                }
            }
        }
    }
    notion_request("PATCH", f"{NOTION_BASE}/databases/{DATABASE_ID}", payload)


def update_schema_select(property_name, values):
    payload = {
        "properties": {
            property_name: {
                "select": {
                    "options": [{"name": v} for v in sorted(values)]
                }
            }
        }
    }
    notion_request("PATCH", f"{NOTION_BASE}/databases/{DATABASE_ID}", payload)


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():

    print("Phase 1 — Fetching collection")
    collection = get_full_collection()
    print("Total releases identified:", len(collection))

    folder_map = get_folder_map()
    field_map = get_collection_fields()

    unique_selects = defaultdict(set)
    processed_items = []

    # -------------------------------
    # SCAN & COLLECT UNIQUE VALUES
    # -------------------------------

    for item in collection:

        basic = item["basic_information"]
        release_id = basic["id"]
        instance_id = item["instance_id"]

        folder = folder_map.get(item.get("folder_id"))
        date_added = item.get("date_added")

        media = sleeve = None
        true_notes = []

        for n in item.get("notes", []):
            field_name = field_map.get(n.get("field_id"))
            value = n.get("value")
            if not value:
                continue
            if field_name == "Media Condition":
                media = value
            elif field_name == "Sleeve Condition":
                sleeve = value
            else:
                true_notes.append(value)

        notes = "\n".join(true_notes) if true_notes else None

        genres = basic.get("genres") or []
        styles = basic.get("styles") or []
        labels = basic.get("labels") or []

        genre_values = genres
        style_values = styles

        country = basic.get("country")
        catno = labels[0].get("catno") if labels else ""

        size, speed, details = parse_formats(basic.get("formats"))

        # Collect unique select values
        for field, value in [
            ("Folder", folder),
            ("Media Condition", media),
            ("Sleeve Condition", sleeve),
            ("Country", country),
            ("FormatSpeed", speed),
            ("FormatSize", size),
        ]:
            if value:
                unique_selects[field].add(value)

        for g in genre_values:
            unique_selects["Genre"].add(g)

        for s in style_values:
            unique_selects["Style"].add(s)

        processed_items.append({
            "release_id": release_id,
            "instance_id": instance_id,
            "title": basic.get("title"),
            "artist": ", ".join(a["name"] for a in basic.get("artists", [])),
            "year": basic.get("year"),
            "label": ", ".join(l["name"] for l in labels),
            "catno": catno,
            "genres": genre_values,
            "styles": style_values,
            "country": country,
            "folder": folder,
            "media": media,
            "sleeve": sleeve,
            "size": size,
            "speed": speed,
            "details": details,
            "notes": notes,
            "added": date_added
        })

    # -------------------------------
    # PHASE 2 — SYNC SCHEMA
    # -------------------------------

    print("Phase 2 — Syncing schema")

    for field in ["Folder", "Media Condition", "Sleeve Condition",
                  "Country", "FormatSpeed", "FormatSize"]:
        update_schema_select(field, unique_selects[field])

    update_schema_multi_select("Genre", unique_selects["Genre"])
    update_schema_multi_select("Style", unique_selects["Style"])

    # -------------------------------
    # PHASE 3 — WRITE PAGES
    # -------------------------------

    print("Phase 3 — Writing pages")

    notion_pages = fetch_existing_pages()
    created = updated = failed = 0

    for item in processed_items:

        properties = {
            "Title": {"title": [{"text": {"content": item["title"] or ""}}]},
            "Artist": {"rich_text": [{"text": {"content": item["artist"] or ""}}]},
            "Discogs ID": {"number": item["release_id"]},
            "Instance ID": {"number": item["instance_id"]},
            "Year": {"number": item["year"]},
            "Label": {"rich_text": [{"text": {"content": item["label"] or ""}}]},
            "CatNo": {"rich_text": [{"text": {"content": item["catno"] or ""}}]},
            "FormatDetails": {"rich_text": [{"text": {"content": item["details"] or ""}}]},
            "Added": {"date": {"start": item["added"]}},
            "Notes": {"rich_text": [{"text": {"content": item["notes"]}}]} if item["notes"] else None,
            "Folder": {"select": {"name": item["folder"]}} if item["folder"] else None,
            "Media Condition": {"select": {"name": item["media"]}} if item["media"] else None,
            "Sleeve Condition": {"select": {"name": item["sleeve"]}} if item["sleeve"] else None,
            "Country": {"select": {"name": item["country"]}} if item["country"] else None,
            "FormatSpeed": {"select": {"name": item["speed"]}} if item["speed"] else None,
            "FormatSize": {"select": {"name": item["size"]}} if item["size"] else None,
            "Genre": {"multi_select": [{"name": g} for g in item["genres"]]},
            "Style": {"multi_select": [{"name": s} for s in item["styles"]]},
        }

        properties = {k: v for k, v in properties.items() if v is not None}

        if item["instance_id"] in notion_pages:
            r = notion_request(
                "PATCH",
                f"{NOTION_BASE}/pages/{notion_pages[item['instance_id']]}",
                {"properties": properties}
            )
            if r:
                updated += 1
            else:
                failed += 1
        else:
            r = notion_request(
                "POST",
                f"{NOTION_BASE}/pages",
                {
                    "parent": {"database_id": DATABASE_ID},
                    "properties": properties
                }
            )
            if r:
                created += 1
            else:
                failed += 1

    print("Sync complete.")
    print("Created:", created)
    print("Updated:", updated)
    print("Failed:", failed)


if __name__ == "__main__":
    main()
