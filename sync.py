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
    "User-Agent": "discogs-notion-sync/4.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ---------------------------------------------------
# REQUEST HELPERS
# ---------------------------------------------------

def notion_request(method, url, payload=None):
    r = requests.request(method, url, headers=headers_notion, json=payload)
    if not r.ok:
        print("NOTION ERROR:", r.status_code, r.text)
        return None
    return r


def discogs_request(url):
    r = requests.get(url, headers=headers_discogs)
    if not r.ok:
        print("DISCOGS ERROR:", r.status_code, r.text)
        return None
    time.sleep(1.1)
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
# DISCOGS FETCHERS
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


def get_release_details(release_id):
    r = discogs_request(f"{DISCOGS_BASE}/releases/{release_id}")
    return r.json() if r else {}


def get_market_stats(release_id):
    lowest = median = highest = None

    r_stats = discogs_request(f"{DISCOGS_BASE}/marketplace/stats/{release_id}")
    if r_stats:
        lp = r_stats.json().get("lowest_price")
        if lp:
            lowest = lp.get("value")

    r_price = discogs_request(f"{DISCOGS_BASE}/marketplace/price_suggestions/{release_id}")
    if r_price:
        data = r_price.json()
        if data.get("Very Good Plus (VG+)"):
            median = data["Very Good Plus (VG+)"]["value"]
        if data.get("Mint (M)"):
            highest = data["Mint (M)"]["value"]

    return lowest, median, highest


def get_folder_map():
    r = discogs_request(f"{DISCOGS_BASE}/users/{USERNAME}/collection/folders")
    return {f["id"]: f["name"] for f in r.json().get("folders", [])} if r else {}


def get_collection_fields():
    r = discogs_request(f"{DISCOGS_BASE}/users/{USERNAME}/collection/fields")
    return {f["id"]: f["name"] for f in r.json().get("fields", [])} if r else {}


# ---------------------------------------------------
# NOTION HELPERS
# ---------------------------------------------------

def fetch_select_options(property_name):
    r = notion_request("GET", f"{NOTION_BASE}/databases/{DATABASE_ID}")
    if not r:
        return set()
    db = r.json()
    return set(o["name"] for o in db["properties"][property_name]["select"]["options"])


def update_select_schema(property_name, new_value, existing):
    if new_value in existing:
        return existing

    existing.add(new_value)
    payload = {
        "properties": {
            property_name: {
                "select": {
                    "options": [{"name": name} for name in existing]
                }
            }
        }
    }
    notion_request("PATCH", f"{NOTION_BASE}/databases/{DATABASE_ID}", payload)
    return existing


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


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():

    print("Fetching collection...")
    collection = get_full_collection()
    print("Total releases:", len(collection))

    folder_map = get_folder_map()
    field_map = get_collection_fields()
    notion_pages = fetch_existing_pages()

    select_fields = [
        "Folder", "Media Condition", "Sleeve Condition",
        "Genre", "Style", "Country", "FormatSpeed", "FormatSize"
    ]

    select_options = {field: fetch_select_options(field) for field in select_fields}

    created = updated = failed = 0

    for item in collection:
        try:
            release_id = item["basic_information"]["id"]
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

            full = get_release_details(release_id)
            lowest, median, highest = get_market_stats(release_id)

            genres = full.get("genres") or []
            styles = full.get("styles") or []
            labels = full.get("labels") or []

            genre = genres[0] if genres else None
            style = styles[0] if styles else None
            country = full.get("country")
            catno = labels[0].get("catno") if labels else ""

            size, speed, details = parse_formats(full.get("formats"))

            # Auto-create select options
            for field, value in [
                ("Folder", folder),
                ("Media Condition", media),
                ("Sleeve Condition", sleeve),
                ("Genre", genre),
                ("Style", style),
                ("Country", country),
                ("FormatSpeed", speed),
                ("FormatSize", size),
            ]:
                if value:
                    select_options[field] = update_select_schema(
                        field, value, select_options[field]
                    )

            properties = {
                "Title": {"title": [{"text": {"content": full.get("title", "")}}]},
                "Artist": {"rich_text": [{"text": {"content": ", ".join(a["name"] for a in full.get("artists", []))}}]},
                "Discogs ID": {"number": release_id},
                "Instance ID": {"number": instance_id},
                "Year": {"number": full.get("year")},
                "Label": {"rich_text": [{"text": {"content": ", ".join(l["name"] for l in labels)}}]},
                "CatNo": {"rich_text": [{"text": {"content": catno}}]},
                "FormatDetails": {"rich_text": [{"text": {"content": details or ""}}]},
                "Added": {"date": {"start": date_added}},
                "ValueLow": {"number": lowest},
                "ValueMed": {"number": median},
                "ValueHigh": {"number": highest},
                "Notes": {"rich_text": [{"text": {"content": notes}}]} if notes else None,
                "Folder": {"select": {"name": folder}} if folder else None,
                "Media Condition": {"select": {"name": media}} if media else None,
                "Sleeve Condition": {"select": {"name": sleeve}} if sleeve else None,
                "Genre": {"select": {"name": genre}} if genre else None,
                "Style": {"select": {"name": style}} if style else None,
                "Country": {"select": {"name": country}} if country else None,
                "FormatSpeed": {"select": {"name": speed}} if speed else None,
                "FormatSize": {"select": {"name": size}} if size else None,
            }

            properties = {k: v for k, v in properties.items() if v is not None}

            if instance_id in notion_pages:
                r = notion_request("PATCH", f"{NOTION_BASE}/pages/{notion_pages[instance_id]}", {"properties": properties})
                if r:
                    updated += 1
                else:
                    print(f"[Notion Update Failed] {instance_id}")
                    failed += 1
            else:
                r = notion_request("POST", f"{NOTION_BASE}/pages", {
                    "parent": {"database_id": DATABASE_ID},
                    "properties": properties
                })
                if r:
                    created += 1
                else:
                    print(f"[Notion Create Failed] {instance_id}")
                    failed += 1

        except Exception as e:
            pri
