import os
import time
import re
import requests
import hashlib

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-sync/7.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ---------------------------------------------------
# REQUEST HELPERS (SMART RATE LIMIT)
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


def clean_multiselect(value):
    return value.replace(",", "").strip() if value else value


def compute_hash(d):
    return hashlib.md5("|".join(str(v or "") for v in d.values()).encode()).hexdigest()

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


def get_market_values(release_id):
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
# MAIN
# ---------------------------------------------------

def main():

    print("Phase 1 — Fetching collection")
    collection = get_full_collection()
    folder_map = get_folder_map()
    field_map = get_collection_fields()

    print("Phase 2 — Fetching Notion pages")
    notion_pages = fetch_existing_pages()

    created = updated = skipped = 0

    for item in collection:

        basic = item["basic_information"]
        release_id = basic["id"]
        instance_id = item["instance_id"]

        folder = folder_map.get(item.get("folder_id"))
        date_added = item.get("date_added")

        media = sleeve = None
        real_notes = []

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
                real_notes.append(value)

        notes = "\n".join(real_notes) if real_notes else None

        labels = basic.get("labels") or []
        label = labels[0]["name"] if labels else None
        catno = labels[0]["catno"] if labels else None

        genres = [clean_multiselect(g) for g in (basic.get("genres") or [])]
        styles = [clean_multiselect(s) for s in (basic.get("styles") or [])]

        size, speed, details = parse_formats(basic.get("formats"))

        metadata_hash_payload = {
            "title": basic.get("title"),
            "artist": ", ".join(a["name"] for a in basic.get("artists", [])),
            "year": basic.get("year"),
            "label": label,
            "catno": catno,
            "country": basic.get("country"),
            "folder": folder,
            "media": media,
            "sleeve": sleeve,
            "notes": notes,
            "size": size,
            "speed": speed,
            "details": details,
            "genres": ",".join(genres),
            "styles": ",".join(styles),
        }

        new_hash = compute_hash(metadata_hash_payload)
        existing = notion_pages.get(instance_id)

        if existing and existing["hash"] == new_hash:
            skipped += 1
            continue

        # Phase 3 — Marketplace only if needed
        lowest, median, highest = get_market_values(release_id)

        properties = {
            "Title": {"title": [{"text": {"content": basic.get("title", "")}}]},
            "Artist": {"rich_text": [{"text": {"content": ", ".join(a["name"] for a in basic.get("artists", []))}}]},
            "Discogs ID": {"number": release_id},
            "Instance ID": {"number": instance_id},
            "Year": {"number": basic.get("year")},
            "Label": {"rich_text": [{"text": {"content": label or ""}}]},
            "CatNo": {"rich_text": [{"text": {"content": catno or ""}}]},
            "Country": {"select": {"name": basic.get("country")}} if basic.get("country") else None,
            "Folder": {"select": {"name": folder}} if folder else None,
            "Media Condition": {"select": {"name": media}} if media else None,
            "Sleeve Condition": {"select": {"name": sleeve}} if sleeve else None,
            "Added": {"date": {"start": date_added}} if date_added else None,
            "FormatSize": {"select": {"name": size}} if size else None,
            "FormatSpeed": {"select": {"name": speed}} if speed else None,
            "FormatDetails": {"rich_text": [{"text": {"content": details or ""}}]},
            "Genre": {"multi_select": [{"name": g} for g in genres]},
            "Style": {"multi_select": [{"name": s} for s in styles]},
            "ValueLow": {"number": lowest},
            "ValueMed": {"number": median},
            "ValueHigh": {"number": highest},
            "Notes": {"rich_text": [{"text": {"content": notes}}]} if notes else None,
            "SyncHash": {"rich_text": [{"text": {"content": new_hash}}]},
        }

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
