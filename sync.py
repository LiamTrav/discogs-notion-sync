import os
import time
import re
import requests
import hashlib
from collections import defaultdict, Counter

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-sync/5.2"
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

def clean_commas(value):
    return value.replace(",", "").strip() if value else value


def compute_hash(data_dict):
    hash_input = "|".join(str(v) for v in data_dict.values())
    return hashlib.md5(hash_input.encode()).hexdigest()


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

    print("Existing Notion pages fetched:", len(pages))
    return pages


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
    style_counter = Counter()
    processed_items = []

    # -------- SCAN PHASE --------

    for item in collection:
        basic = item["basic_information"]
        release_id = basic["id"]
        instance_id = item["instance_id"]

        genres = [clean_commas(g) for g in (basic.get("genres") or [])]
        styles = [clean_commas(s) for s in (basic.get("styles") or [])]

        for s in styles:
            style_counter[s] += 1

        processed_items.append({
            "release_id": release_id,
            "instance_id": instance_id,
            "title": basic.get("title"),
            "artist": ", ".join(a["name"] for a in basic.get("artists", [])),
            "year": basic.get("year"),
            "genres": genres,
            "styles": styles
        })

    # -------- STYLE CAP --------

    top_styles = {s for s, _ in style_counter.most_common(100)}

    print("Total unique styles:", len(style_counter))
    print("Capped styles to:", len(top_styles))

    # -------- WRITE PHASE --------

    print("Phase 2 — Writing pages")

    notion_pages = fetch_existing_pages()

    created = updated = skipped = 0

    for item in processed_items:

        allowed_styles = [s for s in item["styles"] if s in top_styles]

        hash_payload = {
            "title": item["title"],
            "artist": item["artist"],
            "year": item["year"],
            "genres": ",".join(item["genres"]),
            "styles": ",".join(allowed_styles)
        }

        new_hash = compute_hash(hash_payload)

        existing = notion_pages.get(item["instance_id"])

        if existing and existing["hash"] == new_hash:
            skipped += 1
            continue

        properties = {
            "Title": {"title": [{"text": {"content": item["title"] or ""}}]},
            "Artist": {"rich_text": [{"text": {"content": item["artist"] or ""}}]},
            "Discogs ID": {"number": item["release_id"]},
            "Instance ID": {"number": item["instance_id"]},
            "Year": {"number": item["year"]},
            "Genre": {"multi_select": [{"name": g} for g in item["genres"]]},
            "Style": {"multi_select": [{"name": s} for s in allowed_styles]},
            "SyncHash": {"rich_text": [{"text": {"content": new_hash}}]},
        }

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
    print("Skipped (unchanged):", skipped)


# ---------------------------------------------------
# REQUIRED DISCOGS HELPERS
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


if __name__ == "__main__":
    main()
