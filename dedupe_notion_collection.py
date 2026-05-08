import os
import time
import requests
from collections import defaultdict
from datetime import datetime

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

NOTION_BASE = "https://api.notion.com/v1"

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

DRY_RUN = True

# ---------------------------------------------------
# HEADERS
# ---------------------------------------------------

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ---------------------------------------------------
# REQUEST HELPER
# ---------------------------------------------------

def notion_request(method, url, payload=None, retries=5):
    for attempt in range(retries):
        r = requests.request(method, url, headers=headers_notion, json=payload)

        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 2))
            print(f"Rate limited. Sleeping {retry}s")
            time.sleep(retry)
            continue

        if r.status_code >= 500:
            wait = 2 ** attempt
            print(f"Notion server error {r.status_code}. Retrying in {wait}s")
            time.sleep(wait)
            continue

        if not r.ok:
            print(f"NOTION ERROR {r.status_code}: {r.text}")
            return None

        return r

    return None

# ---------------------------------------------------
# FETCH ALL PAGES
# ---------------------------------------------------


def fetch_all_pages():
    pages = []

    has_more = True
    start_cursor = None

    while has_more:

        payload = {
            "page_size": 100
        }

        if start_cursor:
            payload["start_cursor"] = start_cursor

        r = notion_request(
            "POST",
            f"{NOTION_BASE}/databases/{DATABASE_ID}/query",
            payload
        )

        if not r:
            raise Exception("Failed fetching Notion pages")

        data = r.json()

        batch = data.get("results", [])
        pages.extend(batch)

        print(f"Fetched {len(pages)} pages so far...")

        has_more = data.get("has_more")
        start_cursor = data.get("next_cursor")

    return pages

# ---------------------------------------------------
# DELETE PAGE
# ---------------------------------------------------


def archive_page(page_id):
    payload = {
        "archived": True
    }

    r = notion_request(
        "PATCH",
        f"{NOTION_BASE}/pages/{page_id}",
        payload
    )

    return bool(r)

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------


def main():

    print("Fetching all Notion pages...")
    pages = fetch_all_pages()

    print(f"Total pages fetched: {len(pages)}")

    grouped = defaultdict(list)

    skipped_missing_instance = 0

    for page in pages:

        props = page.get("properties", {})

        instance_prop = props.get("Instance ID")

        if not instance_prop:
            skipped_missing_instance += 1
            continue

                instance_id = instance_prop.get("number")

        if instance_id is None:
            skipped_missing_instance += 1
            continue

        grouped[instance_id].append({
            "page_id": page["id"],
            "created_time": page["created_time"],
            "title": extract_title(props)
        })

    print(f"Unique Instance IDs: {len(grouped)}")
    print(f"Skipped missing Instance IDs: {skipped_missing_instance}")

    duplicate_groups = 0
    pages_to_delete = []

    for instance_id, items in grouped.items():

        if len(items) <= 1:
            continue

        duplicate_groups += 1

        # oldest first
        items_sorted = sorted(
            items,
            key=lambda x: x["created_time"]
        )

        keep = items_sorted[0]
        delete = items_sorted[1:]

        print("\n--------------------------------------------------")
        print(f"DUPLICATE INSTANCE ID: {instance_id}")
        print(f"KEEP:   {keep['created_time']} | {keep['title']} | {keep['page_id']}")

        for d in delete:
            print(f"DELETE: {d['created_time']} | {d['title']} | {d['page_id']}")
            pages_to_delete.append(d)

    print("\n==================================================")
    print(f"Duplicate groups found: {duplicate_groups}")
    print(f"Pages to delete: {len(pages_to_delete)}")
    print(f"DRY RUN MODE: {DRY_RUN}")
    print("==================================================")

    if DRY_RUN:
        print("\nDry run complete. No pages deleted.")
        return

    deleted = 0

    for item in pages_to_delete:

        success = archive_page(item["page_id"])

        if success:
            deleted += 1
            print(f"Archived: {item['page_id']}")
        else:
            print(f"FAILED: {item['page_id']}")

        time.sleep(0.35)

    print("\n==================================================")
    print(f"Archived pages: {deleted}")
    print("==================================================")

# ---------------------------------------------------
# UTIL
# ---------------------------------------------------


def extract_title(props):
    try:
        title_prop = props["Title"]["title"]
        if title_prop:
            return title_prop[0]["plain_text"]
    except:
        pass

    return "Untitled"

# ---------------------------------------------------

if __name__ == "__main__":
    main()
