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
