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
    main()
