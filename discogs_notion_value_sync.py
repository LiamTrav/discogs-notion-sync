import os
import time
import requests

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-value-sync/1.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ---------------------------------------------------
# REQUEST HELPERS (same behaviour as main script)
# ---------------------------------------------------

def discogs_request(url):
    while True:
        r = requests.get(url, headers=headers_discogs)

        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 5))
            time.sleep(retry)
            continue

        if r.status_code >= 500:
            print(f"DISCOGS ERROR {r.status_code} for {url}")
            time.sleep(2)
            return None

        if not r.ok:
            print(f"DISCOGS ERROR {r.status_code} for {url}")
            return None

        return r


def notion_request(method, url, payload=None):
    r = requests.request(method, url, headers=headers_notion, json=payload)
    if not r.ok:
        print("NOTION ERROR:", r.status_code, r.text)
        return None
    return r


# ---------------------------------------------------
# DISCOGS VALUES (identical logic to main script)
# ---------------------------------------------------

def get_market_values(release_id):
    lowest = median = highest = None

    # Lowest ever sold
    r_stats = discogs_request(f"{DISCOGS_BASE}/marketplace/stats/{release_id}")
    if r_stats:
        lp = r_stats.json().get("lowest_price")
        if lp:
            lowest = lp.get("value")

    # VG+ and Mint suggestions
    r_price = discogs_request(f"{DISCOGS_BASE}/marketplace/price_suggestions/{release_id}")
    if r_price:
        data = r_price.json()

        if data.get("Very Good Plus (VG+)"):
            median = data["Very Good Plus (VG+)"]["value"]

        if data.get("Mint (M)"):
            highest = data["Mint (M)"]["value"]

    return lowest, median, highest


# ---------------------------------------------------
# FETCH ALL NOTION PAGES
# ---------------------------------------------------

def fetch_all_pages():
    pages = []
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

            pages.append({
                "page_id": result["id"],
                "discogs_id": props["Discogs ID"]["number"],
                "low": props["ValueLow"]["number"],
                "med": props["ValueMed"]["number"],
                "high": props["ValueHigh"]["number"]
            })

        has_more = data.get("has_more")
        start_cursor = data.get("next_cursor")

    return pages


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    print("Fetching Notion pages...")
    pages = fetch_all_pages()

    updated = 0
    skipped = 0

    for page in pages:

        release_id = page["discogs_id"]
        if not release_id:
            skipped += 1
            continue

        lowest, median, highest = get_market_values(release_id)

        # Only update if changed
        if (
            page["low"] == lowest and
            page["med"] == median and
            page["high"] == highest
        ):
            skipped += 1
            continue

        properties = {
            "ValueLow": {"number": lowest},
            "ValueMed": {"number": median},
            "ValueHigh": {"number": highest},
        }

        r = notion_request(
            "PATCH",
            f"{NOTION_BASE}/pages/{page['page_id']}",
            {"properties": properties}
        )

        if r:
            updated += 1

    print("Value sync complete.")
    print("Updated:", updated)
    print("Unchanged:", skipped)


if __name__ == "__main__":
    main()
