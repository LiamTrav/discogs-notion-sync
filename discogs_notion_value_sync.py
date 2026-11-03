import os
import time
import requests
from requests.exceptions import RequestException

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-value-sync/2.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ---------------------------------------------------
# VALUE MAPPING
# ---------------------------------------------------
# ValueLow = lowest price currently listed for sale (marketplace stats).
# Every other property = Discogs price suggestion for that condition grade.
# Grades are matched on the text before the bracket in the Discogs key,
# e.g. "Near Mint (NM or M-)" -> "near mint", so small label changes
# on Discogs' side don't break the match.

GRADE_PROPERTIES = {
    "ValueHigh": "mint",
    "ValueNM": "near mint",
    "ValueMed": "very good plus",
    "ValueVG": "very good",
    "ValueGPlus": "good plus",
    "ValueG": "good",
    "ValueFair": "fair",
    "ValuePoor": "poor",
}

VALUE_PROPERTIES = ["ValueLow"] + list(GRADE_PROPERTIES)

# ---------------------------------------------------
# REQUEST HELPERS
# ---------------------------------------------------

def throttle_if_needed(r):
    """Only pause when Discogs says the rate-limit window is nearly used up."""
    remaining = r.headers.get("X-Discogs-Ratelimit-Remaining")
    if remaining is None:
        return
    try:
        remaining = int(remaining)
    except ValueError:
        return
    if remaining <= 2:
        print(f"DISCOGS RATE LIMIT NEARLY USED (remaining {remaining}) - pausing 10s")
        time.sleep(10)

def discogs_request(url, retries=5):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers_discogs, timeout=30)
        except RequestException as e:
            wait = 2 ** attempt
            print(f"DISCOGS REQUEST EXCEPTION: {e} - Retrying in {wait}s")
            time.sleep(wait)
            continue

        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 10))
            print(f"DISCOGS RATE LIMITED - Sleeping {retry}s")
            time.sleep(retry)
            continue

        if r.status_code >= 500:
            wait = 2 ** attempt
            print(f"DISCOGS SERVER ERROR {r.status_code} for {url} - Retrying in {wait}s")
            time.sleep(wait)
            continue

        if not r.ok:
            print(f"DISCOGS ERROR {r.status_code} for {url}")
            return None

        throttle_if_needed(r)
        return r

    print(f"DISCOGS REQUEST FAILED AFTER {retries} ATTEMPTS: {url}")
    return None

def notion_request(method, url, payload=None, retries=5):
    for attempt in range(retries):
        try:
            r = requests.request(
                method,
                url,
                headers=headers_notion,
                json=payload,
                timeout=30
            )

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 2))
                print(f"NOTION RATE LIMITED - Sleeping {retry}s")
                time.sleep(retry)
                continue

            if r.status_code >= 500:
                wait = 2 ** attempt
                print(f"NOTION SERVER ERROR {r.status_code} - Retrying in {wait}s")
                time.sleep(wait)
                continue

            if not r.ok:
                print(f"NOTION ERROR {r.status_code}: {r.text}")
                return None

            return r

        except RequestException as e:
            wait = 2 ** attempt
            print(f"NOTION REQUEST EXCEPTION: {e} - Retrying in {wait}s")
            time.sleep(wait)

    print(f"NOTION REQUEST FAILED AFTER {retries} ATTEMPTS: {url}")
    return None

# ---------------------------------------------------
# SCHEMA CHECK
# ---------------------------------------------------

def check_schema():
    """Stop early, with a clear message, if the Notion properties are missing."""
    r = notion_request("GET", f"{NOTION_BASE}/databases/{DATABASE_ID}")
    if not r:
        raise SystemExit("Could not read the Notion database schema.")

    props = r.json().get("properties", {})
    problems = []

    for name in ["Discogs ID"] + VALUE_PROPERTIES:
        if name not in props:
            problems.append(f"Missing property: {name}")
        elif props[name].get("type") != "number":
            problems.append(
                f"Property {name} is type '{props[name].get('type')}', expected 'number'"
            )

    if problems:
        print("Notion schema check FAILED:")
        for p in problems:
            print(" -", p)
        raise SystemExit(
            "Create the missing Number properties in Notion, then run again."
        )

    print("Notion schema check passed.")

# ---------------------------------------------------
# DISCOGS VALUES
# ---------------------------------------------------

def round_price(v):
    if v is None:
        return None
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None

def get_market_values(release_id):
    """
    Returns a dict of {Notion property: value} for the release.
    Returns None if either Discogs call failed, so that a failed call
    is never mistaken for "no data" and used to blank existing values.
    """
    r_stats = discogs_request(f"{DISCOGS_BASE}/marketplace/stats/{release_id}")
    r_price = discogs_request(f"{DISCOGS_BASE}/marketplace/price_suggestions/{release_id}")

    if not r_stats or not r_price:
        return None

    values = {prop: None for prop in VALUE_PROPERTIES}

    lowest = r_stats.json().get("lowest_price")
    if isinstance(lowest, dict):
        values["ValueLow"] = round_price(lowest.get("value"))

    suggestions = {}
    for key, entry in r_price.json().items():
        if isinstance(entry, dict):
            grade = key.split(" (")[0].strip().lower()
            suggestions[grade] = entry.get("value")

    for prop, grade in GRADE_PROPERTIES.items():
        values[prop] = round_price(suggestions.get(grade))

    return values

# ---------------------------------------------------
# FETCH ALL NOTION PAGES
# ---------------------------------------------------

def get_number(props, name):
    return round_price((props.get(name) or {}).get("number"))

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
            raise Exception("Failed fetching Notion pages")

        data = r.json()

        for result in data.get("results", []):
            props = result["properties"]

            pages.append({
                "page_id": result["id"],
                "discogs_id": (props.get("Discogs ID") or {}).get("number"),
                "values": {p: get_number(props, p) for p in VALUE_PROPERTIES}
            })

        has_more = data.get("has_more")
        start_cursor = data.get("next_cursor")

    return pages

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    check_schema()

    print("Fetching Notion pages...")
    pages = fetch_all_pages()
    print(f"Fetched {len(pages)} pages.")

    updated = 0
    unchanged = 0
    no_release_id = 0
    failed = 0

    # Several copies of one release share the same Discogs values,
    # so each release is only looked up once per run.
    value_cache = {}

    for page in pages:

        release_id = page["discogs_id"]
        if not release_id:
            no_release_id += 1
            continue

        release_id = int(release_id)

        if release_id not in value_cache:
            value_cache[release_id] = get_market_values(release_id)

        new_values = value_cache[release_id]

        if new_values is None:
            print(f"SKIPPED (Discogs lookup failed, existing values kept): release {release_id}")
            failed += 1
            continue

        changed = {
            prop: new_values[prop]
            for prop in VALUE_PROPERTIES
            if page["values"][prop] != new_values[prop]
        }

        if not changed:
            unchanged += 1
            continue

        properties = {prop: {"number": val} for prop, val in changed.items()}

        r = notion_request(
            "PATCH",
            f"{NOTION_BASE}/pages/{page['page_id']}",
            {"properties": properties}
        )

        if r:
            updated += 1
        else:
            failed += 1

    print("Value sync complete.")
    print("Updated:", updated)
    print("Unchanged:", unchanged)
    print("No Discogs ID:", no_release_id)
    print("Failed / skipped:", failed)

if __name__ == "__main__":
    main()
