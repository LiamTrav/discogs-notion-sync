import os
import time
import requests

# Environment variables
DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

DISCOGS_HEADERS = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": f"{USERNAME} discogs-notion-sync"
}

def get_database_pages():
    """Retrieve all pages from the Notion database."""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    pages = []
    has_more = True
    start_cursor = None

    while has_more:
        payload = {"start_cursor": start_cursor} if start_cursor else {}
        response = requests.post(url, headers=NOTION_HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()
        pages.extend(data["results"])
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor", None)

    return pages

def get_discogs_release(release_id):
    """Retrieve release data from Discogs, handling rate limits."""
    url = f"https://api.discogs.com/releases/{release_id}"
    while True:
        response = requests.get(url, headers=DISCOGS_HEADERS)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 2))
            print(f"Rate limited by Discogs, retrying after {retry_after}s...")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        return response.json()

def parse_format_details(format_details_str):
    """Split format details into size, speed, and remaining."""
    if not format_details_str:
        return None, None, None

    details = [s.strip() for s in format_details_str.split(",")]
    size = None
    speed = None
    remaining = []

    for d in details:
        if d.endswith('"') and d[:-1].isdigit():  # e.g., 7" or 12"
            size = d
        elif "RPM" in d.upper():
            speed = d
        else:
            remaining.append(d)

    remaining_str = ", ".join(remaining) if remaining else None
    return size, speed, remaining_str

def safe_number(value):
    """Return a float or None for Notion number property."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def safe_rich_text(value):
    """Return Notion rich_text object or None."""
    if value is None:
        return None
    return [{"type": "text", "text": {"content": str(value)}}]

def safe_select(value):
    """Return Notion select object or None."""
    if not value:
        return None
    return {"name": str(value)}

def update_page(page_id, properties):
    """Update a Notion page with given properties."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    # Remove any None values to avoid 400 errors
    filtered_props = {k: v for k, v in properties.items() if v is not None}
    if not filtered_props:
        return
    response = requests.patch(url, headers=NOTION_HEADERS, json={"properties": filtered_props})
    if not response.ok:
        print(f"Failed to update {page_id}: {response.status_code} {response.text}")
    else:
        print(f"Updated {page_id}")

def main():
    pages = get_database_pages()
    print(f"Found {len(pages)} pages in Notion database.")

    for page in pages:
        notion_id = page["id"]
        discogs_id_prop = page["properties"].get("Discogs ID")
        if not discogs_id_prop:
            print(f"No Discogs ID for page {notion_id}, skipping.")
            continue
        discogs_id = discogs_id_prop.get("number") or discogs_id_prop.get("id")
        if not discogs_id:
            print(f"Discogs ID missing for page {notion_id}, skipping.")
            continue

        try:
            release = get_discogs_release(discogs_id)
        except requests.exceptions.HTTPError as e:
            print(f"Failed to fetch release {discogs_id}: {e}")
            continue

        # Parse format details
        format_str = release.get("format", "")
        if isinstance(format_str, list):
            format_str = ", ".join(format_str)
        format_size, format_speed, format_remaining = parse_format_details(format_str)

        # Build properties payload
        properties = {
            "Title": safe_rich_text(release.get("title")),
            "Country": safe_rich_text(release.get("country")),
            "Folder": safe_select(release.get("folder_name")),
            "ValueLow": safe_number(release.get("value_low")),
            "ValueMid": safe_number(release.get("value_mid")),
            "ValueHigh": safe_number(release.get("value_high")),
            "FormatSize": safe_rich_text(format_size),
            "FormatSpeed": safe_rich_text(format_speed),
            "FormatDetails": safe_rich_text(format_remaining),
        }

        update_page(notion_id, properties)
        time.sleep(1)  # throttle to avoid Discogs API limits

if __name__ == "__main__":
    main()
