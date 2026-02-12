import requests
import os
import time

# Environment variables
DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

# Headers
HEADERS_NOTION = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

HEADERS_DISCOGS = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": f"{USERNAME}-Discogs-Notion-Sync/1.0"
}

def get_notion_pages():
    """Retrieve all pages from the Notion database."""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    results = []
    has_more = True
    start_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        response = requests.post(url, headers=HEADERS_NOTION, json=payload)
        response.raise_for_status()
        data = response.json()
        results.extend(data["results"])
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
    return results

def get_discogs_release(release_id):
    url = f"https://api.discogs.com/releases/{release_id}"
    response = requests.get(url, headers=HEADERS_DISCOGS)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()

def parse_format_details(details):
    """Split FormatDetails into FormatSize, FormatSpeed, and remaining details."""
    size = None
    speed = None
    remaining = []
    for part in details.split(","):
        part = part.strip()
        if '"' in part or "in" in part:
            size = part
        elif "RPM" in part.upper():
            speed = part
        else:
            remaining.append(part)
    return size, speed, ", ".join(remaining)

def update_notion_page(page_id, properties):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": properties}
    response = requests.patch(url, headers=HEADERS_NOTION, json=payload)
    response.raise_for_status()
    return response.json()

def main():
    pages = get_notion_pages()
    print(f"Found {len(pages)} pages in Notion.")

    for page in pages:
        notion_id = page["id"]
        discogs_id_prop = page["properties"].get("Discogs ID")
        if not discogs_id_prop:
            print(f"Skipping {notion_id}: No Discogs ID")
            continue

        discogs_id = None
        if "number" in discogs_id_prop:
            discogs_id = discogs_id_prop["number"]
        elif "id" in discogs_id_prop:
            discogs_id = discogs_id_prop["id"]

        if not discogs_id:
            print(f"Skipping {notion_id}: Discogs ID not found")
            continue

        release = get_discogs_release(discogs_id)
        if not release:
            print(f"Skipping {notion_id}: Discogs release not found")
            continue

        # Extract fields
        country = release.get("country")
        folder_name = release.get("folder") if release.get("folder") else None
        value_low = release.get("value_low")
        value_mid = release.get("value_mid")
        value_high = release.get("value_high")
        format_details_raw = ", ".join([f"{f.get('name')}" for f in release.get("formats", []) if f.get("name")])
        format_size, format_speed, format_details = parse_format_details(format_details_raw)

        # Prepare properties for update
        properties = {
            "Country": {"rich_text": [{"text": {"content": country}}]} if country else None,
            "Folder": {"rich_text": [{"text": {"content": folder_name}}]} if folder_name else None,
            "ValueLow": {"number": value_low} if value_low else None,
            "ValueMid": {"number": value_mid} if value_mid else None,
            "ValueHigh": {"number": value_high} if value_high else None,
            "FormatDetails": {"rich_text": [{"text": {"content": format_details}}]} if format_details else None,
            "FormatSize": {"rich_text": [{"text": {"content": format_size}}]} if format_size else None,
            "FormatSpeed": {"rich_text": [{"text": {"content": format_speed}}]} if format_speed else None,
        }

        # Remove any None properties to avoid Notion validation errors
        properties_clean = {k: v for k, v in properties.items() if v is not None}

        try:
            update_notion_page(notion_id, properties_clean)
            print(f"Updated {notion_id}")
            time.sleep(0.2)  # avoid rate limit
        except requests.exceptions.RequestException as e:
            print(f"Failed to update {notion_id}: {e}")

if __name__ == "__main__":
    main()
