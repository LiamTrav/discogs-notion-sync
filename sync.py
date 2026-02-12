import requests
import os
import time

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "DiscogsNotionSync/1.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# ------------------------------
# Helper functions
# ------------------------------

def get_discogs_collection():
    releases = []
    page = 1

    # First request to get total pages
    url = f"https://api.discogs.com/users/{USERNAME}/collection/folders/0/releases?page=1&per_page=100"
    response = requests.get(url, headers=headers_discogs)

    if response.status_code != 200:
        print("Discogs API failed:")
        print(response.status_code)
        print(response.text)
        exit(1)

    data = response.json()
    total_pages = data.get("pagination", {}).get("pages", 1)

    print(f"Total pages: {total_pages}")

    releases.extend(data.get("releases", []))
    print(f"Collected page 1 ({len(releases)} releases)")

    # Fetch remaining pages
    for page in range(2, total_pages + 1):
        print(f"Fetching page {page}...")

        url = f"https://api.discogs.com/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page=100"
        response = requests.get(url, headers=headers_discogs)

        if response.status_code != 200:
            print("Discogs API failed:")
            print(response.status_code)
            print(response.text)
            exit(1)

        data = response.json()
        releases.extend(data.get("releases", []))

        print(f"Collected so far: {len(releases)}")

        time.sleep(1)

    return releases


def notion_page_exists(discogs_id):
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": "Discogs ID",
            "number": {
                "equals": discogs_id
            }
        }
    }

    response = requests.post(url, headers=headers_notion, json=payload)

    if response.status_code != 200:
        print("Notion query failed:")
        print(response.status_code)
        print(response.text)
        return None

    data = response.json()
    return data["results"][0]["id"] if data.get("results") else None


def safe_number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def create_notion_entry(release):
    info = release["basic_information"]

    # Core fields
    title = info.get("title")
    artist = info["artists"][0]["name"] if info.get("artists") else None
    discogs_id = release.get("id")
    year = safe_number(info.get("year"))
    label = info["labels"][0]["name"] if info.get("labels") else None
    country = info.get("country")
    formats = info.get("formats", [])
    format_size = formats[0].get("name") if formats else None
    format_details = ", ".join(formats[0].get("descriptions", [])) if formats else None
    added_date = release.get("date_added")

    # Optional additional fields (Values, Folder, FormatSpeed, Notes)
    # Currently set to None; we can extend to fetch from Discogs API later
    value_low = None
    value_mid = None
    value_high = None
    format_speed = None
    folder = None
    notes = release.get("notes")

    data = {
        "parent": {"database_id": DATABASE_ID},
        "properties": {
            "Title": {"title": [{"text": {"content": title}}]} if title else None,
            "Artist": {"rich_text": [{"text": {"content": artist}}]} if artist else None,
            "Discogs ID": {"number": discogs_id} if discogs_id else None,
            "Year": {"number": year} if year is not None else None,
            "Label": {"rich_text": [{"text": {"content": label}}]} if label else None,
            "Country": {"select": {"name": country}} if country else None,
            "FormatSize": {"select": {"name": format_size}} if format_size else None,
            "FormatSpeed": {"select": {"name": format_speed}} if format_speed else None,
            "FormatDetails": {"rich_text": [{"text": {"content": format_details}}]} if format_details else None,
            "Added": {"date": {"start": added_date}} if added_date else None,
            "Folder": {"select": {"name": folder}} if folder else None,
            "ValueLow": {"number": value_low} if value_low else None,
            "ValueMid": {"number": value_mid} if value_mid else None,
            "ValueHigh": {"number": value_high} if value_high else None,
            "Notes": {"rich_text": [{"text": {"content": notes}}]} if notes else None
        }
    }

    # Remove null properties
    data["properties"] = {k: v for k, v in data["properties"].items() if v is not None}

    response = requests.post("https://api.notion.com/v1/pages", headers=headers_notion, json=data)

    if response.status_code != 200:
        print("Failed to create page:")
        print(response.status_code)
        print(response.text)


def sync():
    releases = get_discogs_collection()
    print(f"Found {len(releases)} releases in Discogs collection")

    for release in releases:
        discogs_id = release.get("id")
        existing_page = notion_page_exists(discogs_id)

        if not existing_page:
            create_notion_entry(release)


if __name__ == "__main__":
    sync()
