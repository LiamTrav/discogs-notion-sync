import os
import requests
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
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}


# -----------------------------
# GET DISCOGS COLLECTION
# -----------------------------
def get_discogs_collection():
    releases = []

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


# -----------------------------
# CHECK IF PAGE EXISTS
# -----------------------------
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
        return False

    results = response.json().get("results", [])
    return len(results) > 0


# -----------------------------
# CREATE NOTION PAGE
# -----------------------------
def create_notion_page(release):
    basic = release["basic_information"]

    discogs_id = basic.get("id")
    title = basic.get("title", "")
    artists = ", ".join(a["name"] for a in basic.get("artists", []))
    labels = ", ".join(l["name"] for l in basic.get("labels", []))
    year = basic.get("year")
    country = basic.get("country")
    formats = basic.get("formats", [])

    format_size = formats[0]["name"] if formats else None
    format_speed = None
    format_details = None

    if formats and "descriptions" in formats[0]:
        descriptions = formats[0]["descriptions"]
        format_details = ", ".join(descriptions)

    value = release.get("estimated_value", {})
    value_low = value.get("value_low")
    value_mid = value.get("value")
    value_high = value.get("value_high")

    date_added = release.get("date_added")
    folder_id = release.get("folder_id")

    # Convert Notes properly
    notes_raw = release.get("notes", [])
    if isinstance(notes_raw, list):
        notes = " | ".join(n.get("value", "") for n in notes_raw)
    else:
        notes = str(notes_raw)

    properties = {
        "Discogs ID": {"number": discogs_id},
        "Title": {"title": [{"text": {"content": title}}]},
        "Artist": {"rich_text": [{"text": {"content": artists}}]},
        "Label": {"rich_text": [{"text": {"content": labels}}]},
        "Year": {"number": year} if year else None,
        "ValueLow": {"number": value_low} if value_low else None,
        "ValueMid": {"number": value_mid} if value_mid else None,
        "ValueHigh": {"number": value_high} if value_high else None,
        "Country": {"select": {"name": country}} if country else None,
        "FormatSize": {"select": {"name": format_size}} if format_size else None,
        "FormatSpeed": {"select": {"name": format_speed}} if format_speed else None,
        "FormatDetails": {"rich_text": [{"text": {"content": format_details}}]} if format_details else None,
        "Added": {"date": {"start": date_added}} if date_added else None,
        "Folder": {"select": {"name": str(folder_id)}} if folder_id else None,
        "Notes": {"rich_text": [{"text": {"content": notes}}]} if notes else None,
    }

    # Remove None values
    properties = {k: v for k, v in properties.items() if v is not None}

    payload = {
        "parent": {"database_id": DATABASE_ID},
        "properties": properties
    }

    response = requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers_notion,
        json=payload
    )

    if response.status_code != 200:
        print("Failed to create page:")
        print(response.status_code)
        print(response.text)


# -----------------------------
# SYNC
# -----------------------------
def sync():
    releases = get_discogs_collection()
    print(f"Found {len(releases)} releases in Discogs collection")

    for release in releases:
        discogs_id = release["basic_information"]["id"]

        if not notion_page_exists(discogs_id):
            create_notion_page(release)
            print(f"Created {discogs_id}")
            time.sleep(0.5)
        else:
            print(f"Skipped {discogs_id} (already exists)")


if __name__ == "__main__":
    sync()
