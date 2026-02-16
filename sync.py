import os
import requests
import time

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "DiscogsNotionSync/2.0"
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}


# -----------------------------
# DISCOGS: FOLDERS
# -----------------------------
def get_folder_map():
    url = f"https://api.discogs.com/users/{USERNAME}/collection/folders"
    r = requests.get(url, headers=headers_discogs)
    r.raise_for_status()
    data = r.json()
    return {f["id"]: f["name"] for f in data.get("folders", [])}


# -----------------------------
# DISCOGS: COLLECTION
# -----------------------------
def get_discogs_collection():
    releases = []

    url = f"https://api.discogs.com/users/{USERNAME}/collection/folders/0/releases?page=1&per_page=100"
    r = requests.get(url, headers=headers_discogs)
    r.raise_for_status()
    data = r.json()

    total_pages = data.get("pagination", {}).get("pages", 1)
    releases.extend(data.get("releases", []))

    print(f"Total pages: {total_pages}")

    for page in range(2, total_pages + 1):
        url = f"https://api.discogs.com/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page=100"
        r = requests.get(url, headers=headers_discogs)
        r.raise_for_status()
        data = r.json()
        releases.extend(data.get("releases", []))
        print(f"Fetched page {page}")
        time.sleep(1)

    return releases


# -----------------------------
# DISCOGS: MARKET STATS
# -----------------------------
def get_release_value(release_id):
    url = f"https://api.discogs.com/marketplace/stats/{release_id}"
    r = requests.get(url, headers=headers_discogs)

    if r.status_code != 200:
        return None, None, None

    data = r.json()

    lowest = data.get("lowest_price")
    median = data.get("median_price")
    highest = data.get("highest_price")

    return lowest, median, highest


# -----------------------------
# NOTION: FIND PAGE
# -----------------------------
def find_notion_page(discogs_id):
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"

    payload = {
        "filter": {
            "property": "Discogs ID",
            "number": {"equals": discogs_id}
        }
    }

    r = requests.post(url, headers=headers_notion, json=payload)
    r.raise_for_status()

    results = r.json().get("results", [])
    return results[0]["id"] if results else None


# -----------------------------
# BUILD NOTION PROPERTIES
# -----------------------------
def build_properties(release, folder_map):
    basic = release["basic_information"]

    discogs_id = basic.get("id")
    title = basic.get("title", "")
    artists = ", ".join(a["name"] for a in basic.get("artists", []))
    labels = ", ".join(l["name"] for l in basic.get("labels", []))
    year = basic.get("year")
    country = basic.get("country")
    date_added = release.get("date_added")
    folder_name = folder_map.get(release.get("folder_id"))

    # --- FORMAT PARSING ---
    formats = basic.get("formats", [])
    format_size = None
    format_speed = None
    format_details_list = []

    if formats:
        descriptions = formats[0].get("descriptions", [])

        for desc in descriptions:
            if '"' in desc:
                format_size = desc
            elif "RPM" in desc:
                format_speed = desc
            else:
                format_details_list.append(desc)

    format_details = ", ".join(format_details_list) if format_details_list else None

    # --- VALUES ---
    value_low, value_mid, value_high = get_release_value(discogs_id)
    time.sleep(1)

    # --- NOTES ---
    notes_raw = release.get("notes", [])
    if isinstance(notes_raw, list):
        notes = " | ".join(n.get("value", "") for n in notes_raw)
    else:
        notes = str(notes_raw)

    props = {
        "Discogs ID": {"number": discogs_id},
        "Title": {"title": [{"text": {"content": title}}]},
        "Artist": {"rich_text": [{"text": {"content": artists}}]},
        "Label": {"rich_text": [{"text": {"content": labels}}]},
        "Year": {"number": year} if year else None,
        "Country": {"rich_text": [{"text": {"content": country or ""}}]},
        "FormatSize": {"select": {"name": format_size}} if format_size else None,
        "FormatSpeed": {"select": {"name": format_speed}} if format_speed else None,
        "FormatDetails": {"rich_text": [{"text": {"content": format_details}}]} if format_details else None,
        "ValueLow": {"number": value_low} if value_low else None,
        "ValueMid": {"number": value_mid} if value_mid else None,
        "ValueHigh": {"number": value_high} if value_high else None,
        "Added": {"date": {"start": date_added}} if date_added else None,
        "Folder": {"select": {"name": folder_name}} if folder_name else None,
        "Notes": {"rich_text": [{"text": {"content": notes}}]} if notes else None,
    }

    return {k: v for k, v in props.items() if v is not None}


# -----------------------------
# UPSERT
# -----------------------------
def upsert_release(release, folder_map):
    discogs_id = release["basic_information"]["id"]
    page_id = find_notion_page(discogs_id)
    properties = build_properties(release, folder_map)

    if page_id:
        url = f"https://api.notion.com/v1/pages/{page_id}"
        r = requests.patch(url, headers=headers_notion, json={"properties": properties})
        print(f"Updated {discogs_id}")
    else:
        payload = {
            "parent": {"database_id": DATABASE_ID},
            "properties": properties
        }
        r = requests.post("https://api.notion.com/v1/pages", headers=headers_notion, json=payload)
        print(f"Created {discogs_id}")

    if r.status_code not in (200, 201):
        print("Notion error:")
        print(r.text)


# -----------------------------
# MAIN SYNC
# -----------------------------
def sync():
    folder_map = get_folder_map()
    releases = get_discogs_collection()
    print(f"Processing {len(releases)} releases...")

    for release in releases:
        upsert_release(release, folder_map)
        time.sleep(0.4)


if __name__ == "__main__":
    sync()
