import os
import requests
from notion_client import Client
import time
import re

# ----------------- CONFIG -----------------
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DB_ID = os.environ.get("NOTION_DB_ID")
DISCOGS_TOKEN = os.environ.get("DISCOGS_TOKEN")  # for Discogs API
# ----------------------------------------

notion = Client(auth=NOTION_TOKEN)

# ----------------- HELPERS -----------------
def get_discogs_release(discogs_id):
    headers = {"Authorization": f"Discogs token={DISCOGS_TOKEN}"}
    url = f"https://api.discogs.com/releases/{discogs_id}"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

def get_release_value(discogs_id):
    url = f"https://api.discogs.com/marketplace/price_suggestions/{discogs_id}"
    headers = {"Authorization": f"Discogs token={DISCOGS_TOKEN}"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return None, None, None
    data = r.json()
    def extract(val):
        if isinstance(val, dict):
            return val.get("value")
        elif isinstance(val, (int, float)):
            return val
        return None
    return extract(data.get("low")), extract(data.get("mid")), extract(data.get("high"))

def number_field(value):
    return {"number": value} if isinstance(value, (int, float)) else None

def split_format_details(format_str):
    if not format_str:
        return None, None, None
    size = None
    speed = None
    rest_parts = []
    parts = [p.strip() for p in format_str.split(",")]
    for p in parts:
        if re.match(r"^\d+\"$", p):
            size = p
        elif re.match(r"^\d+\s*RPM$", p, re.IGNORECASE):
            speed = p
        else:
            rest_parts.append(p)
    rest = ", ".join(rest_parts) if rest_parts else None
    return size, speed, rest

def build_properties(release):
    # Extract fields safely
    discogs_id = release.get("id")
    title = release.get("title")
    country = release.get("country")
    folder_id = release.get("folder", {}).get("id") if release.get("folder") else None
    folder_name = release.get("folder", {}).get("name") if release.get("folder") else None
    format_details_raw = release.get("format_details")
    format_size, format_speed, format_details = split_format_details(format_details_raw)
    value_low, value_mid, value_high = get_release_value(discogs_id)
    notes = release.get("notes")

    props = {
        "Discogs ID": {"number": discogs_id},
        "Name": {"title": [{"text": {"content": title}}]} if title else None,
        "Country": {"rich_text": [{"text": {"content": country}}]} if country else None,
        "Folder": {"rich_text": [{"text": {"content": folder_name}}]} if folder_name else None,
        "FormatDetails": {"rich_text": [{"text": {"content": format_details}}]} if format_details else None,
        "FormatSize": {"rich_text": [{"text": {"content": format_size}}]} if format_size else None,
        "FormatSpeed": {"rich_text": [{"text": {"content": format_speed}}]} if format_speed else None,
        "ValueLow": number_field(value_low),
        "ValueMid": number_field(value_mid),
        "ValueHigh": number_field(value_high),
        "Notes": {"rich_text": [{"text": {"content": notes}}]} if notes else None,
    }

    # Remove None fields
    return {k: v for k, v in props.items() if v is not None}

def update_notion_page(page_id, properties):
    notion.pages.update(page_id=page_id, properties=properties)

# ----------------- MAIN -----------------
def main():
    # Paginate through Notion database
    next_cursor = None
    while True:
        resp = notion.databases.query(
            **{
                "database_id": NOTION_DB_ID,
                "start_cursor": next_cursor,
                "page_size": 100,
            }
        )
        for page in resp.get("results", []):
            page_id = page["id"]
            discogs_id = page["properties"].get("Discogs ID", {}).get("number")
            if not discogs_id:
                print(f"Skipping page {page_id}, no Discogs ID")
                continue
            try:
                release = get_discogs_release(discogs_id)
                props = build_properties(release)
                update_notion_page(page_id, props)
                print(f"Updated page {page_id}")
                time.sleep(0.25)  # gentle rate limiting
            except Exception as e:
                print(f"Failed to update page {page_id}: {e}")
        next_cursor = resp.get("next_cursor")
        if not next_cursor:
            break

if __name__ == "__main__":
    main()
