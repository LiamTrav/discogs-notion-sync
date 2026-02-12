import re
import time
from notion_client import Client
import discogs_client

# --- CONFIG ---
NOTION_TOKEN = "YOUR_NOTION_TOKEN"
NOTION_DATABASE_ID = "YOUR_DATABASE_ID"
DISCOGS_TOKEN = "YOUR_DISCOGS_TOKEN"

# --- INIT CLIENTS ---
notion = Client(auth=NOTION_TOKEN)
discogs = discogs_client.Client('discogs-notion-sync', user_token=DISCOGS_TOKEN)

# --- HELPERS ---
def split_format_details(details):
    size = speed = ""
    remaining = []
    if details:
        parts = [p.strip() for p in details.split(",")]
        for part in parts:
            if re.match(r'^\d+"$', part):
                size = part
            elif re.match(r'^\d+\s*RPM$', part, re.IGNORECASE):
                speed = part
            else:
                remaining.append(part)
    return size, speed, ", ".join(remaining)

def safe_number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0

def get_folder_name(page):
    folder = page.get("Folder", {})
    # If Folder is already a name, return it; else empty string
    return folder.get("name") if folder else ""

# --- FETCH RECORDS FROM NOTION ---
def get_notion_pages():
    pages = []
    start_cursor = None
    while True:
        response = notion.databases.query(
            database_id=NOTION_DATABASE_ID,
            start_cursor=start_cursor,
            page_size=100
        )
        pages.extend(response["results"])
        if response.get("has_more"):
            start_cursor = response.get("next_cursor")
        else:
            break
    return pages

# --- UPDATE NOTION ---
def update_notion_page(page_id, record):
    format_size, format_speed, format_details = split_format_details(record.get("FormatDetails", ""))

    properties = {
        "Title": {"title": [{"text": {"content": record.get("Title", "")}}]},
        "Country": {"rich_text": [{"text": {"content": record.get("Country", "")}}]},
        "ValueLow": {"number": safe_number(record.get("ValueLow"))},
        "ValueMid": {"number": safe_number(record.get("ValueMid"))},
        "ValueHigh": {"number": safe_number(record.get("ValueHigh"))},
        "Discogs ID": {"number": int(record.get("DiscogsID", 0))},
        "Folder": {"rich_text": [{"text": {"content": record.get("FolderName", "")}}]},
        "FormatDetails": {"rich_text": [{"text": {"content": format_details}}]},
        "FormatSpeed": {"rich_text": [{"text": {"content": format_speed}}]},
        "FormatSize": {"rich_text": [{"text": {"content": format_size}}]},
        "Notes": {"rich_text": [{"text": {"content": record.get("Notes", "")}}]} if record.get("Notes") else {"rich_text": []}
    }

    try:
        notion.pages.update(page_id=page_id, properties=properties)
        print(f"Updated {record.get('Title', 'Unknown')} ({page_id})")
    except Exception as e:
        print(f"Failed {record.get('Title', 'Unknown')} ({page_id}): {e}")

# --- MAIN FLOW ---
notion_pages = get_notion_pages()

for page in notion_pages:
    props = page.get("properties", {})
    discogs_id = props.get("Discogs ID", {}).get("number")

    if not discogs_id:
        print(f"Skipping page {page['id']} — no Discogs ID")
        continue

    try:
        release = discogs.release(discogs_id)
    except Exception as e:
        print(f"Failed to fetch Discogs ID {discogs_id}: {e}")
        continue

    record = {
        "Title": release.title,
        "Country": release.country or "",
        "ValueLow": getattr(release, "lowest_price", 0),
        "ValueMid": getattr(release, "median_price", 0),
        "ValueHigh": getattr(release, "highest_price", 0),
        "DiscogsID": discogs_id,
        "FolderName": get_folder_name(props),
        "FormatDetails": ", ".join([f"{f['qty']} x {f['name']}" if isinstance(f, dict) else str(f) for f in release.formats]),
        "Notes": getattr(release, "notes", "")
    }

    update_notion_page(page["id"], record)
    time.sleep(1)  # rate-limit safety
