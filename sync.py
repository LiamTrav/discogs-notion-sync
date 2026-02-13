import os
import requests
import re
import time


DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

NOTION_API_URL = "https://api.notion.com/v1/pages"
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

DISCOGS_HEADERS = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": f"{USERNAME} discogs-notion-sync"
}


def get_discogs_release(discogs_id, retries=3):
    url = f"https://api.discogs.com/releases/{discogs_id}"

    for attempt in range(retries):
        response = requests.get(url, headers=DISCOGS_HEADERS)

        if response.status_code == 404:
            print(f"Release {discogs_id} not found (404). Skipping.")
            return None

        if response.status_code in (500, 502, 503, 504):
            wait = 2 ** attempt
            print(f"Discogs server error {response.status_code} for {discogs_id}. Retrying in {wait}s...")
            time.sleep(wait)
            continue

        response.raise_for_status()
        return response.json()

    print(f"Failed to fetch release {discogs_id} after retries.")
    return None



def get_folder_map():
    url = f"https://api.discogs.com/users/{USERNAME}/collection/folders"
    response = requests.get(url, headers=DISCOGS_HEADERS)
    response.raise_for_status()
    data = response.json()

    return {folder["id"]: folder["name"] for folder in data.get("folders", [])}


def parse_format_details(format_list):
    if not format_list:
        return "", "", ""

    fmt = format_list[0]
    descriptions = fmt.get("descriptions", [])

    size = ""
    speed = ""
    remaining = []

    for desc in descriptions:
        desc_clean = desc.strip()

        if re.match(r'^\d+"$', desc_clean):
            size = desc_clean
        elif re.match(r'^\d+\s*RPM$', desc_clean, re.IGNORECASE):
            speed = desc_clean
        else:
            remaining.append(desc_clean)

    details = ", ".join(remaining)
    return size, speed, details


def update_page(notion_id, country, format_size, format_speed, format_details, folder_name, retries=3):
    properties = {
        "Country": {"rich_text": [{"text": {"content": country or ""}}]},
        "FormatSize": {"rich_text": [{"text": {"content": format_size or ""}}]},
        "FormatSpeed": {"rich_text": [{"text": {"content": format_speed or ""}}]},
        "FormatDetails": {"rich_text": [{"text": {"content": format_details or ""}}]},
    }

    if folder_name:
        properties["Folder"] = {"select": {"name": folder_name}}

    data = {"properties": properties}

    for attempt in range(retries):
        response = requests.patch(
            f"{NOTION_API_URL}/{notion_id}",
            headers=NOTION_HEADERS,
            json=data
        )

        if response.status_code in (500, 502, 503, 504):
            wait = 2 ** attempt
            print(f"Notion server error {response.status_code}. Retrying in {wait}s...")
            time.sleep(wait)
            continue

        if response.status_code != 200:
            print(f"Failed to update {notion_id}: {response.status_code} {response.text}")
        return

    print(f"Failed to update {notion_id} after retries.")



def main():
    folder_map = get_folder_map()

    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    has_more = True
    next_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if next_cursor:
            payload["start_cursor"] = next_cursor

        response = requests.post(url, headers=NOTION_HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()

        for page in data.get("results", []):
            notion_id = page["id"]
            props = page["properties"]

            discogs_id_prop = props.get("Discogs ID", {})
            discogs_id = discogs_id_prop.get("number")

            if not discogs_id:
                continue

            try:
                release = get_discogs_release(discogs_id)
                if not release:
							    continue
                country = release.get("country")

                format_list = release.get("formats", [])
                fmt_size, fmt_speed, fmt_details = parse_format_details(format_list)

                # 🔴 FIX: derive folder from collection endpoint instead
                folder_name = None
                collection_url = f"https://api.discogs.com/users/{USERNAME}/collection/releases/{discogs_id}"
                collection_response = requests.get(collection_url, headers=DISCOGS_HEADERS)

                if collection_response.status_code == 200:
                    collection_data = collection_response.json()
                    folder_id = collection_data.get("folder_id")
                    folder_name = folder_map.get(folder_id)

                update_page(
                    notion_id,
                    country,
                    fmt_size,
                    fmt_speed,
                    fmt_details,
                    folder_name
                )

            except requests.exceptions.HTTPError as e:
                print(f"Failed to fetch release {discogs_id}: {e}")

        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")


if __name__ == "__main__":
    main()
