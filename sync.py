import os
import requests
import re

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


def get_discogs_release(discogs_id):
    url = f"https://api.discogs.com/releases/{discogs_id}"
    response = requests.get(url, headers=DISCOGS_HEADERS)
    response.raise_for_status()
    return response.json()


def get_folder_map():
    """
    Fetch all Discogs folders and return {folder_id: folder_name}
    """
    url = f"https://api.discogs.com/users/{USERNAME}/collection/folders"
    response = requests.get(url, headers=DISCOGS_HEADERS)
    response.raise_for_status()
    data = response.json()

    folder_map = {}
    for folder in data.get("folders", []):
        folder_map[folder["id"]] = folder["name"]

    return folder_map


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


def update_page(notion_id, country, format_size, format_speed, format_details, folder_name):
    data = {
        "properties": {
            "Country": {"rich_text": [{"text": {"content": country or ""}}]},
            "FormatSize": {"rich_text": [{"text": {"content": format_size or ""}}]},
            "FormatSpeed": {"rich_text": [{"text": {"content": format_speed or ""}}]},
            "FormatDetails": {"rich_text": [{"text": {"content": format_details or ""}}]},
            "Folder": {"rich_text": [{"text": {"content": folder_name or ""}}]},
        }
    }

    response = requests.patch(
        f"{NOTION_API_URL}/{notion_id}",
        headers=NOTION_HEADERS,
        json=data
    )

    if response.status_code != 200:
        print(f"Failed to update {notion_id}: {response.status_code} {response.text}")


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

            folder_prop = props.get("Folder", {})
            folder_id = None

            # If Folder currently stores ID as number
            if folder_prop.get("number") is not None:
                folder_id = folder_prop.get("number")

            if not discogs_id:
                print(f"Skipping page {notion_id} (no Discogs ID)")
                continue

            try:
                release = get_discogs_release(discogs_id)
                country = release.get("country")

                format_list = release.get("formats", [])
                fmt_size, fmt_speed, fmt_details = parse_format_details(format_list)

                folder_name = folder_map.get(folder_id, "")

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
