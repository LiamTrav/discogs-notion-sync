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


def get_discogs_release(discogs_id):
    url = f"https://api.discogs.com/releases/{discogs_id}"
    response = requests.get(url, headers={"Authorization": f"Discogs token={DISCOGS_TOKEN}"})
    response.raise_for_status()
    return response.json()


def parse_format_details(format_list):
    """
    Take Discogs 'formats' list and extract:
    - size (7", 12")
    - speed (45 RPM, 33 RPM, etc.)
    - remaining details (e.g. Single, Promo)
    """
    if not format_list:
        return "", "", ""

    # Sometimes multiple formats exist; just take the first one
    fmt = format_list[0]
    descriptions = fmt.get("descriptions", [])  # e.g., ["Single", "Promo"]
    text_parts = []

    # Extract size
    size = ""
    if "format" in fmt:
        size_match = re.search(r'(\d+\"|\d+ inch)', fmt["format"])
        if size_match:
            size = size_match.group(1)

    # Extract speed
    speed = ""
    if "text" in fmt:
        speed_match = re.search(r'(\d+\s*RPM)', fmt["text"], re.IGNORECASE)
        if speed_match:
            speed = speed_match.group(1)

    # Build remaining details
    for desc in descriptions:
        if desc.upper() != "PROMO":  # leave everything except Promo
            text_parts.append(desc)
    # Include text field except for speed
    if "text" in fmt:
        remaining = re.sub(r'\d+\s*RPM', '', fmt["text"], flags=re.IGNORECASE).strip()
        if remaining:
            text_parts.append(remaining)

    details = ", ".join(text_parts)
    return size, speed, details


def update_page(notion_id, country, format_size, format_speed, format_details):
    data = {
        "properties": {
            "Country": {"rich_text": [{"text": {"content": country or ""}}]},
            "FormatSize": {"rich_text": [{"text": {"content": format_size or ""}}]},
            "FormatSpeed": {"rich_text": [{"text": {"content": format_speed or ""}}]},
            "FormatDetails": {"rich_text": [{"text": {"content": format_details or ""}}]},
        }
    }
    response = requests.patch(f"{NOTION_API_URL}/{notion_id}", headers=NOTION_HEADERS, json=data)
    if response.status_code != 200:
        print(f"Failed to update {notion_id}: {response.status_code} {response.text}")


def main():
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
            discogs_id_prop = page["properties"].get("Discogs ID", {})
            discogs_id = discogs_id_prop.get("number")  # Adjust if your property type differs

            if not discogs_id:
                print(f"Skipping page {notion_id} (no Discogs ID)")
                continue

            try:
                release = get_discogs_release(discogs_id)
                country = release.get("country")

                # Parse format details
                format_list = release.get("formats", [])
                fmt_size, fmt_speed, fmt_details = parse_format_details(format_list)

                update_page(notion_id, country, fmt_size, fmt_speed, fmt_details)

            except requests.exceptions.HTTPError as e:
                print(f"Failed to fetch release {discogs_id}: {e}")

        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")


if __name__ == "__main__":
    main()
