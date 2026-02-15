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


# ----------------------------
# Discogs Helpers
# ----------------------------

def get_collection():
    releases = {}
    page = 1

    while True:
        url = f"https://api.discogs.com/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page=100"
        r = requests.get(url, headers=DISCOGS_HEADERS)
        r.raise_for_status()
        data = r.json()

        for item in data["releases"]:
            discogs_id = item["id"]
            folder_id = item["folder_id"]
            releases[discogs_id] = folder_id

        if page >= data["pagination"]["pages"]:
            break

        page += 1
        time.sleep(1)

    return releases


def get_folder_map():
    url = f"https://api.discogs.com/users/{USERNAME}/collection/folders"
    r = requests.get(url, headers=DISCOGS_HEADERS)
    r.raise_for_status()
    data = r.json()
    return {f["id"]: f["name"] for f in data["folders"]}


def get_release(discogs_id, retries=3):
    url = f"https://api.discogs.com/releases/{discogs_id}"

    for attempt in range(retries):
        r = requests.get(url, headers=DISCOGS_HEADERS)

        if r.status_code == 404:
            print(f"Release {discogs_id} not found (404). Skipping.")
            return None

        if r.status_code in (500, 502, 503, 504):
            wait = 2 ** attempt
            print(f"Discogs error {r.status_code} for {discogs_id}. Retrying in {wait}s...")
            time.sleep(wait)
            continue

        r.raise_for_status()
        return r.json()

    print(f"Failed to fetch release {discogs_id} after retries.")
    return None


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


def get_catno(release):
    labels = release.get("labels", [])
    catnos = []

    for label in labels:
        catno = label.get("catno")
        if catno and catno.lower() != "none":
            catnos.append(catno.strip())

    return ", ".join(catnos)


# ----------------------------
# Notion Helpers
# ----------------------------

def get_notion_pages():
    pages = {}
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    has_more = True
    cursor = None

    while has_more:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor

        r = requests.post(url, headers=NOTION_HEADERS, json=payload)
        r.raise_for_status()
        data = r.json()

        for page in data["results"]:
            discogs_id = page["properties"].get("Discogs ID", {}).get("number")
            if discogs_id:
                pages[discogs_id] = page["id"]

        has_more = data["has_more"]
        cursor = data.get("next_cursor")

    return pages


def update_page(notion_id, country, size, speed, details, folder_name, catno, retries=3):
    properties = {
        "Country": {"rich_text": [{"text": {"content": country or ""}}]},
        "FormatSize": {"rich_text": [{"text": {"content": size or ""}}]},
        "FormatSpeed": {"rich_text": [{"text": {"content": speed or ""}}]},
        "FormatDetails": {"rich_text": [{"text": {"content": details or ""}}]},
        "CatNo": {"rich_text": [{"text": {"content": catno or ""}}]},
    }

    if folder_name:
        properties["Folder"] = {"select": {"name": folder_name}}

    for attempt in range(retries):
        r = requests.patch(
            f"{NOTION_API_URL}/{notion_id}",
            headers=NOTION_HEADERS,
            json={"properties": properties},
        )

        if r.status_code in (500, 502, 503, 504):
            wait = 2 ** attempt
            print(f"Notion error {r.status_code}. Retrying in {wait}s...")
            time.sleep(wait)
            continue

        if r.status_code != 200:
            print(f"Failed update {notion_id}: {r.status_code} {r.text}")
        return

    print(f"Failed to update {notion_id} after retries.")


def create_page(discogs_id, release, folder_name, catno):
    properties = {
        "Title": {
            "title": [
                {"text": {"content": release.get("title", "Unknown")}}
            ]
        },
        "Discogs ID": {"number": discogs_id},
        "Country": {"rich_text": [{"text": {"content": release.get("country", "")}}]},
        "CatNo": {"rich_text": [{"text": {"content": catno or ""}}]},
    }

    if folder_name:
        properties["Folder"] = {"select": {"name": folder_name}}

    r = requests.post(
        NOTION_API_URL,
        headers=NOTION_HEADERS,
        json={
            "parent": {"database_id": DATABASE_ID},
            "properties": properties,
        },
    )

    if r.status_code != 200:
        print(f"Failed create {discogs_id}: {r.status_code} {r.text}")


# ----------------------------
# Main Sync
# ----------------------------

def main():
    print("Loading Discogs collection...")
    collection = get_collection()

    print("Loading folder map...")
    folder_map = get_folder_map()

    print("Loading Notion pages...")
    notion_pages = get_notion_pages()

    for discogs_id, folder_id in collection.items():
        release = get_release(discogs_id)
        if not release:
            continue

        folder_name = folder_map.get(folder_id)
        catno = get_catno(release)

        format_list = release.get("formats", [])
        size, speed, details = parse_format_details(format_list)

        if discogs_id in notion_pages:
            update_page(
                notion_pages[discogs_id],
                release.get("country"),
                size,
                speed,
                details,
                folder_name,
                catno
            )
        else:
            create_page(
                discogs_id,
                release,
                folder_name,
                catno
            )

        time.sleep(0.5)


if __name__ == "__main__":
    main()
