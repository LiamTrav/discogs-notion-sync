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

# ---------------------------------------------------
# DISCOGS
# ---------------------------------------------------

def get_folder_map():
    url = f"https://api.discogs.com/users/{USERNAME}/collection/folders"
    r = requests.get(url, headers=DISCOGS_HEADERS)
    r.raise_for_status()
    data = r.json()
    return {f["id"]: f["name"] for f in data["folders"]}


def get_full_collection():
    collection = {}
    page = 1

    while True:
        url = f"https://api.discogs.com/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page=100"
        r = requests.get(url, headers=DISCOGS_HEADERS)
        r.raise_for_status()
        data = r.json()

        for item in data["releases"]:
            discogs_id = item["id"]
            collection[discogs_id] = {
                "folder_id": item["folder_id"],
                "basic_information": item["basic_information"]
            }

        if page >= data["pagination"]["pages"]:
            break

        page += 1
        time.sleep(1)

    print("Collection length:", len(collection))
    return collection




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


def get_catno(basic_info):
    labels = basic_info.get("labels", [])
    catnos = []

    for label in labels:
        catno = label.get("catno")
        if catno and catno.lower() != "none":
            catnos.append(catno.strip())

    return ", ".join(catnos)

# ---------------------------------------------------
# NOTION
# ---------------------------------------------------

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


def update_page(notion_id, country, size, speed, details, folder_name, catno):
    properties = {
        "Country": {"rich_text": [{"text": {"content": country or ""}}]},
        "FormatSize": {"rich_text": [{"text": {"content": size or ""}}]},
        "FormatSpeed": {"rich_text": [{"text": {"content": speed or ""}}]},
        "FormatDetails": {"rich_text": [{"text": {"content": details or ""}}]},
        "CatNo": {"rich_text": [{"text": {"content": catno or ""}}]},
    }

    if folder_name:
        properties["Folder"] = {"select": {"name": folder_name}}

    requests.patch(
        f"{NOTION_API_URL}/{notion_id}",
        headers=NOTION_HEADERS,
        json={"properties": properties},
    )


def create_page(discogs_id, basic_info, folder_name, catno):
    size, speed, details = parse_format_details(basic_info.get("formats", []))

    properties = {
        "Title": {
            "title": [
                {"text": {"content": basic_info.get("title", "Unknown")}}
            ]
        },
        "Discogs ID": {"number": discogs_id},
        "Country": {"rich_text": [{"text": {"content": basic_info.get("country", "")}}]},
        "FormatSize": {"rich_text": [{"text": {"content": size or ""}}]},
        "FormatSpeed": {"rich_text": [{"text": {"content": speed or ""}}]},
        "FormatDetails": {"rich_text": [{"text": {"content": details or ""}}]},
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

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    print("Loading Discogs collection...")
    collection = get_full_collection()

    print("Loading folder map...")
    folder_map = get_folder_map()

    print("Loading Notion pages...")
    notion_pages = get_notion_pages()

    for discogs_id, item in collection.items():

        folder_name = folder_map.get(item["folder_id"])
        basic_info = item["basic_information"]
        catno = get_catno(basic_info)

        size, speed, details = parse_format_details(basic_info.get("formats", []))

        if discogs_id in notion_pages:
            update_page(
                notion_pages[discogs_id],
                basic_info.get("country"),
                size,
                speed,
                details,
                folder_name,
                catno
            )
        else:
            create_page(
                discogs_id,
                basic_info,
                folder_name,
                catno
            )

        time.sleep(0.5)


if __name__ == "__main__":
    main()
