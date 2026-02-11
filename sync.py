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


def get_discogs_collection():
    releases = []
    page = 1

    while True:
        url = f"https://api.discogs.com/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page=100"
        response = requests.get(url, headers=headers_discogs)
        data = response.json()
        if response.status_code != 200:
            print("Discogs API failed:")
            print(response.status_code)
            print(response.text)
            exit(1)


        releases.extend(data["releases"])

        if page >= data["pagination"]["pages"]:
            break

        page += 1
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
    return data["results"][0]["id"] if data["results"] else None


def create_notion_entry(release):
    info = release["basic_information"]

    artist = info["artists"][0]["name"]
    title = info["title"]
    year = info.get("year")
    label = info["labels"][0]["name"] if info.get("labels") else None
    country = info.get("country")
    formats = info.get("formats", [])

    format_size = formats[0].get("name") if formats else None
    format_details = ", ".join(formats[0].get("descriptions", [])) if formats else None

    added_date = release.get("date_added")

    data = {
        "parent": {"database_id": DATABASE_ID},
        "properties": {
            "Title": {
                "title": [{"text": {"content": title}}]
            },
            "Artist": {
                "rich_text": [{"text": {"content": artist}}]
            },
            "Year": {
                "number": year
            },
            "Discogs ID": {
                "number": release["id"]
            },
            "Label": {
                "rich_text": [{"text": {"content": label}}] if label else {"rich_text": []}
            },
            "Country": {
                "select": {"name": country} if country else None
            },
            "FormatSize": {
                "select": {"name": format_size} if format_size else None
            },
            "FormatDetails": {
                "rich_text": [{"text": {"content": format_details}}] if format_details else {"rich_text": []}
            },
            "Added": {
                "date": {"start": added_date} if added_date else None
            }
        }
    }

    # Remove null properties (Notion rejects them)
    data["properties"] = {
        k: v for k, v in data["properties"].items() if v is not None
    }

    response = requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers_notion,
        json=data
    )

    if response.status_code != 200:
        print("Failed to create page:")
        print(response.status_code)
        print(response.text)


def sync():
    releases = get_discogs_collection()

    for release in releases:
        discogs_id = release["id"]
        existing_page = notion_page_exists(discogs_id)

        if not existing_page:
            create_notion_entry(release)


if __name__ == "__main__":
    sync()
