import os
import requests
from requests.auth import HTTPBasicAuth

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


def update_page(notion_id, country):
    data = {
        "properties": {
            "Country": {
                "rich_text": [{"text": {"content": country or ""}}]
            }
        }
    }
    response = requests.patch(f"{NOTION_API_URL}/{notion_id}", headers=NOTION_HEADERS, json=data)
    if response.status_code != 200:
        print(f"Failed to update {notion_id}: {response.status_code} {response.text}")


def main():
    # Get all pages from the database
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
            discogs_id = discogs_id_prop.get("number")  # Adjust if needed based on your property type

            if not discogs_id:
                print(f"Skipping page {notion_id} (no Discogs ID)")
                continue

            try:
                release = get_discogs_release(discogs_id)
                country = release.get("country")
                update_page(notion_id, country)
            except requests.exceptions.HTTPError as e:
                print(f"Failed to fetch release {discogs_id}: {e}")

        has_more = data.get("has_more", False)
        next_cursor = data.get("next_cursor")


if __name__ == "__main__":
    main()
