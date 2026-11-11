"""
Notes TRIAGE (read-only).

Reads every Notion record that has text in Notes and PROPOSES how it should be
split into:
  - Keep        (a 'Play A' / 'Play B' / 'Play!' line means you are keeping it)
  - Private     (the play lines)
  - Public      (everything else, suitable for the Discogs listing comments)
  - Review      (notes about your copy differing from the linked release,
                 e.g. 'Not this version', 'My version has...').
                 For these, Notes are LEFT UNCHANGED and Private Notes would
                 simply be set to 'REVIEW' so you can filter on it in Notion.

This script contains NO code that writes to Notion or Discogs. It only reads,
prints a report, and saves notes_triage.csv for you to open in a spreadsheet.
"""

import csv
import os
import re
import time
from collections import defaultdict

import requests
from requests.exceptions import RequestException

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
NOTION_BASE = "https://api.notion.com/v1"

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

CSV_PATH = "notes_triage.csv"

# ---------------------------------------------------
# RULES (edit these if the proposals look wrong)
# ---------------------------------------------------

# A whole line that is just 'Play', 'Play A', 'Play B', 'Play side A', 'Play!'
# Deliberately does NOT match words like 'Playable' or 'very playable'.
PLAY_LINE = re.compile(r"^play(\s+side)?(\s+[ab])?\s*[!.]?$", re.IGNORECASE)

# Lines that compare your copy with the linked Discogs release
VERSION_LINE = re.compile(
    r"^(not this version|my version|mine is|same runouts?|does not have)",
    re.IGNORECASE,
)
VERSION_WORDS = re.compile(r"\b(this version|this release|my version)\b", re.IGNORECASE)

# ---------------------------------------------------
# NOTION HELPERS
# ---------------------------------------------------

def notion_request(method, url, payload=None, retries=5):
    for attempt in range(retries):
        try:
            r = requests.request(method, url, headers=headers_notion, json=payload, timeout=30)

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 2))
                print(f"NOTION RATE LIMITED - sleeping {retry}s")
                time.sleep(retry)
                continue

            if r.status_code >= 500:
                wait = 2 ** attempt
                print(f"NOTION SERVER ERROR {r.status_code} - retrying in {wait}s")
                time.sleep(wait)
                continue

            if not r.ok:
                print(f"NOTION ERROR {r.status_code}: {r.text}")
                return None

            return r

        except RequestException as e:
            wait = 2 ** attempt
            print(f"NOTION REQUEST EXCEPTION: {e} - retrying in {wait}s")
            time.sleep(wait)

    print(f"NOTION REQUEST FAILED AFTER {retries} ATTEMPTS: {url}")
    return None


def plain(rich_text):
    return "".join(part.get("plain_text", "") for part in (rich_text or []))


def fetch_pages_with_notes():
    pages = []
    has_more = True
    cursor = None

    while has_more:
        payload = {
            "page_size": 100,
            "filter": {"property": "Notes", "rich_text": {"is_not_empty": True}},
        }
        if cursor:
            payload["start_cursor"] = cursor

        r = notion_request("POST", f"{NOTION_BASE}/databases/{DATABASE_ID}/query", payload)
        if not r:
            raise Exception("Failed fetching Notion pages")

        data = r.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more")
        cursor = data.get("next_cursor")

    return pages

# ---------------------------------------------------
# CLASSIFICATION
# ---------------------------------------------------

def classify(notes):
    lines = [l.strip() for l in notes.splitlines() if l.strip()]
    play, version, public = [], [], []

    for line in lines:
        core = line.strip("\"' ").strip()  # tolerate stray quote marks
        if PLAY_LINE.match(core):
            play.append(line)
        elif VERSION_LINE.match(core) or VERSION_WORDS.search(core):
            version.append(line)
        else:
            public.append(line)

    return play, version, public

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    print("=" * 60)
    print("NOTES TRIAGE - read-only, nothing will be changed")
    print("=" * 60)

    pages = fetch_pages_with_notes()
    print(f"\nRecords with Notes: {len(pages)}")

    rows = []
    by_category = defaultdict(list)
    watch = []

    for page in pages:
        props = page["properties"]

        title = plain(props.get("Title", {}).get("title"))
        artist = plain(props.get("Artist", {}).get("rich_text"))
        label = f"{artist} - {title}".strip(" -") or page["id"]
        instance_id = props.get("Instance ID", {}).get("number")
        notes = plain(props.get("Notes", {}).get("rich_text"))

        play, version, public = classify(notes)
        keep = bool(play)

        if version:
            category = "REVIEW (copy differs from linked release)"
            proposed_private = "REVIEW"  # marker only, so you can filter on it in Notion
            proposed_public = notes      # Notes are left exactly as they are
        elif keep and public:
            category = "KEEP + public text remains"
            proposed_private = "\n".join(play)
            proposed_public = "\n".join(public)
        elif keep:
            category = "KEEP only (Notes would become empty)"
            proposed_private = "\n".join(play)
            proposed_public = ""
        else:
            category = "PUBLIC only (no change)"
            proposed_private = ""
            proposed_public = "\n".join(public)

        # Highlight notes mentioning 'play' that are NOT keepers (e.g. 'Playable')
        if not keep and re.search(r"play", notes, re.IGNORECASE):
            watch.append((label, notes))

        row = {
            "category": category,
            "keep": "YES" if keep else "",
            "label": label,
            "instance_id": instance_id,
            "original_notes": notes,
            "proposed_private": proposed_private,
            "proposed_public": proposed_public,
            "page_url": page.get("url", ""),
        }
        rows.append(row)
        by_category[category].append(row)

    print("\n--- SUMMARY ---")
    for category in sorted(by_category):
        print(f"{len(by_category[category]):4d}  {category}")
    print(f"Records that would be ticked Keep: {sum(1 for r in rows if r['keep'])}")

    for category in sorted(by_category):
        print("\n" + "=" * 60)
        print(f"{category}  ({len(by_category[category])})")
        print("=" * 60)
        for row in by_category[category]:
            print(f"* {row['label']}  [instance {row['instance_id']}]")
            print(f"    notes: {row['original_notes']!r}")
            if row["proposed_private"] or row["proposed_public"]:
                print(f"    -> private: {row['proposed_private']!r}")
                print(f"    -> public:  {row['proposed_public']!r}")

    if watch:
        print("\n" + "=" * 60)
        print("WATCH: contain the letters 'play' but are NOT keepers")
        print("(a simple 'Notes contains Play' filter would wrongly catch these)")
        print("=" * 60)
        for label, notes in watch:
            print(f"* {label}: {notes!r}")

    with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["category"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nSaved {CSV_PATH}. Nothing was written to Notion or Discogs.")


if __name__ == "__main__":
    main()
