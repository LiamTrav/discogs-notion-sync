"""
Notes APPLY (writes to Notion only).

Uses the same rules as notes_triage.py and, for every Notion record that has
text in Notes:

  - If a line is 'Play A' / 'Play B' / 'Play!' etc.  -> tick Keep
  - If the record is KEEP (no 'REVIEW' conditions)   -> copy the Play line(s)
                                                        into Private Notes
  - If a note says your copy differs from the linked
    release ('Not this version', 'My version...')    -> set Private Notes to
                                                        'REVIEW'
  - Anything else                                    -> no change

SAFETY
  * Writes ONLY the 'Keep' and 'Private Notes' properties. Nothing else.
  * Notes are NEVER changed or cleared (here or on Discogs).
  * Never unticks Keep.
  * Never overwrites a Private Notes value that already has content; those
    records are skipped and listed in the report.
  * Safe to re-run: records already processed are left alone.
  * Set DRY_RUN=true to see what would be written without writing anything.
"""

import os
import re
import time

import requests
from requests.exceptions import RequestException

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
NOTION_BASE = "https://api.notion.com/v1"

DRY_RUN = os.environ.get("DRY_RUN", "false").strip().lower() == "true"
MAX_WRITES = int(os.environ.get("MAX_WRITES", "0"))  # 0 = no cap

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ---------------------------------------------------
# RULES (identical to notes_triage.py)
# ---------------------------------------------------

PLAY_LINE = re.compile(r"^play(\s+side)?(\s+[ab])?\s*[!.]?$", re.IGNORECASE)

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


def fetch_schema():
    r = notion_request("GET", f"{NOTION_BASE}/databases/{DATABASE_ID}")
    if not r:
        raise Exception("Could not read Notion database schema")
    return r.json().get("properties", {})


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
        core = line.strip("\"' ").strip()
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
    print("NOTES APPLY - " + ("DRY RUN (nothing will be written)" if DRY_RUN else "LIVE (writing to Notion)"))
    print("=" * 60)

    schema = fetch_schema()

    problems = []
    if schema.get("Keep", {}).get("type") != "checkbox":
        problems.append("'Keep' must exist and be a Checkbox property")
    if schema.get("Private Notes", {}).get("type") != "rich_text":
        problems.append("'Private Notes' must exist and be a Text property")
    if schema.get("Notes", {}).get("type") != "rich_text":
        problems.append("'Notes' must exist and be a Text property")

    if problems:
        print("\nSTOPPING:")
        for p in problems:
            print(f"  - {p}")
        raise SystemExit(1)

    pages = fetch_pages_with_notes()
    print(f"\nRecords with Notes: {len(pages)}")

    keep_ticked = 0
    play_copied = 0
    review_marked = 0
    already_keep = 0
    skipped_private_exists = []
    no_change = 0
    failed = 0
    writes = 0

    for page in pages:
        props = page["properties"]

        title = plain(props.get("Title", {}).get("title"))
        artist = plain(props.get("Artist", {}).get("rich_text"))
        label = f"{artist} - {title}".strip(" -") or page["id"]

        notes = plain(props["Notes"].get("rich_text"))
        existing_private = plain(props["Private Notes"].get("rich_text")).strip()
        keep_now = bool(props["Keep"].get("checkbox"))

        play, version, public = classify(notes)
        want_keep = bool(play)

        if version:
            want_private = "REVIEW"
        elif play:
            want_private = "\n".join(play)
        else:
            want_private = ""

        update = {}
        actions = []

        if want_keep and not keep_now:
            update["Keep"] = {"checkbox": True}
            actions.append("tick Keep")
        elif want_keep and keep_now:
            already_keep += 1

        if want_private:
            if existing_private:
                skipped_private_exists.append((label, existing_private, want_private))
            else:
                update["Private Notes"] = {"rich_text": [{"text": {"content": want_private}}]}
                actions.append(f"Private Notes = {want_private!r}")

        if not update:
            no_change += 1
            continue

        if MAX_WRITES and writes >= MAX_WRITES:
            print(f"Reached MAX_WRITES={MAX_WRITES}; stopping.")
            break

        print(f"* {label}: " + "; ".join(actions))

        if DRY_RUN:
            writes += 1
        else:
            r = notion_request("PATCH", f"{NOTION_BASE}/pages/{page['id']}", {"properties": update})
            time.sleep(0.35)
            if not r:
                failed += 1
                print(f"    FAILED: {page['id']}")
                continue
            writes += 1

        if "Keep" in update:
            keep_ticked += 1
        if "Private Notes" in update:
            if want_private == "REVIEW":
                review_marked += 1
            else:
                play_copied += 1

    print("\n" + "=" * 60)
    print("SUMMARY" + (" (dry run - nothing written)" if DRY_RUN else ""))
    print("=" * 60)
    print(f"Records examined:                     {len(pages)}")
    print(f"Keep ticked:                          {keep_ticked}")
    print(f"Keep already ticked:                  {already_keep}")
    print(f"Private Notes <- Play line(s):        {play_copied}")
    print(f"Private Notes <- 'REVIEW':            {review_marked}")
    print(f"Skipped (Private Notes already used): {len(skipped_private_exists)}")
    print(f"Nothing to do:                        {no_change}")
    print(f"Failed writes:                        {failed}")

    if skipped_private_exists:
        print("\n--- SKIPPED: Private Notes already has content (left untouched) ---")
        for label, existing, wanted in skipped_private_exists:
            print(f"* {label}: existing={existing!r}; would have set {wanted!r}")

    print("\nNotes were not changed on any record.")


if __name__ == "__main__":
    main()
