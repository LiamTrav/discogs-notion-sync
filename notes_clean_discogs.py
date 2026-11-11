"""
Notes CLEAN (writes to Discogs and Notion).

Makes the Notes field the same, and clean, on Discogs and in Notion.

For each Discogs collection instance whose Notes contain 'Play A' / 'Play B' /
'Play!' lines (and are NOT flagged as REVIEW):
  1. the Play line(s) are removed from the Discogs Notes field, leaving only
     the public text (this is the 'proposed_public' text from the triage CSV);
  2. the Notion Notes property is updated to the same text.

SAFETY GATES
  * A record is only changed if its Play line(s) are already saved in the
    Notion 'Private Notes' property AND 'Keep' is ticked. Run notes_apply.py
    first. Otherwise the record is skipped and reported.
  * REVIEW records ('Not this version', 'My version...') are never changed.
  * Records with no Play lines are never changed.
  * A backup of EVERY non-empty Discogs Notes value is written to
    notes_backup.csv BEFORE any change is made.
  * DRY_RUN defaults to TRUE. Set DRY_RUN=false to actually write.
  * MAX_WRITES limits how many records are changed in one run (0 = no limit).
    Use a small number first as a pilot.
  * Safe to re-run: once a record is cleaned it no longer has Play lines.
"""

import csv
import os
import re
import time

import requests
from requests.exceptions import RequestException

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
USERNAME = os.environ["DISCOGS_USERNAME"]

DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() != "false"
MAX_WRITES = int(os.environ.get("MAX_WRITES", "0") or 0)

DISCOGS_BASE = "https://api.discogs.com"
NOTION_BASE = "https://api.notion.com/v1"
BACKUP_PATH = "notes_backup.csv"

headers_discogs = {
    "Authorization": f"Discogs token={DISCOGS_TOKEN}",
    "User-Agent": "discogs-notion-notes-clean/1.0",
}

headers_notion = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ---------------------------------------------------
# RULES (identical to notes_triage.py / notes_apply.py)
# ---------------------------------------------------

PLAY_LINE = re.compile(r"^play(\s+side)?(\s+[ab])?\s*[!.]?$", re.IGNORECASE)

VERSION_LINE = re.compile(
    r"^(not this version|my version|mine is|same runouts?|does not have)",
    re.IGNORECASE,
)
VERSION_WORDS = re.compile(r"\b(this version|this release|my version)\b", re.IGNORECASE)


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
# REQUEST HELPERS
# ---------------------------------------------------

def discogs_request(method, url, payload=None, retries=5):
    for attempt in range(retries):
        try:
            r = requests.request(
                method, url, headers=headers_discogs, json=payload, timeout=30
            )
        except RequestException as e:
            wait = 2 ** attempt
            print(f"DISCOGS REQUEST EXCEPTION: {e} - retrying in {wait}s")
            time.sleep(wait)
            continue

        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 30))
            print(f"DISCOGS RATE LIMITED - sleeping {retry}s")
            time.sleep(retry)
            continue

        if r.status_code >= 500:
            wait = 2 ** attempt
            print(f"DISCOGS SERVER ERROR {r.status_code} - retrying in {wait}s")
            time.sleep(wait)
            continue

        if not r.ok:
            print(f"DISCOGS ERROR {r.status_code} for {url}: {r.text[:300]}")
            return None

        # Throttle only when close to the limit
        remaining = r.headers.get("X-Discogs-Ratelimit-Remaining")
        if remaining is not None and int(remaining) <= 2:
            time.sleep(10)

        return r

    return None


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

# ---------------------------------------------------
# DISCOGS FETCH
# ---------------------------------------------------

def get_full_collection():
    releases = []
    page = 1
    while True:
        url = f"{DISCOGS_BASE}/users/{USERNAME}/collection/folders/0/releases?page={page}&per_page=100"
        r = discogs_request("GET", url)
        if not r:
            raise Exception(f"Failed fetching Discogs collection page {page}")
        data = r.json()
        releases.extend(data.get("releases", []))
        if page >= data.get("pagination", {}).get("pages", 1):
            break
        page += 1
    return releases


def get_notes_field_id():
    r = discogs_request("GET", f"{DISCOGS_BASE}/users/{USERNAME}/collection/fields")
    if not r:
        raise Exception("Failed fetching Discogs collection fields")
    for f in r.json().get("fields", []):
        if f.get("name") == "Notes":
            return f["id"]
    raise Exception("No Discogs collection field named 'Notes' was found")

# ---------------------------------------------------
# NOTION FETCH
# ---------------------------------------------------

def plain(rich_text):
    return "".join(part.get("plain_text", "") for part in (rich_text or []))


def fetch_schema():
    r = notion_request("GET", f"{NOTION_BASE}/databases/{DATABASE_ID}")
    if not r:
        raise Exception("Could not read Notion database schema")
    return r.json().get("properties", {})


def fetch_keep_pages():
    """Notion pages with Keep ticked, keyed by Instance ID."""
    pages = {}
    has_more = True
    cursor = None

    while has_more:
        payload = {
            "page_size": 100,
            "filter": {"property": "Keep", "checkbox": {"equals": True}},
        }
        if cursor:
            payload["start_cursor"] = cursor

        r = notion_request("POST", f"{NOTION_BASE}/databases/{DATABASE_ID}/query", payload)
        if not r:
            raise Exception("Failed fetching Notion pages")

        data = r.json()
        for result in data.get("results", []):
            props = result["properties"]
            instance_id = props["Instance ID"].get("number")
            if instance_id is None:
                continue
            pages[int(instance_id)] = {
                "page_id": result["id"],
                "private": plain(props["Private Notes"].get("rich_text")),
            }

        has_more = data.get("has_more")
        cursor = data.get("next_cursor")

    return pages

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    print("=" * 60)
    print("NOTES CLEAN - " + ("DRY RUN (nothing will be changed)" if DRY_RUN else "LIVE (writing to Discogs and Notion)"))
    if MAX_WRITES:
        print(f"Cap: at most {MAX_WRITES} record(s) this run")
    print("=" * 60)

    schema = fetch_schema()
    problems = []
    if schema.get("Keep", {}).get("type") != "checkbox":
        problems.append("'Keep' must exist and be a Checkbox property")
    if schema.get("Private Notes", {}).get("type") != "rich_text":
        problems.append("'Private Notes' must exist and be a Text property")
    if schema.get("Notes", {}).get("type") != "rich_text":
        problems.append("'Notes' must exist and be a Text property")
    if schema.get("Instance ID", {}).get("type") != "number":
        problems.append("'Instance ID' must exist and be a Number property")
    if problems:
        print("\nSTOPPING:")
        for p in problems:
            print(f"  - {p}")
        raise SystemExit(1)

    notes_field_id = get_notes_field_id()
    print(f"Discogs 'Notes' field id: {notes_field_id}")

    print("Fetching Discogs collection...")
    collection = get_full_collection()
    print(f"Discogs instances: {len(collection)}")

    print("Fetching Notion pages with Keep ticked...")
    keep_pages = fetch_keep_pages()
    print(f"Notion records with Keep ticked: {len(keep_pages)}")

    backup_rows = []
    targets = []
    blocked = []
    with_notes = review_skipped = public_only = 0

    for item in collection:
        instance_id = item["instance_id"]
        basic = item["basic_information"]
        artists = ", ".join(a["name"] for a in basic.get("artists", []))
        label = f"{artists} - {basic.get('title', '')}".strip(" -")

        original = ""
        for n in item.get("notes", []):
            if n.get("field_id") == notes_field_id:
                original = n.get("value") or ""

        if not original.strip():
            continue

        with_notes += 1
        play, version, public = classify(original)
        new_text = "\n".join(public)

        backup_rows.append({
            "instance_id": instance_id,
            "release_id": basic["id"],
            "folder_id": item.get("folder_id"),
            "label": label,
            "original_notes": original,
        })

        if version:
            review_skipped += 1
            continue
        if not play:
            public_only += 1
            continue

        page = keep_pages.get(int(instance_id))
        if not page:
            blocked.append((label, "no Notion page with Keep ticked for this instance"))
            continue
        if not all(line in page["private"] for line in play):
            blocked.append((label, "Play line(s) not found in Private Notes (run notes_apply first)"))
            continue

        targets.append({
            "instance_id": instance_id,
            "release_id": basic["id"],
            "folder_id": item.get("folder_id"),
            "label": label,
            "original": original,
            "new": new_text,
            "page_id": page["page_id"],
        })

    # Backup BEFORE any change
    with open(BACKUP_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f, fieldnames=["instance_id", "release_id", "folder_id", "label", "original_notes"]
        )
        writer.writeheader()
        writer.writerows(backup_rows)
    print(f"Backup of {len(backup_rows)} Discogs Notes values saved to {BACKUP_PATH}")

    # Pilot-friendly order: one record that keeps some text, then one that
    # becomes empty, then the rest - so a small capped run tests both cases.
    mixed = [t for t in targets if t["new"]]
    cleared = [t for t in targets if not t["new"]]
    ordered = []
    if mixed:
        ordered.append(mixed.pop(0))
    if cleared:
        ordered.append(cleared.pop(0))
    ordered.extend(mixed + cleared)

    print(f"\nDiscogs instances with Notes: {with_notes}")
    print(f"  REVIEW (left unchanged):     {review_skipped}")
    print(f"  Public only (no change):     {public_only}")
    print(f"  Blocked by safety gate:      {len(blocked)}")
    print(f"  To clean:                    {len(targets)}")

    if blocked:
        print("\n--- BLOCKED (left unchanged) ---")
        for label, reason in blocked:
            print(f"* {label}: {reason}")

    changed = failed = notion_failed = 0

    print("\n--- RECORDS TO CLEAN ---")
    for t in ordered:
        if MAX_WRITES and changed >= MAX_WRITES:
            print(f"Reached cap of {MAX_WRITES}; stopping.")
            break

        print(f"* {t['label']}  [instance {t['instance_id']}]")
        print(f"    before: {t['original']!r}")
        print(f"    after:  {t['new']!r}" + ("   (field will be emptied)" if not t["new"] else ""))

        if DRY_RUN:
            changed += 1
            continue

        url = (
            f"{DISCOGS_BASE}/users/{USERNAME}/collection/folders/{t['folder_id']}"
            f"/releases/{t['release_id']}/instances/{t['instance_id']}"
            f"/fields/{notes_field_id}"
        )
        r = discogs_request("POST", url, {"value": t["new"]})
        time.sleep(1.2)  # stay comfortably inside the Discogs rate limit
        if not r:
            failed += 1
            print("    DISCOGS UPDATE FAILED - Notion left unchanged for this record")
            continue

        notes_payload = (
            {"rich_text": [{"text": {"content": t["new"]}}]} if t["new"] else {"rich_text": []}
        )
        rn = notion_request(
            "PATCH", f"{NOTION_BASE}/pages/{t['page_id']}", {"properties": {"Notes": notes_payload}}
        )
        time.sleep(0.35)
        if not rn:
            notion_failed += 1
            print("    Discogs updated, but Notion update failed (the next daily sync will correct it)")

        changed += 1

    print("\n" + "=" * 60)
    print("SUMMARY" + (" (dry run - nothing changed)" if DRY_RUN else ""))
    print("=" * 60)
    print(f"Records {'that would be' if DRY_RUN else ''} cleaned this run: {changed}")
    if not DRY_RUN:
        print(f"Discogs update failures: {failed}")
        print(f"Notion update failures:  {notion_failed}")
    print(f"Backup file: {BACKUP_PATH}")
    print("\nThe next daily sync will refresh the other fields for the changed records as normal.")


if __name__ == "__main__":
    main()
