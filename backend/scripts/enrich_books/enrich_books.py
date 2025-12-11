#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv, find_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

# ---------- config ----------
load_dotenv(find_dotenv())

def _env(key_primary: str, key_alt: str, default: Optional[str] = None) -> Optional[str]:
    """Read env with primary name; fall back to an alternate name."""
    return os.getenv(key_primary, os.getenv(key_alt, default))

PG = dict(
    host=_env("PGHOST", "PG_HOST", "localhost"),
    port=int(_env("PGPORT", "PG_PORT", "5432")),
    dbname=_env("PGDATABASE", "PG_DB", "reading"),
    user=_env("PGUSER", "PG_USER", "app"),
    password=_env("PGPASSWORD", "PG_PASSWORD", "app"),
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
MAX_BOOKS  = int(os.getenv("MAX_BOOKS", "0"))          # 0 = all
SLEEP_MS   = int(os.getenv("REQUEST_SLEEP_MS", "120")) # polite pause between API calls

# ---------- helpers ----------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "licenta-enrichment/1.0 (contact: you@example.local)"})

class TransientHTTP(Exception):
    pass

@retry(
    retry=retry_if_exception_type(TransientHTTP),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=20),
    stop=stop_after_attempt(5),
)
def http_json(url: str, timeout=20) -> dict:
    r = SESSION.get(url, timeout=timeout)
    if r.status_code >= 500:
        raise TransientHTTP(f"{r.status_code} from {url}")
    if r.status_code == 429:
        raise TransientHTTP("429 Too Many Requests")
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

ISBN10_RE = re.compile(r"^[0-9]{9}[0-9Xx]$")
ISBN13_RE = re.compile(r"^[0-9]{13}$")

def isbn13(s: str) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"[^0-9Xx]", "", s)
    if ISBN13_RE.match(s):
        return s
    if ISBN10_RE.match(s):
        core = "978" + s[:9]
        total = sum((int(core[i]) * (3 if i % 2 else 1)) for i in range(12))
        check = (10 - (total % 10)) % 10
        return core + str(check)
    return None

# Map subjects/categories to a controlled genre list (edit freely)
GENRE_MAP = {
    "fiction": "Fiction",
    "nonfiction": "Non-Fiction",
    "fantasy": "Fantasy",
    "science fiction": "Sci-Fi",
    "sci-fi": "Sci-Fi",
    "ya": "Young Adult",
    "young adult": "Young Adult",
    "romance": "Romance",
    "mystery": "Mystery",
    "thriller": "Thriller",
    "horror": "Horror",
    "historical": "Historical",
    "history": "History",
    "biography": "Biography",
    "memoir": "Memoir",
    "self-help": "Self-Help",
    "business": "Business",
    "philosophy": "Philosophy",
    "poetry": "Poetry",
    "children": "Children",
    "graphic novels": "Comics/Graphic",
    "comics": "Comics/Graphic",
    "technology": "Technology",
    "computer": "Technology",
    "programming": "Technology",
    "science": "Science",
    "math": "Math",
    "religion": "Religion",
    "art": "Art",
    "travel": "Travel",
}

def normalize_genres(labels: List[str]) -> List[str]:
    out = set()
    for raw in labels or []:
        s = raw.strip().lower()
        for key, val in GENRE_MAP.items():
            if key in s:
                out.add(val)
    return sorted(out)

def pick_best_int(*vals: Optional[int]) -> Optional[int]:
    for v in vals:
        if isinstance(v, int) and v > 0:
            return v
    return None

# ---------- source adapters ----------
def from_openlibrary(isbn: str) -> Tuple[Optional[int], List[str], Optional[str]]:
    b = http_json(f"https://openlibrary.org/isbn/{isbn}.json")
    pages = b.get("number_of_pages")
    if not pages:
        pages = b.get("pagination")
        if isinstance(pages, str):
            m = re.search(r"(\d+)", pages)
            pages = int(m.group(1)) if m else None
        else:
            pages = None

    subjects = b.get("subjects") or []
    subjects = [s if isinstance(s, str) else s.get("name") for s in subjects]
    subjects = [s for s in subjects if s]

    desc = b.get("description")
    if isinstance(desc, dict):
        desc = desc.get("value")
    description = desc.strip() if isinstance(desc, str) else None

    if not description or not subjects:
        works = b.get("works") or []
        if works and isinstance(works[0], dict) and "key" in works[0]:
            wk = http_json(f"https://openlibrary.org{works[0]['key']}.json")
            if not description:
                d = wk.get("description")
                description = (d.get("value") if isinstance(d, dict) else d) if d else None
            if not subjects:
                subs = wk.get("subjects") or []
                subjects = [s for s in subs if isinstance(s, str)]

    genres = normalize_genres(subjects)
    pc = pages if isinstance(pages, int) and pages > 0 else None
    return pc, genres, description

def from_google_books(isbn: str) -> Tuple[Optional[int], List[str], Optional[str]]:
    j = http_json(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}")
    items = j.get("items") or []
    if not items:
        return None, [], None
    v = items[0].get("volumeInfo", {})
    pc = v.get("pageCount")
    cats = v.get("categories") or []
    desc = v.get("description")
    return (
        pc if isinstance(pc, int) and pc > 0 else None,
        normalize_genres(cats),
        (desc or None),
    )

# ---------- DB ----------
def connect():
    conn = psycopg2.connect(**PG)
    conn.autocommit = False
    return conn

def fetch_isbns_to_enrich(cur, limit: int) -> List[str]:
    sql = """
      SELECT isbn13
      FROM books
      WHERE (page_count IS NULL OR page_count <= 0)
         OR (genres IS NULL OR array_length(genres,1) IS NULL)
         OR (description IS NULL OR length(description) < 10)
      ORDER BY id
      LIMIT %s
    """
    cur.execute(sql, (limit,))
    return [r[0] for r in cur.fetchall() if r[0]]

def upsert_enrichment(cur, rows: List[Tuple[Optional[int], List[str], Optional[str], str]]):
    """
    rows: (page_count, genres[], description, isbn13)
    Cast placeholders so Postgres knows their types and doesn't raise
    'could not determine polymorphic type because input has type unknown'.
    """
    sql = """
      UPDATE books
      SET
        page_count = COALESCE(%s::int, page_count),
        genres     = COALESCE(NULLIF(%s::text[], '{}'), genres),
        description = COALESCE(%s::text, description)
      WHERE isbn13 = %s
    """
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)

# ---------- main ----------
def main():
    conn = connect()
    cur = conn.cursor()

    processed = 0
    target = MAX_BOOKS if MAX_BOOKS > 0 else None

    try:
        while True:
            need = BATCH_SIZE if target is None else max(0, min(BATCH_SIZE, target - processed))
            if need == 0:
                break

            isbns = fetch_isbns_to_enrich(cur, need)
            if not isbns:
                break

            updates: List[Tuple[Optional[int], List[str], Optional[str], str]] = []

            for isbn in tqdm(isbns, desc="Enriching"):
                i13 = isbn13(isbn)
                if not i13:
                    continue

                # Try OpenLibrary first
                ol_pc, ol_genres, ol_desc = from_openlibrary(i13)
                # Fallback to Google Books where missing
                gb_pc, gb_genres, gb_desc = (None, [], None)
                if not ol_pc or not ol_genres or not ol_desc:
                    gb_pc, gb_genres, gb_desc = from_google_books(i13)

                page_count  = pick_best_int(ol_pc, gb_pc)
                genres      = ol_genres if ol_genres else gb_genres
                description = ol_desc if (ol_desc and len(ol_desc) >= 10) else gb_desc

                # Only queue an update if we have something useful
                if page_count or genres or (description and len(description) >= 10):
                    updates.append((
                        page_count if page_count else None,
                        genres if genres else [],              # empty array -> keep existing via NULLIF(...,'{}')
                        description if description else None,
                        i13,
                    ))

                # polite pause between API calls
                time.sleep(SLEEP_MS / 1000.0)

            if updates:
                upsert_enrichment(cur, updates)
                conn.commit()

            processed += len(isbns)
            if target is not None and processed >= target:
                break

    finally:
        cur.close()
        conn.close()

    print(f"Done. Processed batches for ~{processed} books.")

if __name__ == "__main__":
    main()
