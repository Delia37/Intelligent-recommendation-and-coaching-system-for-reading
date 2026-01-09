# #!/usr/bin/env python3
# from __future__ import annotations
#
# import json
# import os
# import re
# import time
# from pathlib import Path
# from typing import List, Optional, Tuple
# from requests import exceptions as req_exc
#
# import psycopg2
# import psycopg2.extras
# import requests
# from dotenv import load_dotenv, find_dotenv
# from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
# from tqdm import tqdm
#
# # ---------- config ----------
# load_dotenv(find_dotenv())
#
# def _env(key_primary: str, key_alt: str, default: Optional[str] = None) -> Optional[str]:
#     """Read env with primary name; fall back to an alternate name."""
#     return os.getenv(key_primary, os.getenv(key_alt, default))
#
# PG = dict(
#     host=_env("PGHOST", "PG_HOST", "localhost"),
#     port=int(_env("PGPORT", "PG_PORT", "5432")),
#     dbname=_env("PGDATABASE", "PG_DB", "reading"),
#     user=_env("PGUSER", "PG_USER", "app"),
#     password=_env("PGPASSWORD", "PG_PASSWORD", "app"),
# )
#
# BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
# MAX_BOOKS  = int(os.getenv("MAX_BOOKS", "0"))          # 0 = all
# SLEEP_MS   = int(os.getenv("REQUEST_SLEEP_MS", "120")) # polite pause between API calls
#
# # ---------- helpers ----------
# SESSION = requests.Session()
# SESSION.headers.update({"User-Agent": "licenta-enrichment/1.0 (contact: you@example.local)"})
#
# class TransientHTTP(Exception):
#     pass
#
# @retry(
#     retry=retry_if_exception_type(TransientHTTP),
#     wait=wait_exponential(multiplier=0.75, min=1, max=30),
#     stop=stop_after_attempt(6),
# )
# def http_json(url: str, timeout=30) -> dict:
#     try:
#         r = SESSION.get(url, timeout=timeout)
#     except (req_exc.Timeout, req_exc.ConnectionError, req_exc.SSLError, req_exc.ProxyError) as e:
#         # network/transient -> retry
#         raise TransientHTTP(f"network error: {e}")  # retried by Tenacity
#
#     if r.status_code >= 500 or r.status_code == 429:
#         # server/backoff -> retry
#         raise TransientHTTP(f"{r.status_code} from {url}")
#
#     if r.status_code != 200:
#         return {}
#     try:
#         return r.json()
#     except Exception:
#         return {}
#
# ISBN10_RE = re.compile(r"^[0-9]{9}[0-9Xx]$")
# ISBN13_RE = re.compile(r"^[0-9]{13}$")
#
# def isbn13(s: str) -> Optional[str]:
#     if not s:
#         return None
#     s = re.sub(r"[^0-9Xx]", "", s)
#     if ISBN13_RE.match(s):
#         return s
#     if ISBN10_RE.match(s):
#         core = "978" + s[:9]
#         total = sum((int(core[i]) * (3 if i % 2 else 1)) for i in range(12))
#         check = (10 - (total % 10)) % 10
#         return core + str(check)
#     return None
#
# # Map subjects/categories to a controlled genre list (edit freely)
# GENRE_MAP = {
#     "fiction": "Fiction",
#     "nonfiction": "Non-Fiction",
#     "fantasy": "Fantasy",
#     "science fiction": "Sci-Fi",
#     "sci-fi": "Sci-Fi",
#     "ya": "Young Adult",
#     "young adult": "Young Adult",
#     "romance": "Romance",
#     "mystery": "Mystery",
#     "thriller": "Thriller",
#     "horror": "Horror",
#     "historical": "Historical",
#     "history": "History",
#     "biography": "Biography",
#     "memoir": "Memoir",
#     "self-help": "Self-Help",
#     "business": "Business",
#     "philosophy": "Philosophy",
#     "poetry": "Poetry",
#     "children": "Children",
#     "graphic novels": "Comics/Graphic",
#     "comics": "Comics/Graphic",
#     "technology": "Technology",
#     "computer": "Technology",
#     "programming": "Technology",
#     "science": "Science",
#     "math": "Math",
#     "religion": "Religion",
#     "art": "Art",
#     "travel": "Travel",
# }
#
# def normalize_genres(labels: List[str]) -> List[str]:
#     out = set()
#     for raw in labels or []:
#         s = raw.strip().lower()
#         for key, val in GENRE_MAP.items():
#             if key in s:
#                 out.add(val)
#     return sorted(out)
#
# def pick_best_int(*vals: Optional[int]) -> Optional[int]:
#     for v in vals:
#         if isinstance(v, int) and v > 0:
#             return v
#     return None
#
# # ---------- source adapters ----------
# def from_openlibrary(isbn: str) -> Tuple[Optional[int], List[str], Optional[str]]:
#     b = http_json(f"https://openlibrary.org/isbn/{isbn}.json")
#     pages = b.get("number_of_pages")
#     if not pages:
#         pages = b.get("pagination")
#         if isinstance(pages, str):
#             m = re.search(r"(\d+)", pages)
#             pages = int(m.group(1)) if m else None
#         else:
#             pages = None
#
#     subjects = b.get("subjects") or []
#     subjects = [s if isinstance(s, str) else s.get("name") for s in subjects]
#     subjects = [s for s in subjects if s]
#
#     desc = b.get("description")
#     if isinstance(desc, dict):
#         desc = desc.get("value")
#     description = desc.strip() if isinstance(desc, str) else None
#
#     if not description or not subjects:
#         works = b.get("works") or []
#         if works and isinstance(works[0], dict) and "key" in works[0]:
#             wk = http_json(f"https://openlibrary.org{works[0]['key']}.json")
#             if not description:
#                 d = wk.get("description")
#                 description = (d.get("value") if isinstance(d, dict) else d) if d else None
#             if not subjects:
#                 subs = wk.get("subjects") or []
#                 subjects = [s for s in subs if isinstance(s, str)]
#
#     genres = normalize_genres(subjects)
#     pc = pages if isinstance(pages, int) and pages > 0 else None
#     return pc, genres, description
#
# def from_google_books(isbn: str) -> Tuple[Optional[int], List[str], Optional[str]]:
#     j = http_json(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}")
#     items = j.get("items") or []
#     if not items:
#         return None, [], None
#     v = items[0].get("volumeInfo", {})
#     pc = v.get("pageCount")
#     cats = v.get("categories") or []
#     desc = v.get("description")
#     return (
#         pc if isinstance(pc, int) and pc > 0 else None,
#         normalize_genres(cats),
#         (desc or None),
#     )
#
# # ---------- DB ----------
# def connect():
#     conn = psycopg2.connect(**PG)
#     conn.autocommit = False
#     return conn
#
# def fetch_isbns_to_enrich(cur, limit: int) -> List[str]:
#     sql = """
#       SELECT isbn13
#       FROM books
#       WHERE (page_count IS NULL OR page_count <= 0)
#          OR (genres IS NULL OR array_length(genres,1) IS NULL)
#          OR (description IS NULL OR length(description) < 10)
#       ORDER BY id
#       LIMIT %s
#     """
#     cur.execute(sql, (limit,))
#     return [r[0] for r in cur.fetchall() if r[0]]
#
# def upsert_enrichment(cur, rows: List[Tuple[Optional[int], List[str], Optional[str], str]]):
#     """
#     rows: (page_count, genres[], description, isbn13)
#     Cast placeholders so Postgres knows their types and doesn't raise
#     'could not determine polymorphic type because input has type unknown'.
#     """
#     sql = """
#       UPDATE books
#       SET
#         page_count = COALESCE(%s::int, page_count),
#         genres     = COALESCE(NULLIF(%s::text[], '{}'), genres),
#         description = COALESCE(%s::text, description)
#       WHERE isbn13 = %s
#     """
#     psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
#
# # ---------- main ----------
# def main():
#     conn = connect()
#     cur = conn.cursor()
#
#     processed = 0
#     target = MAX_BOOKS if MAX_BOOKS > 0 else None
#
#     try:
#         while True:
#             need = BATCH_SIZE if target is None else max(0, min(BATCH_SIZE, target - processed))
#             if need == 0:
#                 break
#
#             isbns = fetch_isbns_to_enrich(cur, need)
#             if not isbns:
#                 break
#
#             updates: List[Tuple[Optional[int], List[str], Optional[str], str]] = []
#
#             for isbn in tqdm(isbns, desc="Enriching"):
#                 i13 = isbn13(isbn)
#                 if not i13:
#                     continue
#
#                 # Try OpenLibrary first
#                 ol_pc, ol_genres, ol_desc = from_openlibrary(i13)
#                 # Fallback to Google Books where missing
#                 gb_pc, gb_genres, gb_desc = (None, [], None)
#                 if not ol_pc or not ol_genres or not ol_desc:
#                     gb_pc, gb_genres, gb_desc = from_google_books(i13)
#
#                 page_count  = pick_best_int(ol_pc, gb_pc)
#                 genres      = ol_genres if ol_genres else gb_genres
#                 description = ol_desc if (ol_desc and len(ol_desc) >= 10) else gb_desc
#
#                 # Only queue an update if we have something useful
#                 if page_count or genres or (description and len(description) >= 10):
#                     updates.append((
#                         page_count if page_count else None,
#                         genres if genres else [],              # empty array -> keep existing via NULLIF(...,'{}')
#                         description if description else None,
#                         i13,
#                     ))
#
#                 # polite pause between API calls
#                 time.sleep(SLEEP_MS / 1000.0)
#
#             if updates:
#                 upsert_enrichment(cur, updates)
#                 conn.commit()
#
#             processed += len(isbns)
#             if target is not None and processed >= target:
#                 break
#
#     finally:
#         cur.close()
#         conn.close()
#
#     print(f"Done. Processed batches for ~{processed} books.")
#
# if __name__ == "__main__":
#     main()

#!/usr/bin/env python3
from __future__ import annotations

import os, re, time
from typing import List, Optional, Tuple
from requests import exceptions as req_exc

import psycopg2, psycopg2.extras
import requests
from dotenv import load_dotenv, find_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

# ---------------- config / env ----------------
load_dotenv(find_dotenv())

def _env(primary: str, alt: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(primary, os.getenv(alt, default))

PG = dict(
    host=_env("PGHOST", "PG_HOST", "localhost"),
    port=int(_env("PGPORT", "PG_PORT", "5432")),
    dbname=_env("PGDATABASE", "PG_DB", "reading"),
    user=_env("PGUSER", "PG_USER", "app"),
    password=_env("PGPASSWORD", "PG_PASSWORD", "app"),
)

BATCH_SIZE            = int(os.getenv("BATCH_SIZE", "500"))
MAX_BOOKS             = int(os.getenv("MAX_BOOKS", "0"))     # 0 = all
SLEEP_MS              = int(os.getenv("REQUEST_SLEEP_MS", "120"))
ATTEMPT_COOLDOWN_MIN  = int(os.getenv("ATTEMPT_COOLDOWN_MIN", "60"))  # don’t retry same ISBN inside this window
GOOGLE_API_KEY        = os.getenv("GOOGLE_API_KEY")  # optional

# ---------------- HTTP helpers ----------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "licenta-enrichment/1.0 (contact: you@example.local)"})

class TransientHTTP(Exception): pass

@retry(
    retry=retry_if_exception_type(TransientHTTP),
    wait=wait_exponential(multiplier=0.75, min=1, max=30),
    stop=stop_after_attempt(6),
)
def http_json(url: str, timeout=30) -> dict:
    try:
        r = SESSION.get(url, timeout=timeout)
    except (req_exc.Timeout, req_exc.ConnectionError, req_exc.SSLError, req_exc.ProxyError) as e:
        raise TransientHTTP(f"network error: {e}")
    if r.status_code >= 500 or r.status_code == 429:
        raise TransientHTTP(f"{r.status_code} from {url}")
    if r.status_code != 200:
        return {}
    try:
        return r.json()
    except Exception:
        return {}

# ---------------- ISBN & genres ----------------
ISBN10_RE = re.compile(r"^[0-9]{9}[0-9Xx]$")
ISBN13_RE = re.compile(r"^[0-9]{13}$")

def isbn13(s: str) -> Optional[str]:
    if not s: return None
    s = re.sub(r"[^0-9Xx]", "", s)
    if ISBN13_RE.match(s): return s
    if ISBN10_RE.match(s):
        core = "978" + s[:9]
        total = sum((int(core[i]) * (3 if i % 2 else 1)) for i in range(12))
        check = (10 - (total % 10)) % 10
        return core + str(check)
    return None

GENRE_MAP = {
    "fiction":"Fiction","nonfiction":"Non-Fiction","fantasy":"Fantasy",
    "science fiction":"Sci-Fi","sci-fi":"Sci-Fi","ya":"Young Adult","young adult":"Young Adult",
    "romance":"Romance","mystery":"Mystery","thriller":"Thriller","horror":"Horror",
    "historical":"Historical","history":"History","biography":"Biography","memoir":"Memoir",
    "self-help":"Self-Help","business":"Business","philosophy":"Philosophy","poetry":"Poetry",
    "children":"Children","graphic novels":"Comics/Graphic","comics":"Comics/Graphic",
    "technology":"Technology","computer":"Technology","programming":"Technology",
    "science":"Science","math":"Math","religion":"Religion","art":"Art","travel":"Travel",
}

def normalize_genres(labels: List[str]) -> List[str]:
    out = set()
    for raw in labels or []:
        s = raw.strip().lower()
        for k,v in GENRE_MAP.items():
            if k in s: out.add(v)
    return sorted(out)

def pick_best_int(*vals: Optional[int]) -> Optional[int]:
    for v in vals:
        if isinstance(v, int) and v > 0: return v
    return None

# ---------------- source adapters ----------------
def from_openlibrary(isbn: str) -> Tuple[Optional[int], List[str], Optional[str]]:
    b = http_json(f"https://openlibrary.org/isbn/{isbn}.json")
    pages = b.get("number_of_pages")
    if not pages:
        pag = b.get("pagination")
        if isinstance(pag, str):
            m = re.search(r"(\d+)", pag)
            pages = int(m.group(1)) if m else None
    subjects = b.get("subjects") or []
    subjects = [s if isinstance(s,str) else s.get("name") for s in subjects]
    subjects = [s for s in subjects if s]

    desc = b.get("description")
    if isinstance(desc, dict): desc = desc.get("value")
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
    pc = pages if isinstance(pages,int) and pages>0 else None
    return pc, genres, description

def from_google_books(isbn: str) -> Tuple[Optional[int], List[str], Optional[str]]:
    base = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    if GOOGLE_API_KEY: base += f"&key={GOOGLE_API_KEY}"
    j = http_json(base)
    items = j.get("items") or []
    if not items: return None, [], None
    v = items[0].get("volumeInfo", {})
    pc   = v.get("pageCount")
    cats = v.get("categories") or []
    desc = v.get("description")
    return (pc if isinstance(pc,int) and pc>0 else None,
            normalize_genres(cats),
            (desc or None))

# ---------------- DB helpers ----------------
def connect():
    conn = psycopg2.connect(**PG)
    conn.autocommit = False
    return conn

def ensure_schema(cur):
    cur.execute("""
        ALTER TABLE books
          ADD COLUMN IF NOT EXISTS enriched_at     timestamptz,
          ADD COLUMN IF NOT EXISTS last_attempt_at timestamptz,
          ADD COLUMN IF NOT EXISTS attempt_count   int DEFAULT 0;
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_books_isbn13 ON books(isbn13);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_books_last_attempt ON books(last_attempt_at);")

def fetch_isbns_to_enrich(cur, limit: int) -> List[str]:
    # Only pick rows still missing something AND not attempted in the cool-down window
    sql = f"""
      SELECT isbn13
      FROM books
      WHERE (
              (page_count IS NULL OR page_count <= 0)
           OR (genres IS NULL OR array_length(genres,1) IS NULL)
           OR (description IS NULL OR length(description) < 10)
            )
        AND (last_attempt_at IS NULL OR last_attempt_at < NOW() - INTERVAL '{ATTEMPT_COOLDOWN_MIN} minutes')
      ORDER BY last_attempt_at NULLS FIRST, id
      LIMIT %s
    """
    cur.execute(sql, (limit,))
    return [r[0] for r in cur.fetchall() if r[0]]

def upsert_enrichment(cur,
                      rows: List[Tuple[Optional[int], List[str], Optional[str], str]]
                      ):
    """
    rows: (page_count, genres[], description, isbn13)
    - apply typed updates
    - bump enriched_at only if we set any of the fields
    - always bump last_attempt_at and attempt_count
    """
    sql = """
      UPDATE books
      SET
        page_count  = COALESCE(%s::int, page_count),
        genres      = COALESCE(NULLIF(%s::text[], '{}'), genres),
        description = COALESCE(%s::text, description),
        enriched_at = CASE
                        WHEN %s::int    IS NOT NULL
                          OR %s::text[] IS NOT NULL
                          OR %s::text   IS NOT NULL
                        THEN NOW()
                        ELSE enriched_at
                      END,
        last_attempt_at = NOW(),
        attempt_count   = COALESCE(attempt_count,0) + 1
      WHERE isbn13 = %s
    """
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)

# ---------------- main ----------------
def main():
    conn = connect()
    cur  = conn.cursor()
    ensure_schema(cur)
    conn.commit()

    processed = 0
    target = MAX_BOOKS if MAX_BOOKS > 0 else None

    try:
        while True:
            need = BATCH_SIZE if target is None else max(0, min(BATCH_SIZE, target - processed))
            if need == 0: break

            isbns = fetch_isbns_to_enrich(cur, need)
            if not isbns:
                print("No more eligible candidates right now.")
                break

            updates: List[Tuple[Optional[int], List[str], Optional[str], str]] = []

            for isbn in tqdm(isbns, desc="Enriching"):
                i13 = isbn13(isbn)
                if not i13:
                    continue

                ol_pc, ol_genres, ol_desc = from_openlibrary(i13)

                gb_pc, gb_genres, gb_desc = (None, [], None)
                if not ol_pc or not ol_genres or not ol_desc:
                    gb_pc, gb_genres, gb_desc = from_google_books(i13)

                page_count  = pick_best_int(ol_pc, gb_pc)
                genres      = ol_genres if ol_genres else gb_genres
                description = ol_desc if (ol_desc and len(ol_desc) >= 10) else gb_desc

                # Only queue if we learned anything
                if page_count or genres or (description and len(description) >= 10):
                    p = page_count if page_count else None
                    g = genres if genres else []   # empty list => no change via NULLIF('{}')
                    d = description if description else None
                    updates.append((p, g, d, p, g, d, i13))
                else:
                    # even if no new data, we still mark the attempt to avoid tight retry loops
                    updates.append((None, [], None, None, [], None, i13))

                if SLEEP_MS > 0:
                    time.sleep(SLEEP_MS / 1000.0)

            if updates:
                upsert_enrichment(cur, updates)
                conn.commit()

                cur.execute("""
                    SELECT
                      COUNT(*) FILTER (WHERE enriched_at   > NOW() - INTERVAL '5 minutes') AS changed_rows,
                      COUNT(*) FILTER (WHERE last_attempt_at > NOW() - INTERVAL '5 minutes') AS attempted_rows
                    FROM books
                """)
                changed, attempted = cur.fetchone()
                print(f"Committed {len(updates)} updates; changed in 5 min: {changed}, attempted in 5 min: {attempted}")
            else:
                print("Batch produced 0 updates.")

            processed += len(isbns)
            if target is not None and processed >= target:
                break

    finally:
        cur.close()
        conn.close()

    print(f"Done. Processed ~{processed} ISBN candidates.")

if __name__ == "__main__":
    main()
