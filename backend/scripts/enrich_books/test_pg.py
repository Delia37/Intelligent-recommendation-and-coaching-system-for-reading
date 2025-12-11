# save as backend/scripts/enrich_books/test_pg.py
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import os, psycopg2
print("PG_USER=", os.getenv("PG_USER"), "PG_DB=", os.getenv("PG_DB"))
conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    dbname=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
)
with conn.cursor() as cur:
    cur.execute("select now()")
    print("OK:", cur.fetchone())
conn.close()
