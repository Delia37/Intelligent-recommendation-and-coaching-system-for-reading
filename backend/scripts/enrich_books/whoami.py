# backend/scripts/enrich_books/whoami.py
import os, psycopg2
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

conn = psycopg2.connect(
    host=os.getenv("PGHOST") or os.getenv("PG_HOST"),
    port=os.getenv("PGPORT") or os.getenv("PG_PORT"),
    dbname=os.getenv("PGDATABASE") or os.getenv("PG_DB"),
    user=os.getenv("PGUSER") or os.getenv("PG_USER"),
    password=os.getenv("PGPASSWORD") or os.getenv("PG_PASSWORD"),
)
cur = conn.cursor()
cur.execute("select current_database(), current_user, inet_server_addr(), inet_server_port()")
print(cur.fetchone())
cur.close(); conn.close()
