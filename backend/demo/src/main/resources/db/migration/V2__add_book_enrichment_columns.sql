ALTER TABLE books
    ADD COLUMN IF NOT EXISTS page_count      int,
    ADD COLUMN IF NOT EXISTS genres          text[] DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS description     text,
    ADD COLUMN IF NOT EXISTS cover_url       text,
    ADD COLUMN IF NOT EXISTS enrichment_src  jsonb,           -- provenance per book
    ADD COLUMN IF NOT EXISTS enrichment_at   timestamptz;

-- helpful indexes for filtering
CREATE INDEX IF NOT EXISTS idx_books_page_count ON books(page_count);
CREATE INDEX IF NOT EXISTS idx_books_genres_gin ON books USING gin(genres);
