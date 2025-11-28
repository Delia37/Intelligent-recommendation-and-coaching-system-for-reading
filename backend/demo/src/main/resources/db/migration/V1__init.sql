-- Enable pgvector (safe if already enabled)
CREATE EXTENSION IF NOT EXISTS vector;

-- Users
CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  pass_hash TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('USER','ADMIN')),
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Books (will be enriched later)
CREATE TABLE IF NOT EXISTS books (
  id BIGSERIAL PRIMARY KEY,
  isbn13 TEXT UNIQUE,
  title TEXT NOT NULL,
  author TEXT NOT NULL,
  description TEXT,
  cover_s TEXT,
  cover_m TEXT,
  cover_l TEXT,
  genres TEXT[] DEFAULT '{}',
  page_count INT,
  page_count_source TEXT,
  page_count_confidence REAL,
  genre_source TEXT,
  genre_confidence REAL,
  embedding VECTOR(384)    -- MiniLM (we'll fill later)
);

-- Ratings
CREATE TABLE IF NOT EXISTS ratings (
  user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
  book_id BIGINT REFERENCES books(id) ON DELETE CASCADE,
  rating SMALLINT CHECK (rating BETWEEN 0 AND 10),
  rated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (user_id, book_id)
);

-- User profile (onboarding + taste vector)
CREATE TABLE IF NOT EXISTS user_profile (
  user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  preferred_genres TEXT[] DEFAULT '{}',
  preferred_authors TEXT[] DEFAULT '{}',
  pages_per_day INT DEFAULT 20,
  taste_embedding VECTOR(384)
);

-- Popularity materialized view (optional simple prior)
CREATE MATERIALIZED VIEW IF NOT EXISTS book_popularity AS
SELECT b.id, COALESCE(AVG(r.rating),0) AS avg_rating, COUNT(r.*) AS n_ratings
FROM books b LEFT JOIN ratings r ON r.book_id=b.id
GROUP BY b.id;

-- ANN index for embeddings (tune later)
CREATE INDEX IF NOT EXISTS idx_books_embedding_ivfflat
  ON books USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
