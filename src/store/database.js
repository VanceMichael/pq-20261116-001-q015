import { DatabaseSync } from "node:sqlite";
import { dirname, resolve } from "node:path";
import { mkdirSync } from "node:fs";

export const SCHEMA_VERSION = 2;

export function openDatabase(filename) {
  const path = resolve(filename);
  mkdirSync(dirname(path), { recursive: true });
  const database = new DatabaseSync(path, { timeout: 5000 });
  database.exec("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;");
  migrate(database);
  return database;
}

export function migrate(database) {
  const version = database.prepare("PRAGMA user_version").get().user_version;
  if (version > SCHEMA_VERSION) throw new Error(`database schema ${version} is newer than supported ${SCHEMA_VERSION}`);
  if (version === SCHEMA_VERSION) return;
  if (version === 0) {
    database.exec(`
    BEGIN IMMEDIATE;
    CREATE TABLE IF NOT EXISTS feeds (
      feed_id TEXT PRIMARY KEY,
      base_url TEXT NOT NULL,
      allowed_hosts_json TEXT NOT NULL,
      minimum_items INTEGER NOT NULL DEFAULT 3,
      missing_threshold INTEGER NOT NULL DEFAULT 3,
      last_accepted_at TEXT,
      last_item_count INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS captures (
      capture_id INTEGER PRIMARY KEY,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      fingerprint TEXT NOT NULL,
      captured_at TEXT NOT NULL,
      accepted INTEGER NOT NULL CHECK(accepted IN (0, 1)),
      candidate_count INTEGER NOT NULL,
      normalized_count INTEGER NOT NULL,
      rejected_count INTEGER NOT NULL,
      duplicate_count INTEGER NOT NULL,
      reason TEXT,
      UNIQUE(feed_id, fingerprint)
    );
    CREATE INDEX IF NOT EXISTS captures_feed_time ON captures(feed_id, captured_at DESC);
    CREATE TABLE IF NOT EXISTS articles (
      article_id INTEGER PRIMARY KEY,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      canonical_url TEXT NOT NULL,
      title TEXT NOT NULL,
      published_at TEXT,
      first_seen_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL,
      seen_count INTEGER NOT NULL DEFAULT 1,
      missing_streak INTEGER NOT NULL DEFAULT 0,
      visibility TEXT NOT NULL DEFAULT 'visible' CHECK(visibility IN ('visible', 'quiet')),
      review_state TEXT NOT NULL DEFAULT 'unreviewed' CHECK(review_state IN ('unreviewed', 'confirmed', 'withdrawn')),
      review_note TEXT,
      review_revision INTEGER NOT NULL DEFAULT 0,
      UNIQUE(feed_id, canonical_url)
    );
    CREATE INDEX IF NOT EXISTS articles_feed_visibility ON articles(feed_id, visibility, last_seen_at DESC);
    CREATE TABLE IF NOT EXISTS capture_items (
      capture_id INTEGER NOT NULL REFERENCES captures(capture_id) ON DELETE CASCADE,
      article_id INTEGER NOT NULL REFERENCES articles(article_id),
      source_index INTEGER NOT NULL,
      PRIMARY KEY(capture_id, article_id)
    );
    CREATE TABLE IF NOT EXISTS fetch_attempts (
      attempt_id INTEGER PRIMARY KEY,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      started_at TEXT NOT NULL,
      finished_at TEXT NOT NULL,
      outcome TEXT NOT NULL CHECK(outcome IN ('accepted', 'rejected', 'not_modified', 'failed')),
      http_status INTEGER,
      attempt_count INTEGER NOT NULL,
      requested_url TEXT NOT NULL,
      final_url TEXT,
      capture_id INTEGER REFERENCES captures(capture_id),
      error_code TEXT,
      error_message TEXT
    );
    CREATE INDEX IF NOT EXISTS fetch_attempts_feed_time ON fetch_attempts(feed_id, attempt_id DESC);
    CREATE TABLE IF NOT EXISTS review_events (
      event_id INTEGER PRIMARY KEY,
      article_id INTEGER NOT NULL REFERENCES articles(article_id),
      prior_state TEXT NOT NULL,
      next_state TEXT NOT NULL,
      note TEXT,
      revision INTEGER NOT NULL,
      changed_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS review_events_article_time ON review_events(article_id, event_id DESC);
    PRAGMA user_version = 1;
    COMMIT;
  `);
  }
  if (database.prepare("PRAGMA user_version").get().user_version === 1) {
    database.exec(`
    BEGIN IMMEDIATE;
    CREATE TABLE IF NOT EXISTS delivery_inbox (
      inbox_id INTEGER PRIMARY KEY,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      capture_id INTEGER NOT NULL REFERENCES captures(capture_id),
      captured_at TEXT NOT NULL,
      article_id INTEGER NOT NULL REFERENCES articles(article_id),
      canonical_url TEXT NOT NULL,
      title TEXT NOT NULL,
      published_at TEXT,
      source_index INTEGER NOT NULL,
      UNIQUE(capture_id, article_id)
    );
    CREATE INDEX IF NOT EXISTS delivery_inbox_pending ON delivery_inbox(capture_id);
    CREATE TABLE IF NOT EXISTS delivery_events (
      seq INTEGER PRIMARY KEY,
      feed_seq INTEGER NOT NULL,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      capture_id INTEGER NOT NULL REFERENCES captures(capture_id),
      event_type TEXT NOT NULL CHECK(event_type IN ('observed', 'field_corrected', 'omitted')),
      article_id INTEGER NOT NULL REFERENCES articles(article_id),
      canonical_url TEXT NOT NULL,
      captured_at TEXT NOT NULL,
      source_index INTEGER,
      payload_json TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS delivery_events_feed_seq ON delivery_events(feed_id, feed_seq);
    CREATE INDEX IF NOT EXISTS delivery_events_article ON delivery_events(feed_id, article_id, feed_seq);
    CREATE TABLE IF NOT EXISTS delivery_index_state (
      capture_id INTEGER PRIMARY KEY REFERENCES captures(capture_id),
      feed_id TEXT NOT NULL,
      event_count INTEGER NOT NULL,
      indexed_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS delivery_consumers (
      consumer_id TEXT NOT NULL,
      feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
      last_acked_seq INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY(feed_id, consumer_id)
    );
    CREATE TABLE IF NOT EXISTS delivery_feed_state (
      feed_id TEXT PRIMARY KEY REFERENCES feeds(feed_id),
      retained_from_seq INTEGER NOT NULL DEFAULT 1,
      last_seq INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL
    );
    -- Backfill the crash-safe delivery inbox for accepted captures that
    -- existed before the delivery schema, so recovery treats every accepted
    -- capture uniformly instead of failing on missing staging rows.
    INSERT INTO delivery_inbox
      (feed_id, capture_id, captured_at, article_id, canonical_url, title, published_at, source_index)
    SELECT c.feed_id, c.capture_id, c.captured_at, a.article_id, a.canonical_url, a.title,
      a.published_at, ci.source_index
    FROM captures c
    JOIN capture_items ci ON ci.capture_id = c.capture_id
    JOIN articles a ON a.article_id = ci.article_id
    WHERE c.accepted = 1;
    PRAGMA user_version = 2;
    COMMIT;
  `);
  }
}

export function closeDatabase(database) {
  if (database?.isOpen) database.close();
}
