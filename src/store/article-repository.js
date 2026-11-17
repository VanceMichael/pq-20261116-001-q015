export class ArticleRepository {
  constructor(database) {
    this.database = database;
    this.upsertArticle = database.prepare(`INSERT INTO articles
      (feed_id, canonical_url, title, published_at, first_seen_at, last_seen_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(feed_id, canonical_url) DO UPDATE SET
        title=excluded.title,
        published_at=COALESCE(excluded.published_at, articles.published_at),
        last_seen_at=excluded.last_seen_at,
        seen_count=articles.seen_count+1,
        missing_streak=0,
        visibility='visible'`);
    this.getArticleId = database.prepare("SELECT article_id FROM articles WHERE feed_id=? AND canonical_url=?");
    this.insertCaptureItem = database.prepare("INSERT INTO capture_items(capture_id, article_id, source_index) VALUES (?, ?, ?)");
    this.markMissing = database.prepare(`UPDATE articles SET missing_streak=missing_streak+1,
      visibility=CASE WHEN missing_streak+1 >= ? THEN 'quiet' ELSE visibility END
      WHERE feed_id=? AND last_seen_at < ? AND visibility='visible'`);
    this.listArticles = database.prepare(`SELECT article_id, feed_id, canonical_url, title, published_at,
      first_seen_at, last_seen_at, seen_count, missing_streak, visibility, review_state
      FROM articles WHERE feed_id=? AND (? IS NULL OR visibility=?)
      ORDER BY COALESCE(published_at, last_seen_at) DESC, article_id DESC LIMIT ? OFFSET ?`);
    this.countByVisibility = database.prepare(`SELECT visibility, COUNT(*) AS count FROM articles
      WHERE feed_id=? GROUP BY visibility`);
    this.total = database.prepare("SELECT COUNT(*) AS count FROM articles WHERE feed_id=?");
    this.byUrl = database.prepare("SELECT * FROM articles WHERE feed_id=? AND canonical_url=?");
    this.captureArticles = database.prepare(`SELECT a.article_id, a.feed_id, a.canonical_url, a.title,
      a.published_at, a.first_seen_at, a.last_seen_at, a.seen_count, a.missing_streak, a.visibility,
      ci.source_index FROM capture_items ci JOIN articles a ON a.article_id=ci.article_id
      WHERE ci.capture_id=? ORDER BY ci.source_index ASC`);
    this.timelineQuery = database.prepare(`SELECT c.capture_id, c.captured_at, c.normalized_count,
      ci.source_index FROM capture_items ci JOIN captures c ON c.capture_id=ci.capture_id
      WHERE c.feed_id=? AND ci.article_id=? AND c.accepted=1
      ORDER BY c.capture_id DESC LIMIT ?`);
  }

  recordCapture(captureId, feed, records, capturedAt) {
    const present = new Set();
    const staged = [];
    for (const [index, record] of records.entries()) {
      this.upsertArticle.run(feed.feedId, record.canonicalUrl, record.title, record.publishedAt,
        capturedAt, capturedAt);
      const articleId = Number(this.getArticleId.get(feed.feedId, record.canonicalUrl).article_id);
      this.insertCaptureItem.run(captureId, articleId, record.sourceIndex ?? index);
      present.add(record.canonicalUrl);
      staged.push({
        articleId,
        canonicalUrl: record.canonicalUrl,
        title: record.title,
        publishedAt: record.publishedAt ?? null,
        sourceIndex: record.sourceIndex ?? index
      });
    }
    this.markMissing.run(feed.missingThreshold, feed.feedId, capturedAt);
    return { observedCount: present.size, staged };
  }

  list(feedId, { visibility = null, limit = 50, offset = 0 } = {}) {
    const pageSize = Math.max(1, Math.min(200, Number(limit) || 50));
    const start = Math.max(0, Number(offset) || 0);
    return this.listArticles.all(feedId, visibility, visibility, pageSize, start).map(mapArticle);
  }

  get(feedId, canonicalUrl) {
    const row = this.byUrl.get(feedId, canonicalUrl);
    return row ? mapArticle(row) : null;
  }

  stats(feedId) {
    const visibility = Object.fromEntries(this.countByVisibility.all(feedId).map((row) => [row.visibility, row.count]));
    return { total: this.total.get(feedId).count, visible: visibility.visible ?? 0, quiet: visibility.quiet ?? 0 };
  }

  forCapture(captureId) {
    return this.captureArticles.all(captureId).map(mapArticle);
  }

  timeline(feedId, articleId, limit = 50) {
    return this.timelineQuery.all(feedId, articleId, Math.max(1, Math.min(200, Number(limit) || 50)));
  }
}

function mapArticle(row) {
  return {
    articleId: row.article_id,
    feedId: row.feed_id,
    canonicalUrl: row.canonical_url,
    title: row.title,
    publishedAt: row.published_at,
    firstSeenAt: row.first_seen_at,
    lastSeenAt: row.last_seen_at,
    seenCount: row.seen_count,
    missingStreak: row.missing_streak,
    visibility: row.visibility,
    sourceIndex: row.source_index ?? null,
    reviewState: row.review_state ?? "unreviewed"
  };
}
