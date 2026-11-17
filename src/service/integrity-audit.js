export class IntegrityAuditService {
  constructor(database) {
    this.database = database;
    this.integrityCheck = database.prepare("PRAGMA integrity_check");
    this.foreignKeyCheck = database.prepare("PRAGMA foreign_key_check");
    this.captureLinkMismatches = database.prepare(`SELECT c.capture_id, c.normalized_count,
      COUNT(ci.article_id) AS linked_count FROM captures c
      LEFT JOIN capture_items ci ON ci.capture_id=c.capture_id
      WHERE c.accepted=1 GROUP BY c.capture_id HAVING c.normalized_count != linked_count`);
    this.articleCountMismatches = database.prepare(`SELECT a.article_id, a.seen_count,
      COUNT(ci.capture_id) AS observed_count FROM articles a
      LEFT JOIN capture_items ci ON ci.article_id=a.article_id
      GROUP BY a.article_id HAVING a.seen_count != observed_count`);
    this.feedWatermarkMismatches = database.prepare(`SELECT f.feed_id, f.last_accepted_at, f.last_item_count,
      c.captured_at AS expected_at, c.normalized_count AS expected_count
      FROM feeds f LEFT JOIN captures c ON c.capture_id=(
        SELECT capture_id FROM captures WHERE feed_id=f.feed_id AND accepted=1 ORDER BY capture_id DESC LIMIT 1
      ) WHERE (f.last_accepted_at IS NOT c.captured_at) OR (f.last_item_count != COALESCE(c.normalized_count, 0))`);
    this.deliveryIndexMismatches = database.prepare(`SELECT c.capture_id, c.feed_id,
      (SELECT COUNT(*) FROM delivery_events e WHERE e.capture_id=c.capture_id) AS event_count,
      s.event_count AS indexed_count FROM captures c
      LEFT JOIN delivery_index_state s ON s.capture_id=c.capture_id
      WHERE c.accepted=1 AND s.capture_id IS NOT NULL
      AND s.event_count != (SELECT COUNT(*) FROM delivery_events e WHERE e.capture_id=c.capture_id)`);
    this.deliveryOrphans = database.prepare(`SELECT i.inbox_id, i.capture_id FROM delivery_inbox i
      JOIN captures c ON c.capture_id=i.capture_id WHERE c.accepted=0`);
  }

  run() {
    const checks = [
      { name: "sqlite_integrity", rows: this.integrityCheck.all().filter((row) => row.integrity_check !== "ok") },
      { name: "foreign_keys", rows: this.foreignKeyCheck.all() },
      { name: "capture_item_counts", rows: this.captureLinkMismatches.all() },
      { name: "article_observation_counts", rows: this.articleCountMismatches.all() },
      { name: "feed_watermarks", rows: this.feedWatermarkMismatches.all() },
      { name: "delivery_index_counts", rows: this.deliveryIndexMismatches.all() },
      { name: "delivery_inbox_orphans", rows: this.deliveryOrphans.all() }
    ].map((check) => ({
      name: check.name,
      passed: check.rows.length === 0,
      issueCount: check.rows.length,
      examples: check.rows.slice(0, 10)
    }));
    return {
      status: checks.every((check) => check.passed) ? "consistent" : "inconsistent",
      checkedAt: new Date().toISOString(),
      checks
    };
  }
}
