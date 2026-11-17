export class DeliveryRepository {
  constructor(database) {
    this.database = database;
    this.stageInboxItem = database.prepare(`INSERT INTO delivery_inbox
      (feed_id, capture_id, captured_at, article_id, canonical_url, title, published_at, source_index)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    this.pendingCaptures = database.prepare(`SELECT c.capture_id, c.feed_id, c.captured_at
      FROM captures c
      WHERE c.accepted=1 AND NOT EXISTS (
        SELECT 1 FROM delivery_index_state s WHERE s.capture_id=c.capture_id
      ) ORDER BY c.capture_id ASC LIMIT ?`);
    this.inboxForCapture = database.prepare(`SELECT feed_id, capture_id, captured_at, article_id,
      canonical_url, title, published_at, source_index FROM delivery_inbox
      WHERE capture_id=? ORDER BY source_index ASC, article_id ASC`);
    this.insertEvent = database.prepare(`INSERT INTO delivery_events
      (feed_seq, feed_id, capture_id, event_type, article_id, canonical_url, captured_at, source_index, payload_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    this.markIndexed = database.prepare(`INSERT INTO delivery_index_state
      (capture_id, feed_id, event_count, indexed_at) VALUES (?, ?, ?, ?)`);
    this.deleteInbox = database.prepare("DELETE FROM delivery_inbox WHERE capture_id=?");
    this.eventsAfter = database.prepare(`SELECT seq, feed_seq, feed_id, capture_id, event_type, article_id,
      canonical_url, captured_at, source_index, payload_json FROM delivery_events
      WHERE feed_id=? AND feed_seq>? ORDER BY feed_seq ASC LIMIT ?`);
    this.feedState = database.prepare("SELECT * FROM delivery_feed_state WHERE feed_id=?");
    this.insertFeedState = database.prepare(`INSERT INTO delivery_feed_state (feed_id, retained_from_seq, last_seq, updated_at)
      VALUES (?, 1, 0, ?) ON CONFLICT(feed_id) DO NOTHING`);
    this.updateRetained = database.prepare("UPDATE delivery_feed_state SET retained_from_seq=?, updated_at=? WHERE feed_id=?");
    this.advanceLastSeq = database.prepare("UPDATE delivery_feed_state SET last_seq=?, updated_at=? WHERE feed_id=?");
    this.pruneEventsBefore = database.prepare("DELETE FROM delivery_events WHERE feed_id=? AND feed_seq<?");
    this.lastRetainedSeqQuery = database.prepare("SELECT COALESCE(MAX(feed_seq), 0) AS last_seq FROM delivery_events WHERE feed_id=?");
    this.oldestEventAtQuery = database.prepare("SELECT MIN(captured_at) AS oldest_at FROM delivery_events WHERE feed_id=?");
    this.consumer = database.prepare("SELECT * FROM delivery_consumers WHERE feed_id=? AND consumer_id=?");
    this.listConsumers = database.prepare("SELECT * FROM delivery_consumers WHERE feed_id=? ORDER BY consumer_id");
    this.insertConsumer = database.prepare(`INSERT INTO delivery_consumers
      (consumer_id, feed_id, last_acked_seq, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`);
    this.updateConsumerAck = database.prepare(`UPDATE delivery_consumers SET last_acked_seq=?, updated_at=?
      WHERE feed_id=? AND consumer_id=?`);
    this.articleExistedBefore = database.prepare(`SELECT 1 FROM capture_items ci JOIN captures c
      ON c.capture_id=ci.capture_id
      WHERE ci.article_id=? AND c.accepted=1 AND c.capture_id<? LIMIT 1`);
    this.previousAcceptedCapture = database.prepare(`SELECT capture_id, captured_at FROM captures
      WHERE feed_id=? AND accepted=1 AND capture_id<? ORDER BY capture_id DESC LIMIT 1`);
    this.observedForCapture = database.prepare(`SELECT article_id, canonical_url, payload_json
      FROM delivery_events WHERE capture_id=? AND event_type='observed'`);
    // Frozen title/publishedAt from a previous capture still awaiting its first
    // indexing pass; the inbox is deleted right after indexing.
    this.inboxFallbackQuery = database.prepare(`SELECT article_id, canonical_url, title,
      published_at, source_index FROM delivery_inbox WHERE capture_id=? ORDER BY source_index ASC`);
    this.indexedState = database.prepare("SELECT * FROM delivery_index_state WHERE capture_id=?");
    this.orphanedInbox = database.prepare(`SELECT i.inbox_id, i.capture_id FROM delivery_inbox i
      JOIN captures c ON c.capture_id=i.capture_id WHERE c.accepted=0`);
  }

  stage(captureId, feedId, capturedAt, records) {
    for (const record of records) {
      this.stageInboxItem.run(feedId, captureId, capturedAt, record.articleId,
        record.canonicalUrl, record.title, record.publishedAt, record.sourceIndex);
    }
  }

  pendingCaptureIds(limit = 100) {
    return this.pendingCaptures.all(Math.max(1, Math.min(500, Number(limit) || 100)));
  }

  inbox(captureId) {
    return this.inboxForCapture.all(captureId).map(mapInboxRow);
  }

  appendEvent(event) {
    const result = this.insertEvent.run(event.feedSeq, event.feedId, event.captureId, event.eventType,
      event.articleId, event.canonicalUrl, event.capturedAt, event.sourceIndex ?? null, event.payloadJson);
    return { rowId: Number(result.lastInsertRowid), feedSeq: event.feedSeq };
  }

  maxSeq(feedId) {
    return Number(this.feedState.get(feedId)?.last_seq ?? 0);
  }

  nextSeq(feedId, now) {
    this.ensureFeedState(feedId, now);
    const next = this.maxSeq(feedId) + 1;
    this.advanceLastSeq.run(next, now, feedId);
    return next;
  }

  markCaptureIndexed(captureId, feedId, eventCount, indexedAt) {
    this.markIndexed.run(captureId, feedId, eventCount, indexedAt);
  }

  clearInbox(captureId) {
    this.deleteInbox.run(captureId);
  }

  readEvents(feedId, afterSeq, limit) {
    return this.eventsAfter.all(feedId, afterSeq, limit).map(mapEventRow);
  }

  getFeedState(feedId) {
    const row = this.feedState.get(feedId);
    return row ? {
      feedId: row.feed_id,
      retainedFromSeq: Number(row.retained_from_seq),
      lastSeq: Number(row.last_seq),
      updatedAt: row.updated_at
    } : null;
  }

  ensureFeedState(feedId, now) {
    this.insertFeedState.run(feedId, now);
    return this.getFeedState(feedId);
  }

  setRetainedFromSeq(feedId, seq, now) {
    this.updateRetained.run(seq, now, feedId);
  }

  pruneBefore(feedId, seq) {
    return Number(this.pruneEventsBefore.run(feedId, seq).changes);
  }

  lastRetainedSeq(feedId) {
    const value = Number(this.lastRetainedSeqQuery.get(feedId).last_seq);
    return value === 0 ? null : value;
  }

  oldestEventAt(feedId) {
    return this.oldestEventAtQuery.get(feedId).oldest_at;
  }

  getConsumer(feedId, consumerId) {
    const row = this.consumer.get(feedId, consumerId);
    return row ? mapConsumerRow(row) : null;
  }

  consumers(feedId) {
    return this.listConsumers.all(feedId).map(mapConsumerRow);
  }

  createConsumer(feedId, consumerId, lastAckedSeq, now) {
    this.insertConsumer.run(consumerId, feedId, lastAckedSeq, now, now);
    return this.getConsumer(feedId, consumerId);
  }

  checkpoint(feedId, consumerId, seq, now) {
    this.updateConsumerAck.run(seq, now, feedId, consumerId);
  }

  articleSeenBefore(articleId, captureId) {
    return Boolean(this.articleExistedBefore.get(articleId, captureId));
  }

  previousCapture(feedId, captureId) {
    const row = this.previousAcceptedCapture.get(feedId, captureId);
    return row ? { captureId: row.capture_id, capturedAt: row.captured_at } : null;
  }

  observedArticles(captureId) {
    return this.observedForCapture.all(captureId).map((row) => ({
      articleId: row.article_id,
      canonicalUrl: row.canonical_url,
      payload: JSON.parse(row.payload_json)
    }));
  }

  inboxFallback(captureId) {
    return this.inboxFallbackQuery.all(captureId).map((row) => ({
      articleId: row.article_id,
      canonicalUrl: row.canonical_url,
      title: row.title,
      publishedAt: row.published_at,
      sourceIndex: row.source_index
    }));
  }

  isIndexed(captureId) {
    return Boolean(this.indexedState.get(captureId));
  }

  findOrphanedInbox() {
    return this.orphanedInbox.all().map((row) => ({ inboxId: row.inbox_id, captureId: row.capture_id }));
  }
}

function mapInboxRow(row) {
  return {
    feedId: row.feed_id,
    captureId: row.capture_id,
    capturedAt: row.captured_at,
    articleId: row.article_id,
    canonicalUrl: row.canonical_url,
    title: row.title,
    publishedAt: row.published_at,
    sourceIndex: row.source_index
  };
}

function mapEventRow(row) {
  return {
    seq: Number(row.feed_seq),
    feedId: row.feed_id,
    captureId: row.capture_id,
    eventType: row.event_type,
    articleId: row.article_id,
    canonicalUrl: row.canonical_url,
    capturedAt: row.captured_at,
    sourceIndex: row.source_index,
    payload: JSON.parse(row.payload_json)
  };
}

function mapConsumerRow(row) {
  return {
    feedId: row.feed_id,
    consumerId: row.consumer_id,
    lastAckedSeq: Number(row.last_acked_seq),
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}
