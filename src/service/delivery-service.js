import { validateFeedId } from "../store/feed-repository.js";

export const OMISSION_NOTE = "Omission means absent from this listing capture, not that the publisher deleted the article.";
const DEFAULT_RETENTION_DAYS = 7;
const DEFAULT_PAGE_LIMIT = 100;
const MAX_PAGE_LIMIT = 500;
const STARTING_POSITIONS = new Set(["earliest", "latest"]);

export class DeliveryError extends Error {
  constructor(status, code, message, details = undefined) {
    super(message);
    this.name = "DeliveryError";
    this.statusCode = status;
    this.code = code;
    if (details !== undefined) this.details = details;
  }
}

export class DeliveryService {
  constructor({ database, feeds, delivery, clock = () => new Date(), retentionDays = DEFAULT_RETENTION_DAYS }) {
    this.database = database;
    this.feeds = feeds;
    this.delivery = delivery;
    this.clock = clock;
    if (!Number.isInteger(retentionDays) || retentionDays < 1 || retentionDays > 3650) {
      throw new RangeError("retentionDays must be between 1 and 3650");
    }
    this.retentionDays = retentionDays;
  }

  now() {
    return this.clock().toISOString();
  }

  // Staging runs inside the caller's capture transaction, so it must not
  // start its own transaction.
  stage(captureId, feedId, capturedAt, records) {
    this.delivery.ensureFeedState(feedId, this.now());
    this.delivery.stage(captureId, feedId, capturedAt, records);
  }

  // Convert any committed-but-not-yet-indexed accepted captures into immutable
  // delivery events. Each capture is indexed in its own transaction and is
  // idempotent, so a crash between capture commit and indexing self-heals.
  indexPending({ limit = 100 } = {}) {
    const indexed = [];
    for (const pending of this.delivery.pendingCaptureIds(limit)) {
      if (!this.delivery.inbox(pending.capture_id).length) continue;
      indexed.push(this.indexCapture(pending.capture_id));
    }
    return indexed;
  }

  // Best-effort recovery hook used by ingest and read paths: pending indexing
  // must never break an already-committed capture or a consumer read.
  catchUp() {
    try {
      return this.indexPending();
    } catch (error) {
      return [];
    }
  }

  indexCapture(captureId) {
    if (this.delivery.isIndexed(captureId)) {
      return { captureId, eventCount: 0, replayed: true };
    }
    const rows = this.delivery.inbox(captureId);
    if (!rows.length) throw new Error(`delivery staging rows are missing for capture ${captureId}`);
    const first = rows[0];
    const feedId = first.feedId;
    const capturedAt = first.capturedAt;
    this.database.exec("BEGIN IMMEDIATE");
    try {
      if (this.delivery.isIndexed(captureId)) {
        this.database.exec("ROLLBACK");
        return { captureId, eventCount: 0, replayed: true };
      }
      this.delivery.ensureFeedState(feedId, this.now());
      const previousRow = this.delivery.previousCapture(feedId, captureId);
      const previous = previousRow ? { ...previousRow, feedId } : null;
      const previousItems = previous ? this.loadPreviousItems(previous.captureId) : new Map();
      const currentIds = new Set(rows.map((row) => row.articleId));
      const appended = [];
      const emit = (event) => {
        const feedSeq = this.delivery.nextSeq(feedId, this.now());
        const result = this.delivery.appendEvent({ ...event, feedSeq });
        appended.push({ seq: result.feedSeq });
      };
      for (const row of rows) {
        const prior = previousItems.get(row.articleId);
        if (prior) {
          if (prior.title !== row.title) {
            emit(this.buildCorrection(row, "title", prior.title, row.title, capturedAt));
          }
          if ((prior.publishedAt ?? null) !== (row.publishedAt ?? null)) {
            emit(this.buildCorrection(row, "publishedAt", prior.publishedAt ?? null,
              row.publishedAt ?? null, capturedAt));
          }
        }
        const firstObserved = !this.delivery.articleSeenBefore(row.articleId, captureId);
        emit(this.buildObserved(row, firstObserved, capturedAt));
      }
      if (previous) {
        for (const prior of previousItems.values()) {
          if (!currentIds.has(prior.articleId)) {
            emit(this.buildOmission(prior, previous, capturedAt));
          }
        }
      }
      this.delivery.markCaptureIndexed(captureId, feedId, appended.length, this.now());
      this.delivery.clearInbox(captureId);
      this.database.exec("COMMIT");
      return { captureId, feedId, capturedAt, eventCount: appended.length, replayed: false,
        eventSeqs: appended.map((event) => event.seq) };
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }

  loadPreviousItems(captureId) {
    const items = new Map();
    for (const observed of this.delivery.observedArticles(captureId)) {
      items.set(observed.articleId, {
        articleId: observed.articleId,
        canonicalUrl: observed.canonicalUrl,
        title: observed.payload.title,
        publishedAt: observed.payload.publishedAt ?? null,
        sourceIndex: observed.payload.sourceIndex
      });
    }
    if (items.size) return items;
    // Recovery source: the previous capture may still be staged in the crash-
    // safe inbox when indexing fell behind. Its rows are frozen too. When both
    // the events and inbox rows are gone (retention), corrections cannot be
    // reconstructed, so we only emit observations for the new capture.
    for (const fallback of this.delivery.inboxFallback(captureId)) {
      items.set(fallback.articleId, fallback);
    }
    return items;
  }

  buildObserved(row, firstObserved, capturedAt) {
    const payload = {
      eventType: "observed",
      canonicalUrl: row.canonicalUrl,
      title: row.title,
      publishedAt: row.publishedAt ?? null,
      sourceIndex: row.sourceIndex,
      firstObserved,
      observedAt: capturedAt
    };
    return {
      feedId: row.feedId, captureId: row.captureId, eventType: "observed",
      articleId: row.articleId, canonicalUrl: row.canonicalUrl, capturedAt,
      sourceIndex: row.sourceIndex, payloadJson: JSON.stringify(payload)
    };
  }

  buildCorrection(row, field, previousValue, currentValue, capturedAt) {
    const payload = {
      eventType: "field_corrected",
      field,
      canonicalUrl: row.canonicalUrl,
      title: row.title,
      previousValue,
      currentValue,
      sourceIndex: row.sourceIndex,
      changedAt: capturedAt
    };
    return {
      feedId: row.feedId, captureId: row.captureId, eventType: "field_corrected",
      articleId: row.articleId, canonicalUrl: row.canonicalUrl, capturedAt,
      sourceIndex: row.sourceIndex, payloadJson: JSON.stringify(payload)
    };
  }

  buildOmission(prior, previous, capturedAt) {
    const payload = {
      eventType: "omitted",
      canonicalUrl: prior.canonicalUrl,
      title: prior.title,
      publishedAt: prior.publishedAt ?? null,
      previousSourceIndex: prior.sourceIndex,
      previousCaptureId: previous.captureId,
      omittedFromCaptureCapturedAt: capturedAt,
      note: OMISSION_NOTE
    };
    return {
      feedId: previous.feedId, captureId: previous.captureId, eventType: "omitted",
      articleId: prior.articleId, canonicalUrl: prior.canonicalUrl, capturedAt,
      sourceIndex: prior.sourceIndex, payloadJson: JSON.stringify(payload)
    };
  }

  registerConsumer(feedId, consumerId, { startAt = "earliest" } = {}) {
    validateFeedId(feedId);
    validateConsumerId(consumerId);
    if (!this.feeds.get(feedId)) throw new DeliveryError(404, "feed_not_found", `unknown feed: ${feedId}`);
    if (!STARTING_POSITIONS.has(startAt)) {
      throw new DeliveryError(400, "invalid_start_at", "startAt must be earliest or latest");
    }
    this.catchUp();
    const existing = this.delivery.getConsumer(feedId, consumerId);
    if (existing) {
      return { status: 200, replayed: true, consumer: existing,
        retainedFromSeq: this.retainedFloor(feedId), latestSeq: this.delivery.maxSeq(feedId) };
    }
    const now = this.now();
    this.delivery.ensureFeedState(feedId, now);
    const initialAck = startAt === "latest" ? this.delivery.maxSeq(feedId) : 0;
    const consumer = this.delivery.createConsumer(feedId, consumerId, initialAck, now);
    return { status: 201, replayed: false, consumer,
      retainedFromSeq: this.retainedFloor(feedId), latestSeq: this.delivery.maxSeq(feedId) };
  }

  read(feedId, consumerId, { limit = DEFAULT_PAGE_LIMIT } = {}) {
    validateFeedId(feedId);
    validateConsumerId(consumerId);
    if (!this.feeds.get(feedId)) throw new DeliveryError(404, "feed_not_found", `unknown feed: ${feedId}`);
    const consumer = this.delivery.getConsumer(feedId, consumerId);
    if (!consumer) throw new DeliveryError(404, "consumer_not_found",
      `unknown delivery consumer: ${consumerId}`);
    this.catchUp();
    const retainedFromSeq = this.retainedFloor(feedId);
    if (consumer.lastAckedSeq !== 0 && consumer.lastAckedSeq < retainedFromSeq) {
      throw new DeliveryError(410, "cursor_expired",
        "the consumer cursor is older than the retained event window; re-register the consumer",
        { lastAckedSeq: consumer.lastAckedSeq, retainedFromSeq });
    }
    const pageSize = parseLimit(limit);
    const events = this.delivery.readEvents(feedId, consumer.lastAckedSeq, pageSize);
    const latestSeq = this.delivery.maxSeq(feedId);
    const nextCursor = events.length ? events.at(-1).seq : consumer.lastAckedSeq;
    const lastRetainedSeq = this.delivery.lastRetainedSeq(feedId);
    return {
      feedId,
      consumerId,
      events,
      cursor: { lastAckedSeq: consumer.lastAckedSeq, nextCursor },
      page: { limit: pageSize, returned: events.length, hasMore: lastRetainedSeq !== null && nextCursor < lastRetainedSeq },
      retention: { retainedFromSeq, latestSeq },
      omissionNote: OMISSION_NOTE
    };
  }

  checkpoint(feedId, consumerId, seq) {
    validateFeedId(feedId);
    validateConsumerId(consumerId);
    if (!this.feeds.get(feedId)) throw new DeliveryError(404, "feed_not_found", `unknown feed: ${feedId}`);
    if (!Number.isSafeInteger(seq) || seq < 0) {
      throw new DeliveryError(400, "invalid_seq", "seq must be a non-negative safe integer");
    }
    const consumer = this.delivery.getConsumer(feedId, consumerId);
    if (!consumer) throw new DeliveryError(404, "consumer_not_found",
      `unknown delivery consumer: ${consumerId}`);
    this.catchUp();
    const latestSeq = this.delivery.maxSeq(feedId);
    if (seq > latestSeq) {
      throw new DeliveryError(409, "seq_not_available",
        "seq is beyond the newest delivered event", { latestSeq });
    }
    const retainedFromSeq = this.retainedFloor(feedId);
    if (seq !== 0 && seq < retainedFromSeq) {
      throw new DeliveryError(410, "cursor_expired",
        "the checkpoint is older than the retained event window; re-register the consumer",
        { seq, retainedFromSeq });
    }
    if (seq < consumer.lastAckedSeq) {
      throw new DeliveryError(409, "stale_checkpoint",
        "checkpoints can only move forward", { current: consumer.lastAckedSeq });
    }
    if (seq === consumer.lastAckedSeq) {
      return { status: 200, unchanged: true, consumer, retainedFromSeq, latestSeq };
    }
    this.delivery.checkpoint(feedId, consumerId, seq, this.now());
    return { status: 200, unchanged: false,
      consumer: this.delivery.getConsumer(feedId, consumerId), retainedFromSeq, latestSeq };
  }

  sweep(feedId, { now: nowOverride } = {}) {
    validateFeedId(feedId);
    if (!this.feeds.get(feedId)) throw new DeliveryError(404, "feed_not_found", `unknown feed: ${feedId}`);
    this.catchUp();
    const nowValue = nowOverride ? Date.parse(nowOverride) : this.clock().getTime();
    if (Number.isNaN(nowValue)) throw new DeliveryError(400, "invalid_time", "sweep time must be a valid date-time");
    const cutoff = new Date(nowValue - this.retentionDays * 24 * 60 * 60 * 1000).toISOString();
    const oldestAt = this.delivery.oldestEventAt(feedId);
    const state = this.delivery.ensureFeedState(feedId, this.now());
    let pruned = 0;
    let retainedFromSeq = state.retainedFromSeq;
    if (oldestAt && oldestAt < cutoff) {
      const boundary = this.findRetentionBoundary(feedId, cutoff);
      pruned = this.delivery.pruneBefore(feedId, boundary.seq);
      retainedFromSeq = boundary.seq;
      this.delivery.setRetainedFromSeq(feedId, retainedFromSeq, this.now());
    }
    const expiredConsumers = this.delivery.consumers(feedId)
      .filter((consumer) => consumer.lastAckedSeq !== 0 && consumer.lastAckedSeq < retainedFromSeq)
      .map((consumer) => ({ consumerId: consumer.consumerId, lastAckedSeq: consumer.lastAckedSeq }));
    return {
      feedId,
      retentionDays: this.retentionDays,
      cutoffAt: cutoff,
      prunedEvents: pruned,
      retainedFromSeq,
      latestSeq: this.delivery.maxSeq(feedId),
      expiredConsumers
    };
  }

  findRetentionBoundary(feedId, cutoff) {
    let afterSeq = 0;
    for (;;) {
      const page = this.delivery.readEvents(feedId, afterSeq, MAX_PAGE_LIMIT);
      if (!page.length) return { seq: this.delivery.maxSeq(feedId) + 1 };
      const fresh = page.find((event) => event.capturedAt >= cutoff);
      if (fresh) return { seq: fresh.seq };
      afterSeq = page.at(-1).seq;
      if (page.length < MAX_PAGE_LIMIT) return { seq: this.delivery.maxSeq(feedId) + 1 };
    }
  }

  retainedFloor(feedId) {
    return this.delivery.ensureFeedState(feedId, this.now()).retainedFromSeq;
  }
}

function parseLimit(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new DeliveryError(400, "invalid_limit", "limit must be a positive integer");
  }
  return Math.min(parsed, MAX_PAGE_LIMIT);
}

export function validateConsumerId(consumerId) {
  if (typeof consumerId !== "string" || !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/.test(consumerId)) {
    throw new DeliveryError(400, "invalid_consumer_id",
      "consumerId must be 1 to 64 safe letters, digits, dots, underscores or hyphens");
  }
}
