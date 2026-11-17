import { extractLinks, pageFingerprint } from "../feed/html.js";
import { normalizeCandidates } from "../feed/normalize.js";
import { validateFeedId } from "../store/feed-repository.js";
import { assessCapture, validateCapturedAt } from "./capture-policy.js";

export class ImportService {
  constructor({ database, feeds, captures, articles, delivery = null, clock = () => new Date() }) {
    this.database = database;
    this.feeds = feeds;
    this.captures = captures;
    this.articles = articles;
    this.delivery = delivery;
    this.clock = clock;
  }

  importHtml(feedId, html, capturedAtValue) {
    validateFeedId(feedId);
    const feed = this.feeds.get(feedId);
    if (!feed) throw new Error(`unknown feed: ${feedId}`);
    const capturedAt = validateCapturedAt(capturedAtValue, this.clock());
    const fingerprint = pageFingerprint(html);
    const previous = this.captures.findByFingerprint(feedId, fingerprint);
    if (previous) return { ...previous, replay: true, stats: this.articles.stats(feedId) };

    const candidates = extractLinks(html, feed.baseUrl);
    const normalized = normalizeCandidates(candidates, { baseUrl: feed.baseUrl, allowedHosts: feed.allowedHosts });
    const policy = assessCapture({
      candidateCount: candidates.length,
      normalizedCount: normalized.records.length,
      rejectedCount: normalized.rejected.length,
      previousCount: feed.lastItemCount,
      capturedAt,
      feed
    });
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const captureId = this.captures.create({
        feedId,
        fingerprint,
        capturedAt,
        accepted: policy.accepted,
        candidateCount: candidates.length,
        normalizedCount: normalized.records.length,
        rejectedCount: normalized.rejected.length,
        duplicateCount: normalized.duplicateCount,
        reason: policy.reason
      });
      if (policy.accepted) {
        const recorded = this.articles.recordCapture(captureId, feed, normalized.records, capturedAt);
        // Staged inside the same transaction: once the capture commits, its
        // delivery payload is durable even if the process exits before indexing.
        this.delivery?.stage(captureId, feedId, capturedAt, recorded.staged);
        this.feeds.markAccepted(feedId, capturedAt, normalized.records.length);
      }
      this.database.exec("COMMIT");
      if (policy.accepted && this.delivery) {
        // Indexing is crash-safe and idempotent; a failure here must not undo
        // the committed capture, and later reads/refresh will retry it.
        this.delivery.catchUp();
      }
      return {
        captureId,
        feedId,
        fingerprint,
        capturedAt,
        accepted: policy.accepted,
        replay: false,
        candidateCount: candidates.length,
        normalizedCount: normalized.records.length,
        rejectedCount: normalized.rejected.length,
        duplicateCount: normalized.duplicateCount,
        reason: policy.reason,
        rejected: normalized.rejected,
        stats: this.articles.stats(feedId)
      };
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }
}
