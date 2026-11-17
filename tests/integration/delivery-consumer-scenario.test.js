import test from "node:test";
import assert from "node:assert/strict";
import { makeArchive, baseFeed, captureTime, page, standardItems } from "../helpers.js";

// End-to-end simulation of a slow downstream editor consuming a rolling feed:
// resume after disconnect, repeated reads, retention boundary, and the promise
// that an omission never means the publisher deleted an article.
const DAY = 24 * 60 * 60 * 1000;
const fixedPage = (items) => page(items.map((item) => ({ ...item, time: "2026-11-16T01:00:00.000Z" })));
const at = (day, second = 0) => new Date(Date.parse("2026-11-16T02:00:00.000Z") + day * DAY + second * 1000).toISOString();

test("local consumer resumes, tolerates duplicate reads and survives the retention boundary", async (t) => {
  const clock = { current: Date.parse(at(0, 30)) };
  const { archive } = await makeArchive(t, { retentionDays: 3, clock: () => new Date(clock.current) });
  archive.registerFeed(baseFeed);

  // Day 0: four articles, then one rotates out while a new one rotates in.
  archive.importSnapshot("daily-main", fixedPage(standardItems(4)), at(0, 1));
  archive.importSnapshot("daily-main", fixedPage([{ id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]), at(0, 2));
  archive.registerDeliveryConsumer("daily-main", "desk", { startAt: "earliest" });

  // First read is interrupted after two events; no checkpoint is sent.
  const interrupted = archive.readDelivery("daily-main", "desk", { limit: 2 });
  assert.equal(interrupted.events.length, 2);
  // Reconnecting with the same cursor replays the identical page (at-least-once).
  const replayed = archive.readDelivery("daily-main", "desk", { limit: 2 });
  assert.deepEqual(replayed.events, interrupted.events);

  // Drain the rest in pages, checkpointing each page.
  let cursor = 0;
  const delivered = [];
  for (;;) {
    const read = archive.readDelivery("daily-main", "desk", { limit: 3 });
    if (!read.events.length) break;
    delivered.push(...read.events);
    cursor = read.cursor.nextCursor;
    archive.checkpointDelivery("daily-main", "desk", cursor);
    if (!read.page.hasMore) break;
  }
  const omission = delivered.find((event) => event.eventType === "omitted");
  assert.equal(omission.payload.canonicalUrl.endsWith("article-1.shtml"), true);
  assert.match(omission.payload.note, /not that the publisher deleted/);
  assert.ok(delivered.some((event) => event.eventType === "observed"
    && event.payload.canonicalUrl.endsWith("article-5.shtml") && event.payload.firstObserved));

  // Nothing new: reading again returns an empty page without moving the cursor.
  const idle = archive.readDelivery("daily-main", "desk", {});
  assert.equal(idle.events.length, 0);
  assert.equal(idle.cursor.lastAckedSeq, cursor);
  assert.equal(idle.page.hasMore, false);

  // Day 5: collection keeps running while the consumer is offline; old events age out.
  clock.current = Date.parse(at(5));
  archive.importSnapshot("daily-main", `${fixedPage(standardItems(4, 2))}<!--day5-->`, at(5, 1));
  const sweep = archive.sweepDelivery("daily-main", { now: at(6) });
  assert.ok(sweep.prunedEvents > 0);
  assert.ok(cursor < sweep.retainedFromSeq);

  // The stale cursor must fail explicitly rather than silently jump to newest.
  assert.throws(() => archive.readDelivery("daily-main", "desk", {}),
    (error) => error.statusCode === 410 && error.code === "cursor_expired");

  // Re-registering at the retained window lets the consumer continue without
  // re-reading history that no longer exists, while new events still flow.
  archive.registerDeliveryConsumer("daily-main", "desk-restarted", { startAt: "earliest" });
  const restarted = archive.readDelivery("daily-main", "desk-restarted", {});
  assert.ok(restarted.events.every((event) => event.seq >= sweep.retainedFromSeq));
  clock.current = Date.parse(at(7));
  archive.importSnapshot("daily-main", `${fixedPage(standardItems(4, 3))}<!--day7-->`, at(7, 1));
  const newer = archive.readDelivery("daily-main", "desk-restarted", {});
  assert.ok(newer.events.some((event) => event.capturedAt === at(7, 1)));
});
