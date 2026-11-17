import test from "node:test";
import assert from "node:assert/strict";
import { makeArchive, baseFeed, captureTime, page, standardItems } from "../helpers.js";

const fixedPage = (items) => page(items.map((item) => ({ ...item, time: "2026-11-16T01:00:00.000Z" })));
const DAY = 24 * 60 * 60 * 1000;

function seeded(t, retentionDays = 7) {
  const clock = { current: Date.parse("2026-11-16T02:00:30.000Z") };
  return makeArchive(t, {
    retentionDays,
    clock: () => new Date(clock.current)
  }).then((context) => ({ ...context, clock }));
}

test("sweep removes only events older than the retention window and keeps the sequence counter", async (t) => {
  const { archive } = await seeded(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  archive.importSnapshot("daily-main", `${fixedPage(standardItems(3))}<!--b-->`, captureTime(2));
  archive.registerDeliveryConsumer("daily-main", "c", {});
  const before = archive.readDelivery("daily-main", "c", {});
  assert.equal(before.events.length, 6);
  const sweepNow = new Date(Date.parse(captureTime(1)) + 8 * DAY).toISOString();
  const sweep = archive.sweepDelivery("daily-main", { now: sweepNow });
  assert.ok(sweep.prunedEvents > 0);
  assert.equal(sweep.latestSeq, 6);
  assert.equal(sweep.retainedFromSeq, sweep.prunedEvents + 1);
  // New events continue the per-feed sequence instead of reusing pruned numbers.
  archive.importSnapshot("daily-main", `${fixedPage(standardItems(3))}<!--c-->`, captureTime(3));
  const after = archive.readDelivery("daily-main", "c", {});
  assert.ok(after.events.every((event) => event.seq >= sweep.retainedFromSeq));
  assert.equal(after.retention.latestSeq, 9);
});

test("an expired cursor fails loudly with 410 instead of silently jumping to the newest event", async (t) => {
  const { archive } = await seeded(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  archive.importSnapshot("daily-main", `${fixedPage(standardItems(3))}<!--b-->`, captureTime(2));
  archive.registerDeliveryConsumer("daily-main", "slow", {});
  archive.checkpointDelivery("daily-main", "slow", 2);
  const sweepNow = new Date(Date.parse(captureTime(1)) + 8 * DAY).toISOString();
  const sweep = archive.sweepDelivery("daily-main", { now: sweepNow });
  assert.ok(2 < sweep.retainedFromSeq);
  assert.deepEqual(sweep.expiredConsumers, [{ consumerId: "slow", lastAckedSeq: 2 }]);
  const expired = () => archive.readDelivery("daily-main", "slow", {});
  assert.throws(expired, (error) => error.statusCode === 410 && error.code === "cursor_expired");
  try {
    expired();
  } catch (error) {
    assert.deepEqual(error.details, { lastAckedSeq: 2, retainedFromSeq: sweep.retainedFromSeq });
  }
  const expiredCheckpoint = () => archive.checkpointDelivery("daily-main", "slow", 2);
  assert.throws(expiredCheckpoint, (error) => error.statusCode === 410 && error.code === "cursor_expired");
  // A different, fresh consumer starting at the retained window is unaffected.
  archive.registerDeliveryConsumer("daily-main", "fresh", { startAt: "earliest" });
  assert.doesNotThrow(() => archive.readDelivery("daily-main", "fresh", {}));
});

test("a cursor at sequence zero never expires and reads whatever the window still retains", async (t) => {
  const { archive } = await seeded(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  archive.registerDeliveryConsumer("daily-main", "new", {});
  archive.sweepDelivery("daily-main", { now: new Date(Date.parse(captureTime(1)) + 8 * DAY).toISOString() });
  const read = archive.readDelivery("daily-main", "new", {});
  assert.equal(read.cursor.lastAckedSeq, 0);
  assert.deepEqual(read.events, []);
  assert.equal(read.page.hasMore, false);
});

test("a sweep inside the retention window prunes nothing", async (t) => {
  const { archive } = await seeded(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  const sweep = archive.sweepDelivery("daily-main", { now: new Date(Date.parse(captureTime(1)) + DAY).toISOString() });
  assert.equal(sweep.prunedEvents, 0);
  assert.equal(sweep.retainedFromSeq, 1);
});

test("sweep boundary keeps events at or newer than the cutoff even when older events exist", async (t) => {
  const { archive, clock } = await seeded(t, 2);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  const later = "2026-11-20T02:00:01.000Z";
  clock.current = Date.parse(later);
  archive.importSnapshot("daily-main", `${fixedPage(standardItems(3))}<!--late-->`, later);
  const sweep = archive.sweepDelivery("daily-main", { now: "2026-11-22T02:00:01.000Z" });
  assert.equal(sweep.prunedEvents, 3);
  assert.equal(sweep.retainedFromSeq, 4);
  archive.registerDeliveryConsumer("daily-main", "c", {});
  const read = archive.readDelivery("daily-main", "c", {});
  assert.deepEqual(read.events.map((event) => event.seq), [4, 5, 6]);
});
