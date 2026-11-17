import test from "node:test";
import assert from "node:assert/strict";
import { makeArchive, baseFeed, captureTime, page, standardItems } from "../helpers.js";

const itemUrl = (id) => `https://news.example.test/2026/11-16/article-${id}.shtml`;
const fixedTimePage = (items) => page(items.map((item) => ({ ...item, time: "2026-11-16T01:00:00.000Z" })));

function seedThreeCaptures(archive) {
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedTimePage(standardItems(4)), captureTime(1));
  archive.importSnapshot("daily-main", fixedTimePage([{ id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]), captureTime(2));
  archive.importSnapshot("daily-main", fixedTimePage([
    { id: 2, title: "改名二" }, { id: 3 }, { id: 4 }, { id: 5 }
  ]), captureTime(3));
}

test("delivers accepted observations, field corrections and omissions as an ordered immutable stream", async (t) => {
  const { archive } = await makeArchive(t);
  seedThreeCaptures(archive);
  archive.registerDeliveryConsumer("daily-main", "editor-1", { startAt: "earliest" });
  const read = archive.readDelivery("daily-main", "editor-1", { limit: 200 });
  const types = read.events.map((event) => event.eventType);
  // First capture: four observations. Second capture: observations for 2-5 plus omission of 1.
  assert.equal(types.filter((type) => type === "observed").length, 12);
  const omitted = read.events.filter((event) => event.eventType === "omitted");
  assert.equal(omitted.length, 1);
  assert.equal(omitted[0].payload.canonicalUrl, itemUrl(1));
  assert.equal(omitted[0].payload.previousSourceIndex, 0);
  assert.match(omitted[0].payload.note, /not that the publisher deleted/);
  assert.match(read.omissionNote, /not that the publisher deleted/);
  const correction = read.events.find((event) => event.eventType === "field_corrected");
  assert.equal(correction.payload.field, "title");
  assert.equal(correction.payload.previousValue, "标题2");
  assert.equal(correction.payload.currentValue, "改名二");
  // Sequences are dense, per-feed and monotonic.
  assert.deepEqual(read.events.map((event) => event.seq), read.events.map((_, index) => index + 1));
});

test("flags only the first observation of an article while keeping re-appearances as ordinary observations", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedTimePage(standardItems(4)), captureTime(1));
  const reduced = fixedTimePage(standardItems(3, 2));
  for (const second of [2, 3, 4]) archive.importSnapshot("daily-main", `${reduced}<!--${second}-->`, captureTime(second));
  archive.importSnapshot("daily-main", `${fixedTimePage(standardItems(4))}<!--back-->`, captureTime(5));
  archive.registerDeliveryConsumer("daily-main", "c", {});
  const events = archive.readDelivery("daily-main", "c", { limit: 500 }).events;
  const firstSeen = events.filter((event) => event.eventType === "observed" && event.payload.firstObserved);
  assert.deepEqual(firstSeen.map((event) => event.payload.canonicalUrl), [1, 2, 3, 4].map(itemUrl));
  const reappearance = events.find((event) => event.eventType === "observed"
    && event.payload.canonicalUrl === itemUrl(1) && !event.payload.firstObserved);
  assert.ok(reappearance);
  const omissions = events.filter((event) => event.eventType === "omitted" && event.payload.canonicalUrl === itemUrl(1));
  assert.equal(omissions.length, 1);
});

test("paginates from an independent cursor and resumes after an explicit checkpoint", async (t) => {
  const { archive } = await makeArchive(t);
  seedThreeCaptures(archive);
  archive.registerDeliveryConsumer("daily-main", "editor-1", {});
  const first = archive.readDelivery("daily-main", "editor-1", { limit: 5 });
  assert.equal(first.events.length, 5);
  assert.equal(first.cursor.lastAckedSeq, 0);
  assert.equal(first.cursor.nextCursor, 5);
  assert.equal(first.page.hasMore, true);
  // Re-reading without a checkpoint must return the same page, unchanged.
  const repeated = archive.readDelivery("daily-main", "editor-1", { limit: 5 });
  assert.deepEqual(repeated.events, first.events);
  const checkpoint = archive.checkpointDelivery("daily-main", "editor-1", 5);
  assert.equal(checkpoint.status, 200);
  const second = archive.readDelivery("daily-main", "editor-1", { limit: 500 });
  assert.equal(second.cursor.lastAckedSeq, 5);
  assert.equal(second.events[0].seq, 6);
  assert.deepEqual([...first.events, ...second.events].map((event) => event.seq),
    [...first.events, ...second.events].map((_, index) => index + 1));
});

test("keeps consumers independent so one consumer's checkpoint never moves another", async (t) => {
  const { archive } = await makeArchive(t);
  seedThreeCaptures(archive);
  archive.registerDeliveryConsumer("daily-main", "a", {});
  archive.registerDeliveryConsumer("daily-main", "b", {});
  const pageA = archive.readDelivery("daily-main", "a", { limit: 3 });
  archive.checkpointDelivery("daily-main", "a", pageA.events.at(-1).seq);
  const pageB = archive.readDelivery("daily-main", "b", { limit: 3 });
  assert.equal(pageB.cursor.lastAckedSeq, 0);
  assert.deepEqual(pageB.events.map((event) => event.seq), [1, 2, 3]);
  assert.equal(archive.readDelivery("daily-main", "a", {}).cursor.lastAckedSeq, 3);
});

test("a latest-position consumer only sees events published after registration", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedTimePage(standardItems(3)), captureTime(1));
  const registration = archive.registerDeliveryConsumer("daily-main", "late", { startAt: "latest" });
  assert.equal(registration.consumer.lastAckedSeq, 3);
  assert.equal(archive.readDelivery("daily-main", "late", {}).events.length, 0);
  archive.importSnapshot("daily-main", `${fixedTimePage(standardItems(3))}<!--new-->`, captureTime(2));
  const read = archive.readDelivery("daily-main", "late", {});
  assert.ok(read.events.length >= 1);
  assert.ok(read.events.every((event) => event.seq > 3));
});

test("re-registration is idempotent and never resets an existing cursor", async (t) => {
  const { archive } = await makeArchive(t);
  seedThreeCaptures(archive);
  const first = archive.registerDeliveryConsumer("daily-main", "c", { startAt: "earliest" });
  assert.equal(first.status, 201);
  archive.checkpointDelivery("daily-main", "c", 7);
  const again = archive.registerDeliveryConsumer("daily-main", "c", { startAt: "latest" });
  assert.equal(again.status, 200);
  assert.equal(again.replayed, true);
  assert.equal(again.consumer.lastAckedSeq, 7);
});

test("rejects backward, out-of-range and malformed checkpoints instead of jumping cursors", async (t) => {
  const { archive } = await makeArchive(t);
  seedThreeCaptures(archive);
  archive.registerDeliveryConsumer("daily-main", "c", {});
  archive.checkpointDelivery("daily-main", "c", 5);
  assert.throws(() => archive.checkpointDelivery("daily-main", "c", 4),
    (error) => error.statusCode === 409 && error.code === "stale_checkpoint");
  const duplicate = archive.checkpointDelivery("daily-main", "c", 5);
  assert.equal(duplicate.status, 200);
  assert.equal(duplicate.unchanged, true);
  assert.throws(() => archive.checkpointDelivery("daily-main", "c", 9999),
    (error) => error.statusCode === 409 && error.code === "seq_not_available");
  assert.throws(() => archive.checkpointDelivery("daily-main", "c", -1), /non-negative/);
  assert.throws(() => archive.checkpointDelivery("daily-main", "c", 1.5), /non-negative/);
  assert.equal(archive.readDelivery("daily-main", "c", {}).cursor.lastAckedSeq, 5);
});

test("returns structured not-found responses for unknown feeds and consumers", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  assert.throws(() => archive.registerDeliveryConsumer("missing", "c", {}), /unknown feed/);
  archive.registerDeliveryConsumer("daily-main", "c", {});
  assert.throws(() => archive.readDelivery("daily-main", "other", {}), /unknown delivery consumer/);
  assert.throws(() => archive.checkpointDelivery("daily-main", "other", 0), /unknown delivery consumer/);
  assert.throws(() => archive.registerDeliveryConsumer("daily-main", "bad/id", {}), /consumerId/);
  assert.throws(() => archive.registerDeliveryConsumer("daily-main", "d2", { startAt: "middle" }), /startAt/);
  assert.throws(() => archive.readDelivery("daily-main", "c", { limit: 0 }), /positive integer/);
});

test("bounds page sizes so a huge requested limit cannot exhaust memory", async (t) => {
  const { archive } = await makeArchive(t);
  seedThreeCaptures(archive);
  archive.registerDeliveryConsumer("daily-main", "c", {});
  const read = archive.readDelivery("daily-main", "c", { limit: 100000 });
  assert.equal(read.page.limit, 500);
});
