import test from "node:test";
import assert from "node:assert/strict";
import { makeArchive, baseFeed, captureTime, page, standardItems } from "../helpers.js";
import { createHttpServer } from "../../src/http/server.js";

const fixedPage = (items) => page(items.map((item) => ({ ...item, time: "2026-11-16T01:00:00.000Z" })));

async function start(t, archive) {
  const app = createHttpServer(archive, { host: "127.0.0.1", port: 0, logger: { error() {} } });
  const address = await app.listen();
  t.after(() => app.close());
  return `http://127.0.0.1:${address.port}/v1/feeds`;
}

test("registers a delivery consumer and pages events with checkpoint continuation over HTTP", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(4)), captureTime(1));
  archive.importSnapshot("daily-main", fixedPage([{ id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]), captureTime(2));
  const root = await start(t, archive);

  const created = await fetch(`${root}/daily-main/delivery/editor-1`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ startAt: "earliest" })
  });
  assert.equal(created.status, 201);
  const createdBody = await created.json();
  assert.equal(createdBody.consumer.consumerId, "editor-1");
  assert.ok(createdBody.retainedFromSeq >= 1);

  const first = await fetch(`${root}/daily-main/delivery/editor-1?limit=4`);
  assert.equal(first.status, 200);
  const firstBody = await first.json();
  assert.equal(firstBody.events.length, 4);
  assert.equal(firstBody.page.hasMore, true);
  assert.equal(firstBody.omissionNote.includes("not that the publisher deleted"), true);

  const cursor = firstBody.cursor.nextCursor;
  const ack = await fetch(`${root}/daily-main/delivery/editor-1/checkpoint`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ seq: cursor })
  });
  assert.equal(ack.status, 200);

  const second = await (await fetch(`${root}/daily-main/delivery/editor-1?limit=50`)).json();
  assert.equal(second.cursor.lastAckedSeq, cursor);
  assert.ok(second.events.every((event) => event.seq > cursor));
  const omission = second.events.find((event) => event.eventType === "omitted");
  assert.equal(omission.payload.note.includes("not that the publisher deleted"), true);
});

test("two HTTP consumers advance independently through the same stream", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(4)), captureTime(1));
  const root = await start(t, archive);
  for (const consumer of ["desk-a", "desk-b"]) {
    const response = await fetch(`${root}/daily-main/delivery/${consumer}`, { method: "POST", body: "{}" });
    assert.equal(response.status, 201);
  }
  const pageA = await (await fetch(`${root}/daily-main/delivery/desk-a?limit=2`)).json();
  await fetch(`${root}/daily-main/delivery/desk-a/checkpoint`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ seq: pageA.events.at(-1).seq })
  });
  const pageB = await (await fetch(`${root}/daily-main/delivery/desk-b?limit=2`)).json();
  assert.equal(pageB.cursor.lastAckedSeq, 0);
  assert.deepEqual(pageB.events.map((event) => event.seq), [1, 2]);
});

test("surfaces expired cursors as 410 and validation problems as 400/404", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  const root = await start(t, archive);
  await fetch(`${root}/daily-main/delivery/slow`, { method: "POST", body: "{}" });
  archive.checkpointDelivery("daily-main", "slow", 2);
  archive.sweepDelivery("daily-main", { now: "2026-12-01T00:00:00.000Z" });
  const expired = await fetch(`${root}/daily-main/delivery/slow`);
  assert.equal(expired.status, 410);
  const expiredBody = await expired.json();
  assert.equal(expiredBody.error, "cursor_expired");
  assert.equal(expiredBody.details.lastAckedSeq, 2);

  const unknownFeed = await fetch(`${root}/absent/delivery/c`);
  assert.equal(unknownFeed.status, 404);
  const unknownConsumer = await fetch(`${root}/daily-main/delivery/nobody`);
  assert.equal(unknownConsumer.status, 404);
  const badId = await fetch(`${root}/daily-main/delivery/bad%2Fid`, { method: "POST", body: "{}" });
  assert.equal(badId.status, 400);
  const badStart = await fetch(`${root}/daily-main/delivery/c2`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ startAt: "middle" })
  });
  assert.equal(badStart.status, 400);
  const malformedSeq = await fetch(`${root}/daily-main/delivery/slow/checkpoint`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ seq: "nope" })
  });
  assert.equal(malformedSeq.status, 400);
});

test("exposes retention sweeps and keeps the original CSV export endpoint working", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  const root = await start(t, archive);
  const sweep = await fetch(`${root}/daily-main/delivery/retention`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ now: "2026-12-01T00:00:00.000Z" })
  });
  assert.equal(sweep.status, 200);
  const sweepBody = await sweep.json();
  assert.ok(sweepBody.prunedEvents >= 3);

  const csv = await fetch(`${root}/daily-main/items.csv`);
  assert.equal(csv.status, 200);
  const text = await csv.text();
  assert.match(text.split("\r\n")[0], /^article_id,title,canonical_url/);
  assert.equal(text.split("\r\n").filter(Boolean).length, 4);
});
