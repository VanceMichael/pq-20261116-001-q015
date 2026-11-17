import test from "node:test";
import assert from "node:assert/strict";
import { makeArchive, baseFeed, captureTime, page, standardItems } from "../helpers.js";
import { NewsArchive } from "../../src/service/archive.js";
const fixedPage = (items) => page(items.map((item) => ({ ...item, time: "2026-11-16T01:00:00.000Z" })));

test("keeps the capture committed when delivery indexing fails and recovers it on the next read", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.database.exec(`CREATE TRIGGER block_delivery_events BEFORE INSERT ON delivery_events
    BEGIN SELECT RAISE(ABORT, 'simulated index fault'); END;`);
  const capture = archive.importSnapshot("daily-main", fixedPage(standardItems(4)), captureTime(1));
  assert.equal(capture.accepted, true);
  assert.equal(archive.listCaptures("daily-main").length, 1);
  assert.equal(archive.articles.stats("daily-main").total, 4);
  assert.equal(archive.database.prepare("SELECT COUNT(*) AS n FROM delivery_inbox").get().n, 4);
  assert.equal(archive.database.prepare("SELECT COUNT(*) AS n FROM delivery_events").get().n, 0);
  assert.equal(archive.database.prepare("SELECT last_seq FROM delivery_feed_state").get().last_seq, 0);
  archive.database.exec("DROP TRIGGER block_delivery_events");
  // A consumer read self-heals the interrupted indexing.
  archive.registerDeliveryConsumer("daily-main", "c", {});
  const read = archive.readDelivery("daily-main", "c", {});
  assert.equal(read.events.length, 4);
  assert.equal(archive.database.prepare("SELECT COUNT(*) AS n FROM delivery_inbox").get().n, 0);
});

test("recovers staged captures after the process exits between commit and delivery indexing", async (t) => {
  const { filename, directory } = await makeArchive(t);
  let archive = new NewsArchive(filename);
  archive.registerFeed(baseFeed);
  archive.database.exec(`CREATE TRIGGER block_delivery_events BEFORE INSERT ON delivery_events
    BEGIN SELECT RAISE(ABORT, 'simulated index fault'); END;`);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  archive.close();

  // Reopen with a brand-new process-like instance; the trigger is dropped first.
  archive = new NewsArchive(filename);
  archive.database.exec("DROP TRIGGER block_delivery_events");
  const recovered = archive.recoverDelivery();
  assert.equal(recovered.length, 1);
  assert.equal(recovered[0].eventCount, 3);
  assert.deepEqual(recovered[0].eventSeqs, [1, 2, 3]);
  // Recovery is idempotent: running it again changes nothing.
  assert.deepEqual(archive.recoverDelivery(), []);
  archive.registerDeliveryConsumer("daily-main", "c", {});
  assert.equal(archive.readDelivery("daily-main", "c", {}).events.length, 3);
  archive.close();

  const reopened = new NewsArchive(filename);
  t.after(() => reopened.close());
  assert.equal(reopened.database.prepare("SELECT COUNT(*) AS n FROM delivery_inbox").get().n, 0);
  assert.equal(reopened.database.prepare("SELECT COUNT(*) AS n FROM delivery_index_state").get().n, 1);
});

test("indexes a backlog of several pending captures in capture order after recovery", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.database.exec(`CREATE TRIGGER block_delivery_events BEFORE INSERT ON delivery_events
    BEGIN SELECT RAISE(ABORT, 'simulated index fault'); END;`);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  archive.importSnapshot("daily-main", `${fixedPage(standardItems(3))}<!--b-->`, captureTime(2));
  archive.database.exec("DROP TRIGGER block_delivery_events");
  const recovered = archive.recoverDelivery();
  assert.deepEqual(recovered.map((entry) => entry.captureId), [1, 2]);
  archive.registerDeliveryConsumer("daily-main", "c", {});
  const events = archive.readDelivery("daily-main", "c", { limit: 500 }).events;
  assert.deepEqual(events.map((event) => event.captureId).filter((id, index, all) => all.indexOf(id) === index), [1, 2]);
  assert.deepEqual(events.map((event) => event.seq), events.map((_, index) => index + 1));
});

test("a quality-rejected page is never staged or delivered even when its HTTP fetch succeeded", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  // Block delivery indexing so accepted captures remain staged in the inbox.
  archive.database.exec(`CREATE TRIGGER block_delivery_events BEFORE INSERT ON delivery_events
    BEGIN SELECT RAISE(ABORT, 'simulated index fault'); END;`);
  archive.importSnapshot("daily-main", fixedPage(standardItems(4)), captureTime(1));
  const rejected = archive.importSnapshot("daily-main", fixedPage(standardItems(1, 30)), captureTime(2));
  assert.equal(rejected.accepted, false);
  // Only the accepted capture is staged; the rejection never touches the delivery pipeline.
  assert.deepEqual(
    archive.database.prepare("SELECT DISTINCT capture_id FROM delivery_inbox").all().map((row) => row.capture_id),
    [1]
  );
  assert.equal(
    archive.database.prepare("SELECT COUNT(*) AS n FROM delivery_index_state WHERE capture_id=?").get(rejected.captureId).n,
    0
  );
  archive.database.exec("DROP TRIGGER block_delivery_events");
  archive.recoverDelivery();
  archive.registerDeliveryConsumer("daily-main", "c", {});
  const events = archive.readDelivery("daily-main", "c", { limit: 500 }).events;
  assert.ok(events.length > 0);
  assert.ok(events.every((event) => event.captureId !== rejected.captureId));
  assert.equal(archive.auditIntegrity().checks.find((check) => check.name === "delivery_inbox_orphans").passed, true);
});

test("delivered payloads stay byte-stable when later captures change the same article", async (t) => {
  const { archive } = await makeArchive(t);
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(3)), captureTime(1));
  archive.registerDeliveryConsumer("daily-main", "c", {});
  const firstRead = archive.readDelivery("daily-main", "c", {});
  archive.importSnapshot("daily-main", fixedPage([
    { id: 1, title: "全新标题" }, { id: 2 }, { id: 3 }
  ]), captureTime(2));
  const secondRead = archive.readDelivery("daily-main", "c", {});
  // The already-delivered page remains identical; only the new correction event appears.
  assert.deepEqual(secondRead.events.slice(0, firstRead.events.length), firstRead.events);
  const correction = secondRead.events.find((event) => event.eventType === "field_corrected");
  assert.equal(correction.payload.previousValue, "标题1");
  assert.equal(correction.payload.currentValue, "全新标题");
});

test("an HTTP 200 fetch rejected by quality rules never becomes a delivery change", async (t) => {
  const { archive } = await makeArchive(t, {
    fetcher: {
      fetchFeed: async (feed) => ({
        status: 200, html: fixedPage(standardItems(1)),
        requestedUrl: feed.baseUrl, finalUrl: feed.baseUrl, attemptCount: 1
      })
    }
  });
  archive.registerFeed(baseFeed);
  archive.importSnapshot("daily-main", fixedPage(standardItems(4)), captureTime(1));
  const refresh = await archive.refreshFeed("daily-main");
  assert.equal(refresh.outcome, "rejected");
  assert.equal(refresh.httpStatus, 200);
  archive.registerDeliveryConsumer("daily-main", "c", {});
  const events = archive.readDelivery("daily-main", "c", { limit: 500 }).events;
  assert.equal(events.length, 4);
  assert.ok(events.every((event) => event.captureId !== refresh.capture.captureId));
});
