import { Table, Vector } from "apache-arrow";
import { createServer } from "node:net";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";

import {
  KolaIOError,
  KolaTimestamp,
  Q,
  serializeAsIpcBytes6,
} from "../dist/index.js";

const liveEnabled = process.env.KOLA_TEST_Q_EXTERNAL === "1";
const host = process.env.KOLA_TEST_Q_HOST ?? "127.0.0.1";
const port = Number(process.env.KOLA_TEST_Q_PORT ?? "1801");
const expectedRows = Number(process.env.KOLA_Q_ROWS ?? "10000");
const expectedTimestampNanoseconds =
  BigInt(Date.UTC(2024, 0, 2, 3, 4, 5, 123)) * 1_000_000n + 456_789n;
const connections: Q[] = [];

function trackedQ(): Q {
  const q = new Q({ host, port });
  connections.push(q);
  return q;
}

async function trackedConnectedQ(): Promise<Q> {
  const q = await Q.connect({ host, port });
  connections.push(q);
  return q;
}

afterEach(async () => {
  const active = connections.splice(0);
  const results = await Promise.allSettled(active.map((q) => q.disconnect()));
  const failed = results.find((result) => result.status === "rejected");
  if (failed?.status === "rejected") {
    throw failed.reason;
  }
});

describe.runIf(liveEnabled)("live q IPC", () => {
  it("connects explicitly, auto-connects, disconnects, and reconnects", async () => {
    expect(Number.isInteger(port) && port > 0 && port <= 65_535).toBe(true);
    expect(Number.isInteger(expectedRows) && expectedRows > 0).toBe(true);

    const explicit = trackedQ();
    await explicit.connect();
    await expect(explicit.sync("1b")).resolves.toBe(true);

    const staticConnection = await trackedConnectedQ();
    await expect(staticConnection.sync("1f+1f")).resolves.toBe(2);

    const automatic = trackedQ();
    await expect(automatic.sync("6f*7f")).resolves.toBe(42);
    await automatic.disconnect();
    await expect(automatic.sync("7f*6f")).resolves.toBe(42);
  });

  it("preserves scalar, BigInt, and non-millisecond timestamp values", async () => {
    const q = await trackedConnectedQ();

    await expect(q.sync("6f*7f")).resolves.toBe(42);
    await expect(q.sync("9007199254740993j")).resolves.toBe(9_007_199_254_740_993n);
    for (const boundary of [-(1n << 63n), (1n << 63n) - 1n]) {
      await expect(q.sync("{x}", boundary)).resolves.toBe(boundary);
    }

    const timestamp = await q.sync("2024.01.02D03:04:05.123456789");
    expect(timestamp).toBeInstanceOf(KolaTimestamp);
    if (!(timestamp instanceof KolaTimestamp)) {
      throw new Error("q timestamp did not decode as KolaTimestamp");
    }
    expect(timestamp.nanoseconds).toBe(expectedTimestampNanoseconds);

    const roundTripped = await q.sync("{x}", new KolaTimestamp(expectedTimestampNanoseconds));
    expect(roundTripped).toEqual(new KolaTimestamp(expectedTimestampNanoseconds));

    const preEpoch = new KolaTimestamp(-1n);
    await expect(q.sync("{x}", preEpoch)).resolves.toEqual(preEpoch);
  });

  it("round-trips arbitrary char-vector bytes", async () => {
    const q = trackedQ();
    const bytes = new Uint8Array([0, 1, 2, 127, 128, 254, 255]);

    const result = await q.sync("{x}", bytes);

    expect(Buffer.isBuffer(result)).toBe(true);
    expect(result).toEqual(Buffer.from(bytes));
  });

  it("reads and q-acknowledges an Arrow table send", async () => {
    const q = await trackedConnectedQ();
    const fixtureRows = await q.sync(".kola.rows");
    expect(fixtureRows).toBe(BigInt(expectedRows));

    const table = await q.sync("trade");
    expect(table).toBeInstanceOf(Table);
    if (!(table instanceof Table)) {
      throw new Error("trade did not decode as an Arrow Table");
    }
    expect(table.numRows).toBe(expectedRows);
    expect(table.schema.fields.map((field) => field.name)).toEqual([
      "sym",
      "time",
      "volume",
      "cond",
      "ask0",
      "ask1",
      "ask2",
      "ask3",
      "ask4",
      "bid0",
      "bid1",
      "bid2",
      "bid3",
      "bid4",
    ]);

    await expect(q.sync("{x~trade}", table)).resolves.toBe(true);
    await expect(q.sync("{count x}", table)).resolves.toBe(BigInt(expectedRows));
    const depth = await q.sync("depth");
    expect(depth).toBeInstanceOf(Table);
    await expect(q.sync("{x~depth}", depth)).resolves.toBe(true);
    const serialized = await serializeAsIpcBytes6("sync", false, table);
    expect(Buffer.isBuffer(serialized)).toBe(true);
    expect(serialized.byteLength).toBeGreaterThan(8);
  });

  it("submits concurrent calls to one connection in FIFO order", async () => {
    const q = await trackedConnectedQ();
    await q.sync(".kola.nodeFifo:0#0j");

    const submitted = Array.from({ length: 8 }, (_, index) =>
      q.sync("{.kola.nodeFifo,:x;x}", BigInt(index)),
    );
    await expect(Promise.all(submitted)).resolves.toEqual(
      Array.from({ length: 8 }, (_, index) => BigInt(index)),
    );

    const observed = await q.sync(".kola.nodeFifo");
    expect(observed).toBeInstanceOf(Vector);
    if (!(observed instanceof Vector)) {
      throw new Error("FIFO probe did not return an Arrow Vector");
    }
    expect(Array.from(observed)).toEqual(
      Array.from({ length: 8 }, (_, index) => BigInt(index)),
    );
  });

  it("keeps the event loop live while q handles a slow request", async () => {
    const q = await trackedConnectedQ();
    let eventLoopTicks = 0;
    const interval = setInterval(() => {
      eventLoopTicks += 1;
    }, 25);

    // The external q process must stay busy long enough to observe Node's real event loop;
    // fake timers cannot exercise whether the native IPC worker blocks that loop.
    let result: unknown;
    try {
      result = await q.sync('system "sleep 1";42f');
    } finally {
      clearInterval(interval);
    }

    expect(result).toBe(42);
    expect(eventLoopTicks).toBeGreaterThanOrEqual(5);
  });

  it("receives a q message after an async send on a dedicated connection", async () => {
    const dedicated = await trackedConnectedQ();

    await dedicated.asyn("{neg[.z.w] x}", 4_242n);
    await expect(dedicated.receive()).resolves.toBe(4_242n);
  });

  it("maps q evaluation failures to a stable server error", async () => {
    const q = await trackedConnectedQ();
    await expect(q.sync("1+`a")).rejects.toMatchObject({
      name: "KolaError",
      code: "KOLA_SERVER",
    });
  });

  it("maps a stable refused connection to KolaIOError", async () => {
    const portProbe = createServer();
    await new Promise<void>((resolve, reject) => {
      portProbe.once("error", reject);
      portProbe.listen(0, "127.0.0.1", resolve);
    });
    const closedPort = (portProbe.address() as AddressInfo).port;
    await new Promise<void>((resolve, reject) => {
      portProbe.close((error) => (error === undefined ? resolve() : reject(error)));
    });

    const q = new Q({ host: "127.0.0.1", port: closedPort, timeout: 1_000 });
    connections.push(q);
    await expect(q.connect()).rejects.toMatchObject({
      name: "KolaIOError",
      code: "KOLA_IO",
    });
    await expect(q.connect()).rejects.toBeInstanceOf(KolaIOError);
  });
});
