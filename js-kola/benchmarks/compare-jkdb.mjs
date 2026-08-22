import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { cpus } from "node:os";
import { dirname, resolve } from "node:path";
import process from "node:process";
import { tableFromIPC, Table } from "apache-arrow";
import jkdb from "jkdb";

import { KolaTimestamp, Q } from "../dist/index.js";
import { NativeConnector } from "../native.js";

const runtimeRequire = createRequire(import.meta.url);
const kolaPackage = runtimeRequire("../package.json");
const jkdbPackage = runtimeRequire("jkdb/package.json");

const CLIENTS = ["kola", "jkdb"];
const REVERSED_CLIENTS = ["jkdb", "kola"];
const META = Symbol.for("meta");
const TIMESTAMP_EXPRESSION = "2024.01.02D03:04:05.123456789";
const TIMESTAMP_UNIX_MILLISECONDS = Date.UTC(2024, 0, 2, 3, 4, 5, 123);
const TIMESTAMP_UNIX_NANOSECONDS =
  BigInt(TIMESTAMP_UNIX_MILLISECONDS) * 1_000_000n + 456_789n;
const TIMESTAMP_NANOSECOND_TEXT = "2024-01-02T03:04:05.123456789";
const BIGINT_VALUE = 9_007_199_254_740_993n;
const BASE_COLUMNS = ["sym", "time", "volume", "cond"];
const TABLES = {
  trade: [...BASE_COLUMNS, ...quoteColumns(5)],
  wide: [...BASE_COLUMNS, ...quoteColumns(30)],
  depth: ["sym", "time", "volume", "ask", "bid"],
};
const CANONICAL_SERIALIZED_SIZE_LAMBDA = "{[x]count -8!x}";
const TIMED_SEND_LAMBDA = "{[x]count x}";
const PUBLIC_TABLE_REPRESENTATIONS = {
  kola: "Apache Arrow Table returned by Kola's documented public API",
  jkdb:
    "column-oriented object with Symbol.for(\"meta\") table metadata returned by jkdb's documented public API",
};
let pairedWorkloadIndex = 0;

function quoteColumns(levels) {
  return ["ask", "bid"].flatMap((side) =>
    Array.from({ length: levels }, (_, level) => `${side}${level}`),
  );
}

function fail(message) {
  throw new Error(message);
}

function parseInteger(raw, label, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    fail(`${label} must be an integer from ${minimum} through ${maximum}`);
  }
  return value;
}

function optionValue(flag, environmentName, fallback) {
  const positions = process.argv
    .slice(2)
    .map((value, index) => (value === flag ? index + 2 : -1))
    .filter((index) => index >= 0);
  if (positions.length > 1) {
    fail(`${flag} may be specified only once`);
  }
  if (positions.length === 1) {
    const value = process.argv[positions[0] + 1];
    if (value === undefined || value.startsWith("--")) {
      fail(`${flag} requires a value`);
    }
    return value;
  }
  return process.env[environmentName] ?? fallback;
}

function parseOptions() {
  const valueFlags = new Set([
    "--rows",
    "--warmups",
    "--iterations",
    "--memory-results",
    "--output",
  ]);
  for (let index = 2; index < process.argv.length; index += 1) {
    const argument = process.argv[index];
    if (!valueFlags.has(argument)) {
      fail(`Unknown argument: ${argument}`);
    }
    index += 1;
    if (index >= process.argv.length) {
      fail(`${argument} requires a value`);
    }
  }

  const iterations = parseInteger(
    optionValue("--iterations", "KOLA_BENCH_ITERATIONS", "100"),
    "--iterations",
    20,
    1_000,
  );
  if (iterations % 2 !== 0) {
    fail("--iterations must be even for ABBA-balanced client order");
  }

  return {
    host: process.env.KOLA_TEST_Q_HOST ?? "127.0.0.1",
    port: parseInteger(
      process.env.KOLA_TEST_Q_PORT ?? "1801",
      "KOLA_TEST_Q_PORT",
      1,
      65_535,
    ),
    rows: parseInteger(
      optionValue("--rows", "KOLA_BENCH_ROWS", "100000"),
      "--rows",
      1,
      2_000_000,
    ),
    warmups: parseInteger(
      optionValue("--warmups", "KOLA_BENCH_WARMUPS", "2"),
      "--warmups",
      0,
      100,
    ),
    iterations,
    memoryResults: parseInteger(
      optionValue("--memory-results", "KOLA_BENCH_MEMORY_RESULTS", "5"),
      "--memory-results",
      1,
      1_000,
    ),
    output: optionValue("--output", "KOLA_BENCH_OUTPUT", "") || undefined,
  };
}

function clientOrder(index, reversePattern) {
  const forward = index % 4 === 0 || index % 4 === 3;
  return forward !== reversePattern ? CLIENTS : REVERSED_CLIENTS;
}

async function timeAsync(operation) {
  const start = process.hrtime.bigint();
  const value = await operation();
  const durationNs = Number(process.hrtime.bigint() - start);
  return { value, durationNs };
}

function timeSync(operation) {
  const start = process.hrtime.bigint();
  const value = operation();
  const durationNs = Number(process.hrtime.bigint() - start);
  return { value, durationNs };
}

function median(values) {
  const ordered = [...values].sort((left, right) => left - right);
  const middle = Math.floor(ordered.length / 2);
  return ordered.length % 2 === 0
    ? (ordered[middle - 1] + ordered[middle]) / 2
    : ordered[middle];
}

function nearestRank(values, percentile) {
  const ordered = [...values].sort((left, right) => left - right);
  return ordered[Math.ceil(percentile * ordered.length) - 1];
}

function durationReport(samples) {
  return {
    rawSamplesNs: samples,
    summaryNs: {
      sampleCount: samples.length,
      median: median(samples),
      p95NearestRank: nearestRank(samples, 0.95),
    },
  };
}

function payloadRateReport(samples, logicalPayloadBytes) {
  const rawRatesBytesPerSecond = samples.map(
    (durationNs) => (logicalPayloadBytes * 1_000_000_000) / durationNs,
  );
  const durationP95 = nearestRank(samples, 0.95);
  return {
    logicalPayloadBytes,
    rawRatesBytesPerSecond,
    summaryBytesPerSecond: {
      sampleCount: rawRatesBytesPerSecond.length,
      medianRate: median(rawRatesBytesPerSecond),
      rateAtDurationP95: (logicalPayloadBytes * 1_000_000_000) / durationP95,
    },
  };
}

function metricReport(samples, logicalPayloadBytes) {
  return {
    duration: durationReport(samples),
    ...(logicalPayloadBytes === undefined
      ? {}
      : { logicalPayloadRate: payloadRateReport(samples, logicalPayloadBytes) }),
  };
}

async function runPairedSamples({ warmups, iterations, operations, validate }) {
  const reversePattern = pairedWorkloadIndex % 2 === 1;
  pairedWorkloadIndex += 1;
  for (let sample = 0; sample < warmups; sample += 1) {
    for (const client of clientOrder(sample, reversePattern)) {
      validate(client, await operations[client]());
    }
  }

  const samples = { kola: [], jkdb: [] };
  const order = [];
  for (let sample = 0; sample < iterations; sample += 1) {
    const currentOrder = clientOrder(sample, reversePattern);
    order.push([...currentOrder]);
    for (const client of currentOrder) {
      const measured = await timeAsync(operations[client]);
      validate(client, measured.value);
      samples[client].push(measured.durationNs);
    }
  }
  return { samples, order };
}

function validateScalar(_client, value) {
  assert.equal(value, 42, "scalar result mismatch");
}

function validateBigInt(_client, value) {
  assert.equal(value, BIGINT_VALUE, "BigInt result mismatch");
}

function validateTimestamp(client, value) {
  if (client === "kola") {
    assert(value instanceof KolaTimestamp, "Kola timestamp result is not KolaTimestamp");
    assert.equal(value.nanoseconds, TIMESTAMP_UNIX_NANOSECONDS, "Kola timestamp lost nanoseconds");
    return;
  }
  assert.equal(value, TIMESTAMP_NANOSECOND_TEXT, "jkdb timestamp lost nanoseconds");
}

function validateKolaTable(value, tableName, rows) {
  assert(value instanceof Table, `${tableName}: Kola result is not an Arrow Table`);
  assert.equal(value.numRows, rows, `${tableName}: Kola row count mismatch`);
  assert.deepEqual(
    value.schema.fields.map((field) => field.name),
    TABLES[tableName],
    `${tableName}: Kola columns mismatch`,
  );
}

function validateJkdbTable(value, tableName, rows) {
  assert(value !== null && typeof value === "object", `${tableName}: jkdb result is not an object`);
  const metadata = value[META];
  assert(metadata !== null && typeof metadata === "object", `${tableName}: jkdb metadata missing`);
  assert.deepEqual(metadata.c, TABLES[tableName], `${tableName}: jkdb columns mismatch`);
  assert(Array.isArray(metadata.t), `${tableName}: jkdb column type metadata missing`);
  assert.equal(metadata.t.length, TABLES[tableName].length, `${tableName}: jkdb type count mismatch`);
  for (const column of TABLES[tableName]) {
    assert(value[column] !== undefined, `${tableName}: jkdb column ${column} missing`);
    assert.equal(value[column].length, rows, `${tableName}: jkdb ${column} row count mismatch`);
  }
}

function validateTable(client, value, tableName, rows) {
  if (client === "kola") {
    validateKolaTable(value, tableName, rows);
  } else {
    validateJkdbTable(value, tableName, rows);
  }
}

function validateCount(_client, value, rows) {
  assert.equal(value, BigInt(rows), "q-acknowledged count mismatch");
}

function rawTableBytes(result, tableName) {
  assert.equal(result?.ok, true, `${tableName}: raw native request failed`);
  assert.equal(result?.value?.tag, "table", `${tableName}: raw native result is not a table`);
  const bytes = result.value.bytesValue;
  assert(bytes instanceof Uint8Array, `${tableName}: raw native table omitted Arrow IPC bytes`);
  assert(bytes.byteLength > 0, `${tableName}: raw native Arrow IPC payload is empty`);
  return bytes;
}

function validateRawTable(result, tableName, rows) {
  const bytes = rawTableBytes(result, tableName);
  validateKolaTable(tableFromIPC(bytes), tableName, rows);
  return bytes;
}

function memorySnapshot() {
  const value = process.memoryUsage();
  return {
    rss: value.rss,
    heapTotal: value.heapTotal,
    heapUsed: value.heapUsed,
    external: value.external,
    arrayBuffers: value.arrayBuffers,
  };
}

function subtractMemory(after, before) {
  return Object.fromEntries(
    Object.keys(before).map((key) => [key, after[key] - before[key]]),
  );
}

async function retainedResultMemory(iterations, operation, validate) {
  globalThis.gc();
  const beforeBytes = memorySnapshot();
  const retained = [];
  for (let index = 0; index < iterations; index += 1) {
    const value = await operation();
    validate(value);
    retained.push(value);
  }
  globalThis.gc();
  const afterBytes = memorySnapshot();
  assert.equal(retained.length, iterations);
  const report = {
    retainedResults: retained.length,
    beforeBytes,
    afterBytes,
    deltaBytes: subtractMemory(afterBytes, beforeBytes),
  };
  retained.length = 0;
  globalThis.gc();
  return report;
}

async function jkdbConnect(QConnection, options) {
  const connection = new QConnection({
    ...options,
    useBigInt: true,
  });
  await connection.connectAsync();
  return connection;
}

async function main() {
  if (typeof globalThis.gc !== "function") {
    fail("This benchmark requires Node.js --expose-gc for retained-result memory measurements");
  }
  const options = parseOptions();
  assert.equal(jkdbPackage.version, "1.4.0", "benchmark requires installed jkdb 1.4.0");
  const { QConnection } = jkdb;
  assert.equal(typeof QConnection, "function", "jkdb 1.4.0 did not expose QConnection");

  let kola;
  let comparison;
  let comparisonNanoseconds;
  let native;
  let report;
  try {
    kola = await Q.connect({ host: options.host, port: options.port });
    comparison = await jkdbConnect(QConnection, { host: options.host, port: options.port });
    comparisonNanoseconds = await jkdbConnect(QConnection, {
      host: options.host,
      port: options.port,
      includeNanosecond: true,
    });
    native = new NativeConnector({ host: options.host, port: options.port, timeoutSeconds: 30 });
    const nativeConnectResult = await native.connect();
    assert.equal(nativeConnectResult.ok, true, "raw native connection failed");

    const fixtureRows = await kola.sync(".kola.rows");
    const comparisonFixtureRows = await comparison.syncAsync(".kola.rows");
    assert.equal(fixtureRows, BigInt(options.rows), "Kola fixture row count mismatch");
    assert.equal(comparisonFixtureRows, fixtureRows, "clients reported different fixture row counts");
    const qVersion = await kola.sync(".z.K");
    assert.equal(await comparison.syncAsync(".z.K"), qVersion, "clients reported different q versions");
    const qSeed = await kola.sync(".kola.seed");
    assert.equal(qSeed, 42n, "q fixture seed mismatch");
    assert.equal(await comparison.syncAsync(".kola.seed"), qSeed, "clients reported different q seeds");

    const scalar = await runPairedSamples({
      ...options,
      operations: {
        kola: () => kola.sync("6f*7f"),
        jkdb: () => comparison.syncAsync("6f*7f"),
      },
      validate: validateScalar,
    });
    const bigInt = await runPairedSamples({
      ...options,
      operations: {
        kola: () => kola.sync(`${BIGINT_VALUE}j`),
        jkdb: () => comparison.syncAsync(`${BIGINT_VALUE}j`),
      },
      validate: validateBigInt,
    });
    const timestamp = await runPairedSamples({
      ...options,
      operations: {
        kola: () => kola.sync(TIMESTAMP_EXPRESSION),
        jkdb: () => comparisonNanoseconds.syncAsync(TIMESTAMP_EXPRESSION),
      },
      validate: validateTimestamp,
    });

    const publicTables = {};
    const nativeDiagnostics = {};
    const memory = {};
    const sampleOrder = {
      scalar: scalar.order,
      bigint: bigInt.order,
      timestamp: timestamp.order,
      tables: {},
    };

    let tableIndex = 0;
    for (const tableName of Object.keys(TABLES)) {
      const inputs = {
        kola: await kola.sync(tableName),
        jkdb: await comparison.syncAsync(tableName),
      };
      validateTable("kola", inputs.kola, tableName, options.rows);
      validateTable("jkdb", inputs.jkdb, tableName, options.rows);

      const fixtureEqualityLambda = `{[x]${tableName}~x}`;
      const preflightByClient = {};
      for (const client of CLIENTS) {
        const invoke = (lambda) =>
          client === "kola"
            ? kola.sync(lambda, inputs[client])
            : comparison.syncAsync([lambda, inputs[client]]);
        const fixtureValuesAndTypesEqual = await invoke(fixtureEqualityLambda);
        assert.equal(
          fixtureValuesAndTypesEqual,
          true,
          `${tableName}: ${client} prepared representation differs from the q fixture`,
        );
        const serializedSize = await invoke(CANONICAL_SERIALIZED_SIZE_LAMBDA);
        assert.equal(
          typeof serializedSize,
          "bigint",
          `${tableName}: ${client} canonical serialized size is not long`,
        );
        assert(
          serializedSize > 0n && serializedSize <= BigInt(Number.MAX_SAFE_INTEGER),
          `${tableName}: ${client} canonical serialized size is outside the safe positive range`,
        );
        preflightByClient[client] = {
          documentedRepresentation: PUBLIC_TABLE_REPRESENTATIONS[client],
          fixtureValuesAndTypesEqual,
          canonicalSerializedBytes: Number(serializedSize),
        };
      }
      assert.equal(
        preflightByClient.kola.canonicalSerializedBytes,
        preflightByClient.jkdb.canonicalSerializedBytes,
        `${tableName}: client canonical serialized sizes differ`,
      );
      const logicalPayloadBytes = preflightByClient.kola.canonicalSerializedBytes;

      const reads = await runPairedSamples({
        ...options,
        operations: {
          kola: () => kola.sync(tableName),
          jkdb: () => comparison.syncAsync(tableName),
        },
        validate: (client, value) => validateTable(client, value, tableName, options.rows),
      });
      const sends = await runPairedSamples({
        ...options,
        operations: {
          kola: () => kola.sync(TIMED_SEND_LAMBDA, inputs.kola),
          jkdb: () => comparison.syncAsync([TIMED_SEND_LAMBDA, inputs.jkdb]),
        },
        validate: (client, value) => validateCount(client, value, options.rows),
      });

      publicTables[tableName] = {
        expectedRows: options.rows,
        expectedColumns: TABLES[tableName],
        logicalQSerializedPayloadBytes: logicalPayloadBytes,
        qPreflight: {
          fixtureEqualityLambda,
          canonicalSerializedSizeLambda: CANONICAL_SERIALIZED_SIZE_LAMBDA,
          serializedSizeMatch: true,
          clients: preflightByClient,
        },
        read: {
          kola: {
            documentedRepresentation: PUBLIC_TABLE_REPRESENTATIONS.kola,
            representativePreparedResultFixtureValuesAndTypesProvenByQPreflight: true,
            ...metricReport(reads.samples.kola, logicalPayloadBytes),
          },
          jkdb: {
            documentedRepresentation: PUBLIC_TABLE_REPRESENTATIONS.jkdb,
            representativePreparedResultFixtureValuesAndTypesProvenByQPreflight: true,
            ...metricReport(reads.samples.jkdb, logicalPayloadBytes),
          },
        },
        qAcknowledgedCountSend: {
          lambda: TIMED_SEND_LAMBDA,
          kola: metricReport(sends.samples.kola, logicalPayloadBytes),
          jkdb: metricReport(sends.samples.jkdb, logicalPayloadBytes),
        },
      };
      sampleOrder.tables[tableName] = {
        read: reads.order,
        qAcknowledgedCountSend: sends.order,
      };

      for (let warmup = 0; warmup < options.warmups; warmup += 1) {
        validateRawTable(await native.sync(tableName, []), tableName, options.rows);
      }
      const rawSamples = [];
      let rawBytes;
      for (let sample = 0; sample < options.iterations; sample += 1) {
        const measured = await timeAsync(() => native.sync(tableName, []));
        rawBytes = validateRawTable(measured.value, tableName, options.rows);
        rawSamples.push(measured.durationNs);
      }
      assert(rawBytes !== undefined);

      for (let warmup = 0; warmup < options.warmups; warmup += 1) {
        validateKolaTable(tableFromIPC(rawBytes), tableName, options.rows);
      }
      const materializationSamples = [];
      for (let sample = 0; sample < options.iterations; sample += 1) {
        const measured = timeSync(() => tableFromIPC(rawBytes));
        validateKolaTable(measured.value, tableName, options.rows);
        materializationSamples.push(measured.durationNs);
      }
      nativeDiagnostics[tableName] = {
        nativeTransferToArrowIpcBytes: metricReport(rawSamples, logicalPayloadBytes),
        arrowMaterializationFromRetainedIpcBytes: durationReport(materializationSamples),
        retainedArrowIpcBytes: rawBytes.byteLength,
        retainedResultProcessMemoryDiagnostic: {
          nativeTransferToArrowIpcBytes: await retainedResultMemory(
            options.memoryResults,
            async () => rawTableBytes(await native.sync(tableName, []), tableName),
            (bytes) => assert(bytes instanceof Uint8Array && bytes.byteLength > 0),
          ),
          arrowMaterializationFromRetainedIpcBytes: await retainedResultMemory(
            options.memoryResults,
            async () => tableFromIPC(rawBytes),
            (value) => validateKolaTable(value, tableName, options.rows),
          ),
        },
      };

      const memoryOrder = clientOrder(tableIndex * 2, false);
      memory[tableName] = {
        measurementOrder: [...memoryOrder],
        clients: {},
      };
      for (const client of memoryOrder) {
        memory[tableName].clients[client] = await retainedResultMemory(
          options.memoryResults,
          client === "kola"
            ? () => kola.sync(tableName)
            : () => comparison.syncAsync(tableName),
          (value) => validateTable(client, value, tableName, options.rows),
        );
      }
      tableIndex += 1;
    }

    report = {
      schemaVersion: 2,
      subject: {
        kola: { package: kolaPackage.name, version: kolaPackage.version },
        jkdb: { package: jkdbPackage.name, version: jkdbPackage.version },
      },
      fixture: {
        host: options.host,
        port: options.port,
        qIdentity: {
          source: "queried from q",
          version: qVersion,
          seed: qSeed,
          rows: Number(fixtureRows),
        },
        tables: Object.keys(TABLES),
        configuredTableTimestampSpacingNanoseconds: 1_000_000,
      },
      runtime: {
        node: process.version,
        platform: process.platform,
        architecture: process.arch,
        cpu: cpus()[0]?.model ?? "unavailable",
        exposeGc: true,
      },
      method: {
        warmupsPerClientAndWorkload: options.warmups,
        measuredSamplesPerClientAndWorkload: options.iterations,
        retainedResultsPerMemoryDiagnostic: options.memoryResults,
        valueModes: {
          kola: "lossless bigint and bigint-backed nanosecond temporal wrappers",
          jkdbPrimary: { useBigInt: true, includeNanosecond: false },
          jkdbTimestampCorrectness: { useBigInt: true, includeNanosecond: true },
          tableTimestamps:
            "fixture values are millisecond-aligned and one millisecond apart, so jkdb's default Date representation is value-equivalent; untimed q match preflight proves the round trip",
        },
        publicTableRepresentations: PUBLIC_TABLE_REPRESENTATIONS,
        scheduling:
          "persistent connections; one request at a time; measured client order follows ABBA blocks, reversed on alternating workloads, and is balanced for every permitted even sample count",
        clock: "process.hrtime.bigint",
        durationUnit: "nanoseconds",
        p95: "nearest-rank on raw durations; payload rateAtDurationP95 inverts duration p95",
        median: "middle value, or arithmetic mean of the two middle values for an even sample count",
        logicalPayloadRate:
          "canonical q serialized table bytes per second, based on the matching count -8!x preflight result; this is logical payload rate, not observed wire throughput",
        retainedMemory:
          "noisy process-wide diagnostic only, not a client or library footprint claim; process.memoryUsage is sampled before and after retaining the configured result count, with forced GC at both snapshots, and allocator-wide deltas may be negative",
        diagnosticMemory:
          "Kola raw Arrow IPC buffers and Arrow materialization are retained and measured separately; jkdb exposes no equivalent pre-materialization result",
        validation:
          "every warmup and measured result is checked after its timed interval; before table timing, both prepared public representations must q-match the fixture by value and type and return the same canonical serialized size",
      },
      correctness: {
        scalar: { expression: "6f*7f", expected: 42, kola: "validated", jkdb: "validated" },
        bigint: {
          expression: `${BIGINT_VALUE}j`,
          expected: BIGINT_VALUE.toString(),
          kola: "validated as bigint",
          jkdb: "validated as bigint with useBigInt enabled",
        },
        nonMillisecondTimestamp: {
          expression: TIMESTAMP_EXPRESSION,
          expectedUnixNanoseconds: TIMESTAMP_UNIX_NANOSECONDS.toString(),
          expectedNanosecondText: TIMESTAMP_NANOSECOND_TEXT,
          kola: "validated exact nanoseconds as bigint",
          jkdb: "validated exact nanoseconds with includeNanosecond enabled",
        },
      },
      sampleOrder,
      publicEndToEnd: {
        scalar: {
          kola: metricReport(scalar.samples.kola),
          jkdb: metricReport(scalar.samples.jkdb),
        },
        bigint: {
          kola: metricReport(bigInt.samples.kola),
          jkdb: metricReport(bigInt.samples.jkdb),
        },
        timestamp: {
          kola: metricReport(timestamp.samples.kola),
          jkdb: metricReport(timestamp.samples.jkdb),
        },
        tables: publicTables,
      },
      diagnostics: {
        kola: nativeDiagnostics,
        jkdb: {
          nativeTransfer: "unavailable: jkdb 1.4.0 does not expose a pre-materialization result",
          internalDecode: "unavailable: jkdb 1.4.0 syncAsync returns only the fully decoded value",
        },
        retainedResultProcessMemory: {
          classification: "noisy process-wide diagnostic; not a client or library footprint claim",
          retainedResultsPerClientAndTable: options.memoryResults,
          tables: memory,
        },
      },
    };
  } finally {
    const cleanup = [];
    if (native !== undefined) cleanup.push(native.disconnect());
    if (comparison !== undefined) cleanup.push(comparison.closeAsync());
    if (comparisonNanoseconds !== undefined) cleanup.push(comparisonNanoseconds.closeAsync());
    if (kola !== undefined) cleanup.push(kola.disconnect());
    await Promise.allSettled(cleanup);
  }

  const json = `${JSON.stringify(
    report,
    (_key, value) => (typeof value === "bigint" ? value.toString() : value),
    2,
  )}\n`;
  if (options.output !== undefined) {
    const outputPath = resolve(options.output);
    await mkdir(dirname(outputPath), { recursive: true });
    await writeFile(outputPath, json, "utf8");
  }
  process.stdout.write(json);
}

await main();
