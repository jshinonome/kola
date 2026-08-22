# kola-q

`kola-q` is the ESM Node.js binding for kola's q IPC client. It exposes a strict TypeScript facade, keeps q tables and typed vectors columnar with Apache Arrow, and runs connection work on a dedicated native worker instead of the JavaScript event loop.

## Install

```sh
npm install kola-q
```

Node.js 20 or newer is required. The native addon uses N-API 6. npm must be allowed to install optional dependencies because the supported native binary is distributed in a platform package.

For a source checkout, install the JavaScript dependencies and build both layers:

```sh
cd js-kola
npm install
npm run build
```

`npm run build:native` calls the napi-rs v3 CLI with `bindings/napi-kola/Cargo.toml` and generates the internal `native.js` loader, `native.d.ts`, and local `.node` artifact in this package. The generated loader checks the local artifact during development and the matching optional platform package after installation. The public entry point does not export the generated native declarations.

## Connect and query

```ts
import { Q } from "kola-q";

const q = await Q.connect({
  host: "localhost",
  port: 1800,
  user: "user",
  password: "password",
  timeout: 30_000,
  retries: 2,
});

try {
  const result = await q.sync("select from trade");
  await q.asyn("insert", "trade", ["AAPL", 10n]);
  console.log(result);
} finally {
  await q.disconnect();
}
```

`connect()`, `disconnect()`, `sync()`, `asyn()`, and `receive()` all return promises. `sync()` is a synchronous q IPC request/response transaction, not a synchronous JavaScript function. Calls made on one `Q` are admitted to a bounded, eight-command native FIFO in call order so complete IPC transactions cannot interleave. Admission never waits on the JavaScript thread: when all eight pending slots are occupied, the new call fails with `KOLA_BACKPRESSURE`.

The default socket timeout is 30,000 milliseconds. `timeout` must be positive and cannot exceed 24 hours (86,400,000 milliseconds); the native layer rounds it up to the next whole second. The finite timeout lets a blocked receive eventually release the connection and its worker. `retries` is the number of additional attempts made by an explicit `connect()` after an IO failure; authentication failures are not retried. Query methods can establish the native connection automatically when needed.

Each submitted argument set is limited to 64 MiB after native snapshot accounting, and encoded or decoded IPC messages are limited to 512 MiB. These bounds prevent one connection or hostile peer from forcing process-scale allocations; split larger workloads into smaller requests.

`disconnect()` is idempotent. A `Q` can reconnect after disconnect:

```ts
await q.disconnect();
await q.connect();
const value = await q.sync("42");
```

Always call `disconnect()` in `finally` or an equivalent cleanup hook instead of relying on garbage collection.

## Value mapping

### JavaScript to q

| JavaScript input | q value |
| --- | --- |
| `null` | generic null |
| `boolean` | boolean |
| `number` | float |
| `bigint` | long |
| `string` | symbol; embedded NUL is rejected |
| `Buffer` or `Uint8Array` | char vector, preserving arbitrary bytes |
| ordinary array | mixed list |
| plain string-keyed object | dictionary; keys cannot contain NUL |
| Apache Arrow `Vector` | typed series/list through Arrow IPC |
| Apache Arrow `Table` | table through Arrow IPC |
| `KolaTimestamp` | timestamp as Unix-epoch nanoseconds |
| `KolaDate` | date in `YYYY-MM-DD` form |
| `KolaTime` | millisecond-aligned nanoseconds since midnight |
| `KolaTimespan` | signed nanosecond duration |

### q to JavaScript

| q value | JavaScript output |
| --- | --- |
| boolean and safe-width numeric atoms | `boolean` or `number` |
| long | `bigint` |
| symbol, string, or GUID | `string` |
| char atom | byte value as `number` |
| char vector | `Buffer` |
| timestamp | `KolaTimestamp` with a `bigint` nanosecond payload |
| date | `KolaDate` |
| time | `KolaTime` with a `bigint` nanosecond payload |
| timespan | `KolaTimespan` with a `bigint` nanosecond payload |
| typed list | Apache Arrow `Vector` |
| mixed list | array |
| dictionary | plain string-keyed object |
| table | Apache Arrow `Table` |

The temporal wrappers prevent nanosecond values from being rounded through JavaScript `number` or `Date`:

```ts
import { KolaTime, KolaTimespan, KolaTimestamp } from "kola-q";

const timestamp = new KolaTimestamp(1_725_000_000_000_000_001n);
const noon = new KolaTime(43_200_000_000_000n);
const oneNanosecondAgo = new KolaTimespan(-1n);
```

Tables and typed lists cross the native boundary as Arrow IPC streams and are materialized as Arrow `Table` and `Vector` objects. They are not expanded into row objects. The transfer is columnar, but this package does not claim zero-copy transfer across the N-API boundary.

Top-level `Buffer` and `Uint8Array` values remain lossless for arbitrary bytes. q char data inside Arrow table or nested columns must be valid UTF-8; invalid bytes return a conversion error instead of being replaced or panicking. Because a q char atom is one byte while each Arrow string cell must be valid UTF-8, direct char-atom columns are limited to ASCII. Use a top-level byte value when arbitrary-byte round trips are required.

## Binary helpers

Both helpers are asynchronous because their native parsing and serialization work runs away from the JavaScript event loop:

```ts
import { readBinary6, serializeAsIpcBytes6 } from "kola-q";

const table = await readBinary6("trade.bin");
const message = await serializeAsIpcBytes6("sync", true, table);
```

`readBinary6()` resolves to an Arrow `Table`. `serializeAsIpcBytes6()` resolves to a Node.js `Buffer` containing one q IPC message.

`readBinary6()` accepts regular local files up to 512 MiB and rejects Windows UNC/device paths.

## Subscriptions

A pending `receive()` occupies its q connection until a message arrives or the socket timeout expires. A receive timeout fails with `KOLA_IO`, and the core closes that socket; callers must reconnect and resubscribe before receiving again. Use a dedicated `Q` for subscriptions and another for ordinary queries:

```ts
import { KolaIOError, Q } from "kola-q";

const queries = await Q.connect(options);
const subscription = await Q.connect(options);

try {
  await subscription.asyn(".u.sub", "trade", "");
  for (;;) {
    try {
      const update = await subscription.receive();
      consume(update);
    } catch (error) {
      if (!(error instanceof KolaIOError) || error.code !== "KOLA_IO") {
        throw error;
      }
      await subscription.connect();
      await subscription.asyn(".u.sub", "trade", "");
    }
  }
} finally {
  await Promise.allSettled([subscription.disconnect(), queries.disconnect()]);
}
```

Alternatively, set `timeout` above the maximum expected quiet interval, up to the 24-hour limit. The first release intentionally has no connection pool or separate subscription engine.

## Errors

Native failures become stable public errors:

- `KolaIOError` for `KOLA_IO` transport and connection failures
- `KolaAuthError` for `KOLA_AUTH` authentication failures
- `KolaError` with code `KOLA_BACKPRESSURE` when a connection's native FIFO is full
- `KolaError` for server, conversion, unsupported-value, internal, and other generic native failures

Every error has a stable `code`, its original native text in `nativeMessage`, and the native payload or rejected exception in `cause`. Failure to resolve a local or installed addon is a `KolaIOError` with code `KOLA_NATIVE_LOAD` and remediation in its message.

## Supported native targets

| Platform | Native package | Requirement |
| --- | --- | --- |
| Windows x64 | `kola-q-win32-x64-msvc` | Microsoft x64 ABI |
| Linux x64 | `kola-q-linux-x64-gnu` | glibc 2.28 or newer |
| macOS arm64 | `kola-q-darwin-arm64` | Apple Silicon |

Other operating-system, CPU, and libc combinations are unsupported in the initial release. A generated napi-rs loader reports an unsupported target or a missing optional binary instead of silently falling back to a different build.

## TLS certificate behavior

**`tls: true` encrypts the connection but currently accepts certificates without normal certificate verification. It does not authenticate the server identity and is vulnerable to an active man-in-the-middle.** Do not describe it as certificate-verified TLS or rely on it for server authentication. Use it only where this limitation is acceptable, such as behind a separately authenticated tunnel, until the native connector implements certificate verification.
