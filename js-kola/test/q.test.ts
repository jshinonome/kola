import { beforeEach, describe, expect, it, vi } from "vitest";

import type {
  NativeConnector,
  NativeModule,
  NativeOptions,
  NativeResult,
  NativeValue,
} from "../src/native-contract.js";

const loaderMock = vi.hoisted(() => ({
  loadNativeBinding: vi.fn<() => Promise<NativeModule>>(),
}));

vi.mock("../src/native-loader.js", () => ({
  loadNativeBinding: loaderMock.loadNativeBinding,
}));

import { KolaIOError } from "../src/errors.js";
import { Q } from "../src/q.js";

let callOrder: string[];
let connectorOptions: NativeOptions[];
let connectResults: NativeResult[];
let syncResultPromise: Promise<NativeResult> | undefined;
let lastSync: { readonly expression: string; readonly args: NativeValue[] } | undefined;

class MockNativeConnector implements NativeConnector {
  public constructor(options: NativeOptions) {
    connectorOptions.push(options);
  }

  public connect(): Promise<NativeResult> {
    callOrder.push("connect");
    return Promise.resolve(connectResults.shift() ?? { ok: true });
  }

  public disconnect(): Promise<NativeResult> {
    callOrder.push("disconnect");
    return Promise.resolve({ ok: true });
  }

  public sync(expression: string, args: NativeValue[]): Promise<NativeResult> {
    callOrder.push("sync");
    lastSync = { expression, args };
    return syncResultPromise ?? Promise.resolve({
      ok: true,
      value: { tag: "i64", bigintValue: 42n },
    });
  }

  public asyn(_expression: string, _args: NativeValue[]): Promise<NativeResult> {
    callOrder.push("asyn");
    return Promise.resolve({ ok: true });
  }

  public receive(): Promise<NativeResult> {
    callOrder.push("receive");
    return Promise.resolve({ ok: true, value: { tag: "symbol", stringValue: "update" } });
  }
}

beforeEach(() => {
  callOrder = [];
  connectorOptions = [];
  connectResults = [];
  syncResultPromise = undefined;
  lastSync = undefined;
  loaderMock.loadNativeBinding.mockReset();
  loaderMock.loadNativeBinding.mockResolvedValue({
    NativeConnector: MockNativeConnector,
    readBinary6: async () => ({ ok: true }),
    serializeAsIpcBytes6: async () => ({ ok: true }),
  });
});

describe("Q native delegation", () => {
  it("rejects timeouts above the native 24-hour ceiling", () => {
    expect(
      () => new Q({ host: "localhost", port: 1800, timeout: 86_400_001 }),
    ).toThrowError(RangeError);
  });

  it("delegates calls in invocation order without waiting for an earlier response", async () => {
    let resolveSync: ((result: NativeResult) => void) | undefined;
    syncResultPromise = new Promise((resolve) => {
      resolveSync = resolve;
    });
    const q = new Q({ host: "127.0.0.1", port: 1800 });

    const syncPromise = q.sync("first", 9_007_199_254_740_993n);
    const asynPromise = q.asyn("second", "trade");
    const receivePromise = q.receive();

    await vi.waitFor(() => {
      expect(callOrder).toEqual(["sync", "asyn", "receive"]);
    });
    await expect(asynPromise).resolves.toBeUndefined();
    await expect(receivePromise).resolves.toBe("update");
    expect(lastSync).toEqual({
      expression: "first",
      args: [{ tag: "i64", bigintValue: 9_007_199_254_740_993n }],
    });

    if (resolveSync === undefined) {
      throw new Error("The native sync operation was not invoked");
    }
    resolveSync({ ok: true, value: { tag: "i64", bigintValue: 42n } });
    await expect(syncPromise).resolves.toBe(42n);
  });

  it("supports static connect, idempotent disconnect, and reconnect", async () => {
    const q = await Q.connect({ host: "localhost", port: 1800 });
    await q.disconnect();
    await q.disconnect();
    await q.connect();

    expect(callOrder).toEqual(["connect", "disconnect", "disconnect", "connect"]);
    expect(connectorOptions).toEqual([
      { host: "localhost", port: 1800, timeoutSeconds: 30 },
    ]);
  });

  it("does not load the addon when disconnecting an unused instance", async () => {
    const q = new Q({ host: "localhost", port: 1800 });
    await q.disconnect();

    expect(loaderMock.loadNativeBinding).not.toHaveBeenCalled();
  });

  it("normalizes public milliseconds and retries failed IO connects", async () => {
    connectResults = [
      { ok: false, error: { code: "KOLA_IO", message: "refused" } },
      { ok: true },
    ];
    const q = new Q({
      host: "q.internal",
      port: 5001,
      user: "api",
      password: "secret",
      tls: true,
      timeout: 1_001,
      retries: 1,
    });

    await q.connect();

    expect(callOrder).toEqual(["connect", "connect"]);
    expect(connectorOptions).toEqual([
      {
        host: "q.internal",
        port: 5001,
        user: "api",
        password: "secret",
        tls: true,
        timeoutSeconds: 2,
      },
    ]);
  });

  it("does not retry authentication failures", async () => {
    connectResults = [
      { ok: false, error: { code: "KOLA_AUTH", message: "access denied" } },
      { ok: true },
    ];
    const q = new Q({ host: "localhost", port: 1800, retries: 2 });

    await expect(q.connect()).rejects.toMatchObject({
      name: "KolaAuthError",
      code: "KOLA_AUTH",
    });
    expect(callOrder).toEqual(["connect"]);
  });
  it("does not retry a permanent native-loader failure", async () => {
    loaderMock.loadNativeBinding.mockRejectedValue(
      new KolaIOError("KOLA_NATIVE_LOAD", "missing addon"),
    );
    const q = new Q({ host: "localhost", port: 1800, retries: 5 });

    await expect(q.connect()).rejects.toMatchObject({ code: "KOLA_NATIVE_LOAD" });
    expect(loaderMock.loadNativeBinding).toHaveBeenCalledTimes(1);
  });


  it("preserves rejected native calls as IO errors", async () => {
    syncResultPromise = Promise.reject(new Error("worker stopped"));
    const q = new Q({ host: "localhost", port: 1800 });

    await expect(q.sync("1+1")).rejects.toBeInstanceOf(KolaIOError);
  });
});
