import { Table, tableFromArrays, tableToIPC } from "apache-arrow";
import { Buffer } from "node:buffer";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { NativeValue } from "../src/native-contract.js";

const loaderMock = vi.hoisted(() => ({
  loadNativeBinding: vi.fn(),
}));

vi.mock("../src/native-loader.js", () => ({
  loadNativeBinding: loaderMock.loadNativeBinding,
}));

import { readBinary6, serializeAsIpcBytes6 } from "../src/helpers.js";

beforeEach(() => {
  loaderMock.loadNativeBinding.mockReset();
});

describe("binary helpers", () => {
  it("materializes readBinary6 output as an Arrow Table", async () => {
    const table = tableFromArrays({ size: BigInt64Array.of(10n, 20n) });
    loaderMock.loadNativeBinding.mockResolvedValue({
      NativeConnector: class NativeConnector {},
      readBinary6: async () => ({
        ok: true,
        value: { tag: "table", bytesValue: tableToIPC(table, "stream") },
      }),
      serializeAsIpcBytes6: async () => ({ ok: true }),
    });

    const decoded = await readBinary6("trade.bin");

    expect(decoded).toBeInstanceOf(Table);
    expect([...decoded.getChild("size")!]).toEqual([10n, 20n]);
  });

  it("normalizes helper input and returns exact serialized bytes as a Buffer", async () => {
    let capturedValue: NativeValue | undefined;
    const expected = Uint8Array.of(1, 0, 0, 0, 255);
    loaderMock.loadNativeBinding.mockResolvedValue({
      NativeConnector: class NativeConnector {},
      readBinary6: async () => ({ ok: true }),
      serializeAsIpcBytes6: async (
        _messageType: unknown,
        _compress: unknown,
        value: NativeValue,
      ) => {
        capturedValue = value;
        return { ok: true, value: { tag: "bytes", bytesValue: expected } };
      },
    });

    const bytes = await serializeAsIpcBytes6("sync", true, 9_007_199_254_740_993n);

    expect(capturedValue).toEqual({ tag: "i64", bigintValue: 9_007_199_254_740_993n });
    expect(Buffer.isBuffer(bytes)).toBe(true);
    expect(bytes).toEqual(Buffer.from(expected));
  });
});
