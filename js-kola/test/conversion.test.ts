import { Table, Vector, tableFromArrays, tableToIPC } from "apache-arrow";
import { Buffer } from "node:buffer";
import { describe, expect, it } from "vitest";

import {
  ensureNativeSuccess,
  normalizeInput,
  normalizeOutput,
} from "../src/conversion.js";
import { KolaError } from "../src/errors.js";
import type { NativeValue } from "../src/native-contract.js";
import {
  KolaDate,
  KolaQLambda,
  KolaQOperator,
  KolaTime,
  KolaTimespan,
  KolaTimestamp,
} from "../src/types.js";

// @ts-expect-error Private fields keep KolaQOperator nominal.
const structurallyForgedOperator: KolaQOperator = { name: "+" };
void structurallyForgedOperator;
// @ts-expect-error Private fields keep KolaQLambda nominal.
const structurallyForgedLambda: KolaQLambda = { source: "{x}", context: "" };
void structurallyForgedLambda;

describe("public value conversion", () => {
  it("normalizes scalar, list, dictionary, bigint, and arbitrary byte inputs", () => {
    const bytes = Uint8Array.of(0, 128, 255);

    expect(normalizeInput("trade")).toEqual({ tag: "symbol", stringValue: "trade" });
    expect(normalizeInput(9_007_199_254_740_993n)).toEqual({
      tag: "i64",
      bigintValue: 9_007_199_254_740_993n,
    });
    const normalizedBytes = normalizeInput(bytes);
    expect(normalizedBytes.tag).toBe("bytes");
    expect(Buffer.isBuffer(normalizedBytes.bytesValue)).toBe(true);
    expect(normalizedBytes.bytesValue).toEqual(Buffer.from(bytes));
    expect(normalizeInput([true, 1n, "x"])).toEqual({
      tag: "list",
      items: [
        { tag: "boolean", boolValue: true },
        { tag: "i64", bigintValue: 1n },
        { tag: "symbol", stringValue: "x" },
      ],
    });
    expect(normalizeInput({ sym: "AAPL", size: 10n })).toEqual({
      tag: "dictionary",
      entries: [
        { key: "sym", value: { tag: "symbol", stringValue: "AAPL" } },
        { key: "size", value: { tag: "i64", bigintValue: 10n } },
      ],
    });
  });

  it("round-trips precise temporal wrapper payloads", () => {
    const values = [
      new KolaTimestamp(1_725_000_000_000_000_001n),
      new KolaTimestamp(-1n),
      new KolaDate("2026-08-22"),
      new KolaTime(43_200_000_000_000n),
      new KolaTimespan(-1n),
    ] as const;

    for (const value of values) {
      expect(normalizeOutput(normalizeInput(value))).toEqual(value);
    }
  });

  it("round-trips q operator and lambda wrapper contracts", () => {
    expect(KolaQOperator.PLUS.name).toBe("+");
    for (const name of ["+:", "setenv", "'", "/", "\\"]) {
      expect(new KolaQOperator(name).name).toBe(name);
    }
    expect(normalizeInput(KolaQOperator.PLUS)).toEqual({
      tag: "operator",
      stringValue: "+",
    });

    const root = new KolaQLambda("{x+y}");
    const contextual = new KolaQLambda(" {x+y} ", "ctx");
    const kDialect = new KolaQLambda(" k){x+y} ");
    expect(normalizeInput(root)).toEqual({
      tag: "lambda",
      stringValue: "{x+y}",
      context: "",
    });
    expect(normalizeInput(contextual)).toEqual({
      tag: "lambda",
      stringValue: " {x+y} ",
      context: "ctx",
    });
    expect(normalizeInput(kDialect)).toEqual({
      tag: "lambda",
      stringValue: " k){x+y} ",
      context: "",
    });
    expect(normalizeOutput({ tag: "operator", stringValue: "+" })).toEqual(
      KolaQOperator.PLUS,
    );
    expect(
      normalizeOutput({
        tag: "lambda",
        stringValue: " {x+y} ",
        context: "ctx",
      }),
    ).toEqual(contextual);
  });

  it("rejects invalid constructors and malformed native function envelopes", () => {
    for (const construct of [
      () => new KolaQOperator("plus"),
      () => new KolaQOperator("+\0"),
      () => new KolaQOperator(1 as never),
      () => new KolaQLambda("x+y"),
      () => new KolaQLambda("k)x+y"),
      () => new KolaQLambda("{x\0+y}"),
      () => new KolaQLambda("{x+y}", "bad\0context"),
      () => new KolaQLambda("{x+y}", ".ctx"),
      () => new KolaQLambda(1 as never),
      () => new KolaQLambda("{x+y}", 1 as never),
    ]) {
      expect(construct).toThrowError(expect.objectContaining({ code: "KOLA_CONVERSION" }));
    }

    const invalidEnvelopes: NativeValue[] = [
      { tag: "operator", stringValue: "plus" },
      { tag: "lambda", stringValue: "x+y", context: "" },
      { tag: "lambda", stringValue: "{x+y}", context: "bad\0context" },
      { tag: "lambda", stringValue: "{x+y}", context: ".ctx" },
      { tag: "lambda", stringValue: "{x+y}" },
    ];
    for (const envelope of invalidEnvelopes) {
      expect(() => normalizeOutput(envelope)).toThrowError(
        expect.objectContaining({ code: "KOLA_CONVERSION" }),
      );
    }
  });

  it("freezes callable values and defensively rejects forged instances", () => {
    expect(KolaQOperator.PLUS).toBe(KolaQOperator.PLUS);
    expect(Object.isFrozen(KolaQOperator.PLUS)).toBe(true);
    const lambda = new KolaQLambda("{x+y}");
    expect(Object.isFrozen(lambda)).toBe(true);
    expect(Object.getOwnPropertyDescriptor(KolaQOperator, "PLUS")).toMatchObject({
      get: expect.any(Function),
      set: undefined,
    });
    expect(Reflect.set(KolaQOperator, "PLUS", new KolaQOperator("-"))).toBe(false);
    expect(() =>
      Object.defineProperty(KolaQOperator.PLUS, "name", { value: "-" }),
    ).toThrow(TypeError);
    expect(() =>
      Object.defineProperty(lambda, "context", { value: "ctx" }),
    ).toThrow(TypeError);

    const forgedOperator = Object.create(KolaQOperator.prototype) as KolaQOperator;
    const forgedLambda = Object.create(KolaQLambda.prototype) as KolaQLambda;
    for (const value of [forgedOperator, forgedLambda]) {
      expect(() => normalizeInput(value)).toThrowError(
        expect.objectContaining({ code: "KOLA_CONVERSION" }),
      );
    }
  });

  it("rejects invalid temporal wrappers as stable conversion errors", () => {
    expect(() => normalizeInput(new KolaDate("22 August 2026"))).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
    expect(() => normalizeInput(new KolaTime(86_400_000_000_000n))).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
    expect(() => normalizeInput(new KolaTime(1n))).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
  });
  it("rejects unsupported runtime inputs as stable conversion errors", () => {
    expect(() => normalizeInput(undefined as never)).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
    expect(() => normalizeInput(Symbol("trade") as never)).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
  });

  it("rejects embedded NUL symbols and dictionary keys recursively", () => {
    expect(() => normalizeInput({ outer: ["bad\0symbol"] })).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
    expect(() => normalizeInput({ outer: [{ "bad\0key": 1n }] })).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
  });

  it("rejects over-depth and cyclic arrays and dictionaries", () => {
    let deepArray: unknown = null;
    let deepDictionary: unknown = null;
    for (let depth = 0; depth < 65; depth += 1) {
      deepArray = [deepArray];
      deepDictionary = { value: deepDictionary };
    }
    expect(() => normalizeInput(deepArray as never)).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
    expect(() => normalizeInput(deepDictionary as never)).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );

    const cyclicArray: unknown[] = [];
    cyclicArray.push(cyclicArray);
    const cyclicDictionary: Record<string, unknown> = {};
    cyclicDictionary.self = cyclicDictionary;
    expect(() => normalizeInput(cyclicArray as never)).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
    expect(() => normalizeInput(cyclicDictionary as never)).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
  });


  it("keeps tables and vectors columnar through Arrow IPC", () => {
    const table = tableFromArrays({ price: Float64Array.of(10.5, 11.25) });
    const tableInput = normalizeInput(table);
    expect(tableInput.tag).toBe("table");
    expect(Buffer.isBuffer(tableInput.bytesValue)).toBe(true);

    const decodedTable = normalizeOutput({
      tag: "table",
      bytesValue: tableInput.bytesValue,
    });
    expect(decodedTable).toBeInstanceOf(Table);
    if (!(decodedTable instanceof Table)) {
      throw new Error("Expected an Arrow Table");
    }
    expect([...decodedTable.getChild("price")!]).toEqual([10.5, 11.25]);

    const vector = table.getChild("price")!;
    const vectorInput = normalizeInput(vector);
    expect(vectorInput.tag).toBe("series");
    expect(normalizeOutput(vectorInput)).toBeInstanceOf(Vector);
  });

  it("decodes bigint, bytes, dictionaries, and one-column series without row materialization", () => {
    expect(normalizeOutput({ tag: "i64", bigintValue: 9_007_199_254_740_993n })).toBe(
      9_007_199_254_740_993n,
    );

    const source = Uint8Array.of(0, 128, 255);
    const output = normalizeOutput({ tag: "bytes", bytesValue: source });
    expect(Buffer.isBuffer(output)).toBe(true);
    expect(output).toEqual(Buffer.from(source));

    const dictionary = normalizeOutput({
      tag: "dictionary",
      entries: [
        { key: "__proto__", value: { tag: "symbol", stringValue: "safe" } },
      ],
    });
    expect(Object.getOwnPropertyDescriptor(dictionary, "__proto__")?.value).toBe("safe");

    const vector = tableFromArrays({ value: Int32Array.of(1, 2, 3) }).getChild("value")!;
    const ipc = tableToIPC(new Table({ value: vector }), "stream");
    const decoded = normalizeOutput({ tag: "series", bytesValue: ipc });
    expect(decoded).toBeInstanceOf(Vector);
    if (!(decoded instanceof Vector)) {
      throw new Error("Expected an Arrow Vector");
    }
    expect([...decoded]).toEqual([1, 2, 3]);
  });

  it("rejects malformed native envelopes without hiding the error", () => {
    expect(() => normalizeOutput({ tag: "i64" })).toThrowError(
      expect.objectContaining({ code: "KOLA_CONVERSION" }),
    );
    expect(() => ensureNativeSuccess({ ok: false })).toThrowError(KolaError);
  });
});
