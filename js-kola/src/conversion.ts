import { Table, Vector, tableFromIPC, tableToIPC } from "apache-arrow";
import { Buffer } from "node:buffer";

import { conversionError, mapNativeError } from "./errors.js";
import type { NativeEntry, NativeResult, NativeValue } from "./native-contract.js";
import {
  KolaDate,
  KolaQLambda,
  KolaQOperator,
  KolaTime,
  KolaTimespan,
  KolaTimestamp,
  validatedQLambdaParts,
  validatedQOperatorName,
  type KolaInput,
  type KolaValue,
} from "./types.js";

const MAX_VALUE_DEPTH = 64;
const NANOSECONDS_PER_MILLISECOND = 1_000_000n;
const NANOSECONDS_PER_DAY = 86_400_000_000_000n;
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/u;

function isInputList(value: KolaInput): value is readonly KolaInput[] {
  return Array.isArray(value);
}

function arrowBytes(value: Table | Vector): Buffer {
  try {
    const table = value instanceof Table ? value : new Table({ value });
    const bytes = tableToIPC(table, "stream");
    return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  } catch (cause) {
    throw conversionError("Unable to serialize the Arrow value as an IPC stream", cause);
  }
}

function normalizeDictionary(
  value: Readonly<Record<string, KolaInput>>,
  depth: number,
  active: WeakSet<object>,
): NativeValue {
  const entries: NativeEntry[] = Object.entries(value).map(([key, entryValue]) => {
    if (key.includes("\0")) {
      throw conversionError("q dictionary keys cannot contain NUL bytes");
    }
    return {
      key,
      value: normalizeInputAtDepth(entryValue, depth + 1, active),
    };
  });
  return { tag: "dictionary", entries };
}

function normalizeInputAtDepth(
  value: KolaInput,
  depth: number,
  active: WeakSet<object>,
): NativeValue {
  if (depth > MAX_VALUE_DEPTH) {
    throw conversionError(`Input nesting exceeds ${MAX_VALUE_DEPTH} levels`);
  }
  if (value === null) {
    return { tag: "null" };
  }
  if (typeof value === "boolean") {
    return { tag: "boolean", boolValue: value };
  }
  if (typeof value === "number") {
    return { tag: "f64", numberValue: value };
  }
  if (typeof value === "bigint") {
    return { tag: "i64", bigintValue: value };
  }
  if (typeof value === "string") {
    if (value.includes("\0")) {
      throw conversionError("q symbols cannot contain NUL bytes");
    }
    return { tag: "symbol", stringValue: value };
  }
  if (value instanceof Uint8Array) {
    return {
      tag: "bytes",
      bytesValue: Buffer.from(value.buffer, value.byteOffset, value.byteLength),
    };
  }
  if (value instanceof KolaQOperator) {
    return { tag: "operator", stringValue: validatedQOperatorName(value) };
  }
  if (value instanceof KolaQLambda) {
    const { source, context } = validatedQLambdaParts(value);
    return {
      tag: "lambda",
      stringValue: source,
      context,
    };
  }
  if (value instanceof KolaTimestamp) {
    if (typeof value.nanoseconds !== "bigint") {
      throw conversionError("KolaTimestamp.nanoseconds must be a bigint");
    }
    return { tag: "timestamp", bigintValue: value.nanoseconds };
  }
  if (value instanceof KolaDate) {
    if (typeof value.value !== "string" || !ISO_DATE.test(value.value)) {
      throw conversionError("KolaDate.value must use YYYY-MM-DD form");
    }
    return { tag: "date", stringValue: value.value };
  }
  if (value instanceof KolaTime) {
    if (typeof value.nanoseconds !== "bigint") {
      throw conversionError("KolaTime.nanoseconds must be a bigint");
    }
    if (value.nanoseconds < 0n || value.nanoseconds >= NANOSECONDS_PER_DAY) {
      throw conversionError("KolaTime.nanoseconds must be within one day");
    }
    if (value.nanoseconds % NANOSECONDS_PER_MILLISECOND !== 0n) {
      throw conversionError("KolaTime.nanoseconds must use millisecond precision");
    }
    return { tag: "time", bigintValue: value.nanoseconds };
  }
  if (value instanceof KolaTimespan) {
    if (typeof value.nanoseconds !== "bigint") {
      throw conversionError("KolaTimespan.nanoseconds must be a bigint");
    }
    return { tag: "timespan", bigintValue: value.nanoseconds };
  }
  if (value instanceof Table || value instanceof Vector) {
    return {
      tag: value instanceof Table ? "table" : "series",
      bytesValue: arrowBytes(value),
    };
  }
  if (isInputList(value)) {
    if (active.has(value)) {
      throw conversionError("Cyclic arrays and dictionaries cannot map to q values");
    }
    active.add(value);
    try {
      return {
        tag: "list",
        items: value.map((item) => normalizeInputAtDepth(item, depth + 1, active)),
      };
    } finally {
      active.delete(value);
    }
  }
  if (typeof value !== "object") {
    throw conversionError(`Unsupported JavaScript input type: ${typeof value}`);
  }

  const prototype: object | null = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw conversionError("Only plain string-keyed objects can map to q dictionaries");
  }
  if (active.has(value)) {
    throw conversionError("Cyclic arrays and dictionaries cannot map to q values");
  }
  active.add(value);
  try {
    return normalizeDictionary(value, depth, active);
  } finally {
    active.delete(value);
  }
}

export function normalizeInput(value: KolaInput): NativeValue {
  return normalizeInputAtDepth(value, 0, new WeakSet());
}

function requiredBoolean(value: NativeValue): boolean {
  if (typeof value.boolValue !== "boolean") {
    throw conversionError(`Native ${value.tag} value omitted boolValue`);
  }
  return value.boolValue;
}

function requiredNumber(value: NativeValue): number {
  if (typeof value.numberValue !== "number") {
    throw conversionError(`Native ${value.tag} value omitted numberValue`);
  }
  return value.numberValue;
}

function requiredBigInt(value: NativeValue): bigint {
  if (typeof value.bigintValue !== "bigint") {
    throw conversionError(`Native ${value.tag} value omitted bigintValue`);
  }
  return value.bigintValue;
}

function requiredString(value: NativeValue): string {
  if (typeof value.stringValue !== "string") {
    throw conversionError(`Native ${value.tag} value omitted stringValue`);
  }
  return value.stringValue;
}

function requiredContext(value: NativeValue): string {
  if (typeof value.context !== "string") {
    throw conversionError(`Native ${value.tag} value omitted context`);
  }
  return value.context;
}

function requiredBytes(value: NativeValue): Uint8Array {
  if (!(value.bytesValue instanceof Uint8Array)) {
    throw conversionError(`Native ${value.tag} value omitted bytesValue`);
  }
  return value.bytesValue;
}

function decodeArrowTable(value: NativeValue): Table {
  try {
    return tableFromIPC(requiredBytes(value));
  } catch (cause) {
    throw conversionError(`Unable to decode native ${value.tag} IPC stream`, cause);
  }
}

function decodeDictionary(value: NativeValue): Readonly<Record<string, KolaValue>> {
  if (!Array.isArray(value.entries)) {
    throw conversionError("Native dictionary value omitted entries");
  }
  const output: Record<string, KolaValue> = {};
  for (const entry of value.entries) {
    Object.defineProperty(output, entry.key, {
      configurable: true,
      enumerable: true,
      value: normalizeOutput(entry.value),
      writable: true,
    });
  }
  return output;
}

export function normalizeOutput(value: NativeValue): KolaValue {
  switch (value.tag) {
    case "null":
      return null;
    case "boolean":
      return requiredBoolean(value);
    case "u8":
    case "i16":
    case "i32":
    case "f32":
    case "f64":
    case "char":
      return requiredNumber(value);
    case "i64":
      return requiredBigInt(value);
    case "guid":
    case "symbol":
    case "string":
      return requiredString(value);
    case "operator":
      return new KolaQOperator(requiredString(value));
    case "lambda":
      return new KolaQLambda(requiredString(value), requiredContext(value));
    case "bytes": {
      const bytes = requiredBytes(value);
      return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    }
    case "timestamp":
      return new KolaTimestamp(requiredBigInt(value));
    case "date":
      return new KolaDate(requiredString(value));
    case "time":
      return new KolaTime(requiredBigInt(value));
    case "timespan":
      return new KolaTimespan(requiredBigInt(value));
    case "list":
      if (!Array.isArray(value.items)) {
        throw conversionError("Native list value omitted items");
      }
      return value.items.map((item) => normalizeOutput(item));
    case "dictionary":
      return decodeDictionary(value);
    case "table":
      return decodeArrowTable(value);
    case "series": {
      const vector = decodeArrowTable(value).getChildAt(0);
      if (vector === null) {
        throw conversionError("Native series IPC stream contained no column");
      }
      return vector;
    }
    default:
      throw conversionError(`Unsupported native value tag: ${value.tag}`);
  }
}

export function ensureNativeSuccess(result: NativeResult): void {
  if (result.ok) {
    return;
  }
  if (result.error === undefined) {
    throw conversionError("Native operation failed without an error payload");
  }
  throw mapNativeError(result.error);
}

export function unwrapNativeValue(result: NativeResult): KolaValue {
  ensureNativeSuccess(result);
  if (result.value === undefined) {
    throw conversionError("Native operation succeeded without a value payload");
  }
  return normalizeOutput(result.value);
}
