import type { Table, Vector } from "apache-arrow";
import type { Buffer } from "node:buffer";
import { conversionError } from "./errors.js";

export interface QOptions {
  readonly host: string;
  readonly port: number;
  readonly user?: string;
  readonly password?: string;
  readonly tls?: boolean;
  /** Positive socket timeout in milliseconds, rounded up to seconds; maximum 86,400,000 (24h). */
  readonly timeout?: number;
  /** Additional connection attempts after the first failed IO attempt. */
  readonly retries?: number;
}

export type KolaMessageType = "async" | "sync" | "response";

export class KolaTimestamp {
  public readonly kind = "timestamp";

  public constructor(public readonly nanoseconds: bigint) {}
}

export class KolaDate {
  public readonly kind = "date";

  /** An ISO calendar date in YYYY-MM-DD form. */
  public constructor(public readonly value: string) {}
}

export class KolaTime {
  public readonly kind = "time";

  /** Millisecond-aligned nanoseconds since midnight; q time has millisecond precision. */
  public constructor(public readonly nanoseconds: bigint) {}
}

export class KolaTimespan {
  public readonly kind = "timespan";

  /** Signed duration in nanoseconds. */
  public constructor(public readonly nanoseconds: bigint) {}
}

const SUPPORTED_Q_OPERATOR_NAMES: Readonly<Record<string, true>> = Object.freeze({
  "+:": true,
  "-:": true,
  "*:": true,
  "%:": true,
  "&:": true,
  "|:": true,
  "^:": true,
  "=:": true,
  "<:": true,
  ">:": true,
  "$:": true,
  ",:": true,
  "#:": true,
  "_:": true,
  "~:": true,
  "!:": true,
  "?:": true,
  "@:": true,
  ".:": true,
  "0::": true,
  "1::": true,
  "2::": true,
  avg: true,
  last: true,
  sum: true,
  prd: true,
  min: true,
  max: true,
  exit: true,
  getenv: true,
  abs: true,
  sqrt: true,
  log: true,
  exp: true,
  sin: true,
  asin: true,
  cos: true,
  acos: true,
  tan: true,
  atan: true,
  enlist: true,
  ":": true,
  "+": true,
  "-": true,
  "*": true,
  "%": true,
  "&": true,
  "|": true,
  "^": true,
  "=": true,
  "<": true,
  ">": true,
  "$": true,
  ",": true,
  "#": true,
  _: true,
  "~": true,
  "!": true,
  "?": true,
  "@": true,
  ".": true,
  "0:": true,
  "1:": true,
  "2:": true,
  in: true,
  within: true,
  like: true,
  bin: true,
  ss: true,
  insert: true,
  wsum: true,
  wavg: true,
  div: true,
  xexp: true,
  setenv: true,
  "'": true,
  "/": true,
  "\\": true,
});

function validateQOperatorName(name: unknown): string {
  if (typeof name !== "string") {
    throw conversionError("KolaQOperator.name must be a string");
  }
  if (name.includes("\0")) {
    throw conversionError("KolaQOperator.name cannot contain NUL bytes");
  }
  if (!Object.hasOwn(SUPPORTED_Q_OPERATOR_NAMES, name)) {
    throw conversionError(`Unsupported q primitive operator name: ${JSON.stringify(name)}`);
  }
  return name;
}

function validateQLambdaParts(
  source: unknown,
  context: unknown,
): { readonly source: string; readonly context: string } {
  if (typeof source !== "string") {
    throw conversionError("KolaQLambda.source must be a string");
  }
  if (typeof context !== "string") {
    throw conversionError("KolaQLambda.context must be a string");
  }
  if (source.includes("\0")) {
    throw conversionError("KolaQLambda.source cannot contain NUL bytes");
  }
  if (context.includes("\0")) {
    throw conversionError("KolaQLambda.context cannot contain NUL bytes");
  }
  if (context !== "" && context.startsWith(".")) {
    throw conversionError("q lambda context omits the leading dot");
  }
  const trimmed = source.trim();
  const lambdaSource = trimmed.startsWith("k)") ? trimmed.slice(2) : trimmed;
  if (!lambdaSource.startsWith("{") || !lambdaSource.endsWith("}")) {
    throw conversionError("KolaQLambda.source must be brace-delimited");
  }
  return { source, context };
}

export class KolaQOperator {
  readonly #name: string;

  public static get PLUS(): KolaQOperator {
    return KOLA_Q_PLUS;
  }

  public constructor(name: string) {
    this.#name = validateQOperatorName(name);
    Object.freeze(this);
  }

  public get name(): string {
    return this.#name;
  }
}

const KOLA_Q_PLUS = new KolaQOperator("+");

export class KolaQLambda {
  readonly #source: string;
  readonly #context: string;

  public constructor(source: string, context: string = "") {
    const validated = validateQLambdaParts(source, context);
    this.#source = validated.source;
    this.#context = validated.context;
    Object.freeze(this);
  }

  public get source(): string {
    return this.#source;
  }

  public get context(): string {
    return this.#context;
  }
}

export function validatedQOperatorName(value: KolaQOperator): string {
  let name: unknown;
  try {
    name = value.name;
  } catch (cause) {
    throw conversionError("Invalid KolaQOperator instance", cause);
  }
  return validateQOperatorName(name);
}

export function validatedQLambdaParts(
  value: KolaQLambda,
): { readonly source: string; readonly context: string } {
  let source: unknown;
  let context: unknown;
  try {
    source = value.source;
    context = value.context;
  } catch (cause) {
    throw conversionError("Invalid KolaQLambda instance", cause);
  }
  return validateQLambdaParts(source, context);
}

interface KolaInputArray extends ReadonlyArray<KolaInput> {}

interface KolaInputRecord {
  readonly [key: string]: KolaInput;
}

interface KolaValueArray extends ReadonlyArray<KolaValue> {}

interface KolaValueRecord {
  readonly [key: string]: KolaValue;
}

export type KolaInput =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | KolaTimestamp
  | KolaDate
  | KolaTime
  | KolaTimespan
  | KolaQOperator
  | KolaQLambda
  | Table
  | Vector
  | KolaInputArray
  | KolaInputRecord;

export type KolaValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | Buffer
  | KolaTimestamp
  | KolaDate
  | KolaTime
  | KolaTimespan
  | KolaQOperator
  | KolaQLambda
  | Table
  | Vector
  | KolaValueArray
  | KolaValueRecord;
