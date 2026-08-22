import type { Table, Vector } from "apache-arrow";
import type { Buffer } from "node:buffer";

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
  | Table
  | Vector
  | KolaValueArray
  | KolaValueRecord;
