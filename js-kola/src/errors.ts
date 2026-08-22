import type { NativeError } from "./native-contract.js";

export interface KolaErrorOptions {
  readonly cause?: unknown;
}

export class KolaError extends Error {
  public readonly code: string;
  public readonly nativeMessage: string;

  public constructor(code: string, nativeMessage: string, options?: KolaErrorOptions) {
    super(nativeMessage, options);
    this.name = "KolaError";
    this.code = code;
    this.nativeMessage = nativeMessage;
  }
}

export class KolaIOError extends KolaError {
  public constructor(code: string, nativeMessage: string, options?: KolaErrorOptions) {
    super(code, nativeMessage, options);
    this.name = "KolaIOError";
  }
}

export class KolaAuthError extends KolaError {
  public constructor(code: string, nativeMessage: string, options?: KolaErrorOptions) {
    super(code, nativeMessage, options);
    this.name = "KolaAuthError";
  }
}

export function mapNativeError(error: NativeError): KolaError {
  const options: KolaErrorOptions = { cause: error };
  if (error.code === "KOLA_IO") {
    return new KolaIOError(error.code, error.message, options);
  }
  if (error.code === "KOLA_AUTH") {
    return new KolaAuthError(error.code, error.message, options);
  }
  return new KolaError(error.code, error.message, options);
}

export function rejectionToIOError(cause: unknown): KolaIOError {
  if (cause instanceof KolaIOError) {
    return cause;
  }
  const message = cause instanceof Error ? cause.message : String(cause);
  return new KolaIOError("KOLA_IO", message, { cause });
}

export function conversionError(message: string, cause?: unknown): KolaError {
  return new KolaError("KOLA_CONVERSION", message, cause === undefined ? undefined : { cause });
}
