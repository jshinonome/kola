import {
  ensureNativeSuccess,
  normalizeInput,
  unwrapNativeValue,
} from "./conversion.js";
import { KolaError, KolaIOError, rejectionToIOError } from "./errors.js";
import { loadNativeBinding } from "./native-loader.js";
import type {
  NativeConnector,
  NativeOptions,
  NativeResult,
  NativeValue,
} from "./native-contract.js";
import type { KolaInput, KolaValue, QOptions } from "./types.js";

interface NormalizedOptions {
  readonly native: NativeOptions;
  readonly retries: number;
}

function normalizeOptions(options: QOptions): NormalizedOptions {
  if (!Number.isInteger(options.port) || options.port < 1 || options.port > 65_535) {
    throw new RangeError("QOptions.port must be an integer from 1 through 65535");
  }
  if (
    options.timeout !== undefined &&
    (!Number.isFinite(options.timeout) ||
      options.timeout <= 0 ||
      options.timeout > 86_400_000)
  ) {
    throw new RangeError(
      "QOptions.timeout must be a positive number of milliseconds no greater than 86400000",
    );
  }
  if (
    options.retries !== undefined &&
    (!Number.isInteger(options.retries) || options.retries < 0)
  ) {
    throw new RangeError("QOptions.retries must be a non-negative integer");
  }

  const timeoutSeconds = Math.ceil((options.timeout ?? 30_000) / 1_000);
  const native: NativeOptions = {
    host: options.host,
    port: options.port,
    ...(options.user === undefined ? {} : { user: options.user }),
    ...(options.password === undefined ? {} : { password: options.password }),
    ...(options.tls === undefined ? {} : { tls: options.tls }),
    timeoutSeconds,
  };
  return { native, retries: options.retries ?? 0 };
}

export class Q {
  readonly #nativeOptions: NativeOptions;
  readonly #retries: number;
  #connectorPromise: Promise<NativeConnector> | undefined;

  public constructor(options: QOptions) {
    const normalized = normalizeOptions(options);
    this.#nativeOptions = normalized.native;
    this.#retries = normalized.retries;
  }

  public static async connect(options: QOptions): Promise<Q> {
    const q = new Q(options);
    await q.connect();
    return q;
  }

  async #connector(): Promise<NativeConnector> {
    if (this.#connectorPromise === undefined) {
      this.#connectorPromise = loadNativeBinding().then(
        (binding) => new binding.NativeConnector(this.#nativeOptions),
      );
    }
    return this.#connectorPromise;
  }

  async #invoke(
    operation: (connector: NativeConnector) => Promise<NativeResult>,
  ): Promise<NativeResult> {
    try {
      const connector = await this.#connector();
      return await operation(connector);
    } catch (cause) {
      if (cause instanceof KolaError) {
        throw cause;
      }
      throw rejectionToIOError(cause);
    }
  }

  public async connect(): Promise<void> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        const result = await this.#invoke((connector) => connector.connect());
        ensureNativeSuccess(result);
        return;
      } catch (error) {
        if (!(error instanceof KolaIOError) || error.code !== "KOLA_IO" || attempt >= this.#retries) {
          throw error;
        }
      }
    }
  }

  public async disconnect(): Promise<void> {
    if (this.#connectorPromise === undefined) {
      return;
    }
    const result = await this.#invoke((connector) => connector.disconnect());
    ensureNativeSuccess(result);
  }

  public async sync(
    expression: string,
    ...args: readonly KolaInput[]
  ): Promise<KolaValue> {
    const nativeArgs: NativeValue[] = args.map((argument) => normalizeInput(argument));
    const result = await this.#invoke((connector) => connector.sync(expression, nativeArgs));
    return unwrapNativeValue(result);
  }

  public async asyn(expression: string, ...args: readonly KolaInput[]): Promise<void> {
    const nativeArgs: NativeValue[] = args.map((argument) => normalizeInput(argument));
    const result = await this.#invoke((connector) => connector.asyn(expression, nativeArgs));
    ensureNativeSuccess(result);
  }

  public async receive(): Promise<KolaValue> {
    const result = await this.#invoke((connector) => connector.receive());
    return unwrapNativeValue(result);
  }
}
