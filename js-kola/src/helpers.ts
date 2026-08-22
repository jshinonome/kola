import { Table } from "apache-arrow";
import { Buffer } from "node:buffer";

import { normalizeInput, unwrapNativeValue } from "./conversion.js";
import { conversionError, KolaError, rejectionToIOError } from "./errors.js";
import { loadNativeBinding } from "./native-loader.js";
import type { NativeModule, NativeResult } from "./native-contract.js";
import type { KolaInput, KolaMessageType } from "./types.js";

async function invokeNativeHelper(
  operation: (binding: NativeModule) => Promise<NativeResult>,
): Promise<NativeResult> {
  try {
    const binding = await loadNativeBinding();
    return await operation(binding);
  } catch (cause) {
    if (cause instanceof KolaError) {
      throw cause;
    }
    throw rejectionToIOError(cause);
  }
}

export async function readBinary6(path: string): Promise<Table> {
  const result = await invokeNativeHelper((binding) => binding.readBinary6(path));
  const value = unwrapNativeValue(result);
  if (!(value instanceof Table)) {
    throw conversionError("readBinary6 returned a native value that was not a table");
  }
  return value;
}

export async function serializeAsIpcBytes6(
  messageType: KolaMessageType,
  compress: boolean,
  value: KolaInput,
): Promise<Buffer> {
  const nativeValue = normalizeInput(value);
  const result = await invokeNativeHelper((binding) =>
    binding.serializeAsIpcBytes6(messageType, compress, nativeValue),
  );
  const bytes = unwrapNativeValue(result);
  if (!Buffer.isBuffer(bytes)) {
    throw conversionError("serializeAsIpcBytes6 returned a native value that was not bytes");
  }
  return bytes;
}
