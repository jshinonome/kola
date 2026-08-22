import { KolaIOError } from "./errors.js";
import type { NativeModule } from "./native-contract.js";

export type NativeImporter = () => Promise<unknown>;

const generatedNativeUrl = new URL("../native.js", import.meta.url);

async function importGeneratedNative(): Promise<unknown> {
  return import(generatedNativeUrl.href);
}

function isNativeModule(value: unknown): value is NativeModule {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "NativeConnector" in value &&
    typeof value.NativeConnector === "function" &&
    "readBinary6" in value &&
    typeof value.readBinary6 === "function" &&
    "serializeAsIpcBytes6" in value &&
    typeof value.serializeAsIpcBytes6 === "function"
  );
}

export async function loadNativeBinding(
  importer: NativeImporter = importGeneratedNative,
): Promise<NativeModule> {
  try {
    const moduleValue: unknown = await importer();
    if (!isNativeModule(moduleValue)) {
      throw new TypeError("The generated loader did not expose the expected native exports");
    }
    return moduleValue;
  } catch (cause) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    throw new KolaIOError(
      "KOLA_NATIVE_LOAD",
      "Unable to load the kola-q native addon. For a local checkout run " +
        "`npm run build:native`; for an installed package, reinstall with optional " +
        `dependencies enabled. Loader detail: ${detail}`,
      { cause },
    );
  }
}
