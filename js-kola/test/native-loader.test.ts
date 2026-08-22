import { describe, expect, it } from "vitest";

import { KolaIOError } from "../src/errors.js";
import { loadNativeBinding } from "../src/native-loader.js";

describe("generated native loader integration", () => {
  it("accepts the generated napi-rs export surface", async () => {
    const moduleValue = {
      NativeConnector: class NativeConnector {},
      readBinary6: async () => ({ ok: true }),
      serializeAsIpcBytes6: async () => ({ ok: true }),
    };

    await expect(loadNativeBinding(async () => moduleValue)).resolves.toBe(moduleValue);
  });

  it("reports missing local or optional native binaries clearly", async () => {
    const cause = new Error("Unsupported platform or native binary not found");

    await expect(
      loadNativeBinding(async () => {
        throw cause;
      }),
    ).rejects.toMatchObject({
      name: "KolaIOError",
      code: "KOLA_NATIVE_LOAD",
      cause,
    });

    try {
      await loadNativeBinding(async () => {
        throw cause;
      });
      throw new Error("Expected the loader to fail");
    } catch (error) {
      expect(error).toBeInstanceOf(KolaIOError);
      if (!(error instanceof KolaIOError)) {
        throw error;
      }
      expect(error.nativeMessage).toContain("npm run build:native");
      expect(error.nativeMessage).toContain("optional dependencies enabled");
      expect(error.nativeMessage).toContain(cause.message);
    }
  });

  it("rejects a generated loader with an incompatible declaration surface", async () => {
    await expect(loadNativeBinding(async () => ({ NativeConnector: class {} }))).rejects.toMatchObject(
      {
        code: "KOLA_NATIVE_LOAD",
        cause: expect.objectContaining({
          message: "The generated loader did not expose the expected native exports",
        }),
      },
    );
  });
});
