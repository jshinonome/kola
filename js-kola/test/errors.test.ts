import { describe, expect, it } from "vitest";

import {
  KolaAuthError,
  KolaError,
  KolaIOError,
  mapNativeError,
  rejectionToIOError,
} from "../src/errors.js";

describe("stable public errors", () => {
  it("maps native IO and authentication codes to stable subclasses", () => {
    const ioPayload = { code: "KOLA_IO", message: "connection reset" };
    const authPayload = { code: "KOLA_AUTH", message: "access denied" };

    const ioError = mapNativeError(ioPayload);
    expect(ioError).toBeInstanceOf(KolaIOError);
    expect(ioError).toMatchObject({
      name: "KolaIOError",
      code: "KOLA_IO",
      nativeMessage: "connection reset",
      cause: ioPayload,
    });

    const authError = mapNativeError(authPayload);
    expect(authError).toBeInstanceOf(KolaAuthError);
    expect(authError).toMatchObject({
      name: "KolaAuthError",
      code: "KOLA_AUTH",
      nativeMessage: "access denied",
      cause: authPayload,
    });
  });

  it.each(["KOLA_SERVER", "KOLA_CONVERSION", "KOLA_UNSUPPORTED", "KOLA_ERROR"])(
    "keeps native code %s on KolaError",
    (code) => {
      const payload = { code, message: "native detail" };
      const error = mapNativeError(payload);
      expect(error).toBeInstanceOf(KolaError);
      expect(error).not.toBeInstanceOf(KolaIOError);
      expect(error).toMatchObject({ code, nativeMessage: "native detail", cause: payload });
    },
  );

  it("preserves rejected native exceptions as causes", () => {
    const cause = new Error("worker channel closed");
    const error = rejectionToIOError(cause);

    expect(error).toBeInstanceOf(KolaIOError);
    expect(error).toMatchObject({
      code: "KOLA_IO",
      nativeMessage: "worker channel closed",
      cause,
    });
  });
});
