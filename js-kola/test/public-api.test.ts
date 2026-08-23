import { describe, expect, it } from "vitest";

import * as publicApi from "../src/index.js";

describe("public facade surface", () => {
  it("exports only the facade runtime and keeps native internals private", () => {
    expect(Object.keys(publicApi).sort()).toEqual([
      "KolaAuthError",
      "KolaDate",
      "KolaError",
      "KolaIOError",
      "KolaQLambda",
      "KolaQOperator",
      "KolaTime",
      "KolaTimespan",
      "KolaTimestamp",
      "Q",
      "readBinary6",
      "serializeAsIpcBytes6",
    ]);
  });
});
