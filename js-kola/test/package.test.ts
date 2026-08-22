import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

interface RootPackageMetadata {
  readonly name: string;
  readonly version: string;
  readonly engines: Readonly<Record<string, string>>;
  readonly exports: Readonly<Record<string, unknown>>;
  readonly scripts: Readonly<Record<string, string>>;
  readonly napi: {
    readonly binaryName: string;
    readonly targets: readonly string[];
  };
  readonly optionalDependencies: Readonly<Record<string, string>>;
}

interface PlatformPackageMetadata {
  readonly name: string;
  readonly version: string;
  readonly main: string;
  readonly files: readonly string[];
  readonly os: readonly string[];
  readonly cpu: readonly string[];
  readonly libc?: readonly string[];
  readonly engines: Readonly<Record<string, string>>;
}

async function readPackageMetadata<T>(relativePath: string): Promise<T> {
  const text = await readFile(new URL(relativePath, import.meta.url), "utf8");
  return JSON.parse(text) as T;
}

describe("npm package metadata", () => {
  it("uses the public package name, Node floor, napi-rs v3 targets, and generated loader build", async () => {
    const root = await readPackageMetadata<RootPackageMetadata>("../package.json");

    expect(root.name).toBe("kola-q");
    expect(root.engines.node).toBe(">=20");
    expect(Object.keys(root.exports)).toEqual(["."]);
    expect(root.napi).toEqual({
      binaryName: "kola_q",
      targets: [
        "x86_64-pc-windows-msvc",
        "x86_64-unknown-linux-gnu",
        "aarch64-apple-darwin",
      ],
    });
    expect(root.scripts["build:native"]).toContain(
      "--manifest-path ../bindings/napi-kola/Cargo.toml",
    );
    expect(root.scripts["build:native"]).toContain("--package-json-path ./package.json");
    expect(root.scripts["build:native"]).toContain(
      "--esm --js native.js --dts native.d.ts",
    );
  });

  it("keeps all optional native package versions and platform constraints synchronized", async () => {
    const root = await readPackageMetadata<RootPackageMetadata>("../package.json");
    const windows = await readPackageMetadata<PlatformPackageMetadata>(
      "../npm/win32-x64-msvc/package.json",
    );
    const linux = await readPackageMetadata<PlatformPackageMetadata>(
      "../npm/linux-x64-gnu/package.json",
    );
    const darwin = await readPackageMetadata<PlatformPackageMetadata>(
      "../npm/darwin-arm64/package.json",
    );

    expect(windows).toMatchObject({
      name: "kola-q-win32-x64-msvc",
      version: root.version,
      main: "kola_q.win32-x64-msvc.node",
      files: ["kola_q.win32-x64-msvc.node"],
      os: ["win32"],
      cpu: ["x64"],
      engines: { node: ">=20" },
    });
    expect(linux).toMatchObject({
      name: "kola-q-linux-x64-gnu",
      version: root.version,
      main: "kola_q.linux-x64-gnu.node",
      files: ["kola_q.linux-x64-gnu.node"],
      os: ["linux"],
      cpu: ["x64"],
      libc: ["glibc"],
      engines: { node: ">=20" },
    });
    expect(darwin).toMatchObject({
      name: "kola-q-darwin-arm64",
      version: root.version,
      main: "kola_q.darwin-arm64.node",
      files: ["kola_q.darwin-arm64.node"],
      os: ["darwin"],
      cpu: ["arm64"],
      engines: { node: ">=20" },
    });

    expect(root.optionalDependencies).toEqual({
      [windows.name]: root.version,
      [linux.name]: root.version,
      [darwin.name]: root.version,
    });
  });
});
