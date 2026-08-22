export interface NativeOptions {
  readonly host: string;
  readonly port: number;
  readonly user?: string;
  readonly password?: string;
  readonly tls?: boolean;
  readonly timeoutSeconds?: number;
}

export interface NativeEntry {
  readonly key: string;
  readonly value: NativeValue;
}

export interface NativeValue {
  readonly tag: string;
  readonly boolValue?: boolean;
  readonly numberValue?: number;
  readonly bigintValue?: bigint;
  readonly stringValue?: string;
  readonly bytesValue?: Uint8Array;
  readonly items?: NativeValue[];
  readonly entries?: NativeEntry[];
}

export interface NativeError {
  readonly code: string;
  readonly message: string;
}

export interface NativeResult {
  readonly ok: boolean;
  readonly value?: NativeValue;
  readonly error?: NativeError;
}

export interface NativeConnector {
  connect(): Promise<NativeResult>;
  disconnect(): Promise<NativeResult>;
  sync(expression: string, args: NativeValue[]): Promise<NativeResult>;
  asyn(expression: string, args: NativeValue[]): Promise<NativeResult>;
  receive(): Promise<NativeResult>;
}

export interface NativeConnectorConstructor {
  new (options: NativeOptions): NativeConnector;
}

export interface NativeModule {
  readonly NativeConnector: NativeConnectorConstructor;
  readBinary6(path: string): Promise<NativeResult>;
  serializeAsIpcBytes6(
    messageType: "async" | "sync" | "response",
    compress: boolean,
    value: NativeValue,
  ): Promise<NativeResult>;
}
