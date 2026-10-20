export declare const PROTOCOL_VERSION: 1;
export declare const OPS: readonly string[];

export type Level = 'free' | 'soft' | 'hard' | 'unknown';
export type Transport = 'ws' | 'rest';

export declare class ApiError extends Error {
  status: number;
  /** machine-readable code (body.error) — read this, not the message */
  code: string;
  reason: string | null;
  body: any;
}

export interface Endpoint { url: string; token: string }
export interface Uses { ex: string; sameKey?: boolean | null; sameHost?: boolean | null }

export interface ConnectOptions {
  transport?: 'auto' | Transport;
  /** daemon WebSocket, e.g. { url: 'ws://127.0.0.1:27720/v1', token } */
  ws?: Endpoint;
  /** agent REST, e.g. { url: 'http://127.0.0.1:27719', token } */
  rest?: Endpoint;
  /** register under this name to take part in the control plane */
  name?: string;
  /** exchanges this program calls itself */
  uses?: Uses[];
  reconnect?: boolean;
  timeoutMs?: number;
  now?: () => number;
}

export interface ControlState {
  v: 1;
  boot: string | null;
  epoch: number;
  yield: Record<string, { level: 'soft' | 'hard'; why: string[]; ttlMs: number; since?: number }>;
  frozen: Record<string, { untilMs: number; why: string | null }>;
  notice: { kind: string; inMs: number; why: string | null } | null;
}

export interface Signal { seq: number; at: number; slot: string; signal: { sym: string; [k: string]: any } }
export interface LevelEvent { ex: string; level: Level; why: string[]; frozen: boolean }

export interface Guard {
  level(ex: string): Level;
  frozen(ex: string): boolean;
  why(ex: string): string[];
  /** resolves true when below `below` (default 'hard'), false on timeout; 'unknown' never blocks */
  clear(ex: string, o?: { below?: 'soft' | 'hard'; timeoutMs?: number }): Promise<boolean>;
  on(ex: string, fn: (e: LevelEvent) => void): () => void;
}

export interface ControlTracker {
  /** a control state has been applied */
  readonly known: boolean;
  /** the agent answered control.state with 501/404 (no control plane) */
  readonly unsupported: boolean;
  readonly epoch: number | null;
  readonly boot: string | null;
  readonly notice: ControlState['notice'];
}

export interface Client {
  readonly transport: Transport | null;
  readonly tracker: ControlTracker;
  readonly clientId: string | null;
  readonly gate: { ok: boolean; reason: string | null } | null;
  readonly lastSeq: number;
  readonly guard: Guard;
  on(ev: 'signal', fn: (s: Signal) => void): () => void;
  on(ev: 'control', fn: (c: ControlState) => void): () => void;
  on(ev: 'level', fn: (e: LevelEvent) => void): () => void;
  on(ev: 'gate', fn: (g: { ok: boolean; reason: string | null }) => void): () => void;
  on(ev: 'notice', fn: (n: NonNullable<ControlState['notice']>) => void): () => void;
  on(ev: 'transport', fn: (t: { kind: Transport; wsError: string | null }) => void): () => void;
  on(ev: 'hello' | 'gap' | 'close' | 'error', fn: (v: any) => void): () => void;
  request(op: string, args?: object): Promise<any>;
  status(): Promise<any>;
  condsList(): Promise<any[]>;
  condsAdd(cond: object, ex: string): Promise<any>;
  condsDel(slot: string): Promise<any>;
  signalsSince(since?: number, limit?: number): Promise<{ boot: string; items: Signal[]; next: number; gap: boolean }>;
  peers(): Promise<any>;
  usage(): Promise<any>;
  refreshControl(): Promise<void>;
  reportUsage(ex: string, w: number, bucket?: string): Promise<any>;
  close(): void;
}

export declare function connect(o: ConnectOptions): Promise<Client>;

export interface ControlTracker {
  apply(c: ControlState, at: number): boolean;
  advance(at: number): void;
  level(ex: string, at: number): Level;
  isFrozen(ex: string, at: number): boolean;
  why(ex: string, at: number): string[];
  setUnsupported(): void;
  nextChangeAt(at: number): number | null;
  drainAcks(): { epoch: number; ex: string; level: Exclude<Level, 'unknown'> }[];
  readonly known: boolean;
  readonly unsupported: boolean;
  readonly notice: ControlState['notice'];
  readonly epoch: number | null;
  readonly boot: string | null;
}
export declare function createControlTracker(o?: { uses?: string[] }): ControlTracker;
export declare function createSseParser(onEvent: (e: { event: string; id: string | null; data: string }) => void): { feed(chunk: string): void; readonly lastEventId: string | null };
