import type { MessageId, Queue, QueuePrefix, ValidQueueName } from '@workflow/world';
import { monotonicFactory } from 'ulid';
import type { Bus } from './bus.ts';
import { req, STORES, transact } from './idb.ts';

// Held on globalThis so every copy of this module in a bundled host shares
// one monotonic factory, like the per-database realms below.
const ULID_KEY = Symbol.for('workflow-world-browser.ulid');
const ulid: () => string = ((globalThis as { [ULID_KEY]?: () => string })[ULID_KEY] ??= monotonicFactory());

interface QueuedMessage {
  messageId: MessageId;
  queueName: ValidQueueName;
  payload: unknown;
  availableAt: number;
  attempt: number;
  idempotencyKey?: string;
}

type Handler = (
  message: unknown,
  meta: { attempt: number; queueName: ValidQueueName; messageId: MessageId }
) => Promise<unknown>;

export interface BrowserQueue extends Queue {
  /** Start consuming (in the one instance that wins the consumer lock). */
  start(): void;
  stop(): void;
}

const isBrowser = typeof window !== 'undefined' && typeof document !== 'undefined';

/**
 * Per-database state shared by every World instance in this JS realm. A
 * framework can create several instances of one World in one process (the
 * Workflow runtime keeps a separate one for its queue handler), and they must
 * act as one queue: one consumer, and handlers visible to all of them.
 */
interface Realm {
  handlers: Map<string, Handler>;
  consumer?: object;
}
const REALMS_KEY = Symbol.for('workflow-world-browser.realms');
function realmFor(name: string): Realm {
  const g = globalThis as { [REALMS_KEY]?: Map<string, Realm> };
  g[REALMS_KEY] ??= new Map();
  let realm = g[REALMS_KEY].get(name);
  if (!realm) {
    realm = { handlers: new Map() };
    g[REALMS_KEY].set(name, realm);
  }
  return realm;
}

/** JSON with Uint8Array and Date preserved, for the HTTP transport. */
const wire = {
  stringify: (value: unknown) =>
    JSON.stringify(value, function (key, v) {
      const raw = (this as Record<string, unknown>)[key];
      if (raw instanceof Uint8Array) return { __wwb: 'u8', v: toBase64(raw) };
      if (raw instanceof Date) return { __wwb: 'date', v: raw.toISOString() };
      return v;
    }),
  parse: (text: string) =>
    JSON.parse(text, (_key, v) => {
      if (v && typeof v === 'object' && v.__wwb === 'u8') return fromBase64(v.v);
      if (v && typeof v === 'object' && v.__wwb === 'date') return new Date(v.v);
      return v;
    }),
};

function toBase64(bytes: Uint8Array): string {
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]!);
  return btoa(binary);
}

function fromBase64(text: string): Uint8Array {
  const binary = atob(text);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

/**
 * Outside a browser, messages go to the app's workflow route over HTTP, as
 * world-local's do. Under a server framework that route module registers its
 * handler lazily, and routing through it keeps the framework in the loop.
 */
function flowUrl(): string | undefined {
  if (isBrowser) return undefined;
  const env = globalThis.process?.env ?? {};
  const base = env.WORKFLOW_BROWSER_BASE_URL ?? (env.PORT ? `http://localhost:${env.PORT}` : undefined);
  return base ? `${base.replace(/\/$/, '')}/.well-known/workflow/v1/flow` : undefined;
}

/**
 * A durable queue in IndexedDB. A message is deleted only after its delivery
 * returns, so a tab closed mid-delivery leaves it in place, and the next tab
 * to open redelivers it. Exactly one consumer runs at a time: within a realm
 * the first instance to start, and across tabs the one holding the
 * `navigator.locks` lock.
 */
export function createQueue(db: IDBDatabase, bus: Bus, name: string, concurrency = 4): BrowserQueue {
  const realm = realmFor(name);
  const self = {};
  const inflight = new Set<string>();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let sweep: ReturnType<typeof setInterval> | undefined;
  let leader = false;
  let stopped = false;
  let requested = false;
  let releaseLock: (() => void) | undefined;

  function becomeLeader() {
    leader = true;
    // Bus hints are best-effort; a slow sweep bounds the cost of a lost one.
    sweep = setInterval(() => void pump(), 1000);
    (sweep as { unref?: () => void }).unref?.();
    void pump();
  }

  function ensureConsuming() {
    if (requested || stopped) return;
    // A browser tab can only deliver to a handler registered in it.
    if (isBrowser && realm.handlers.size === 0) return;
    if (realm.consumer && realm.consumer !== self) return;
    realm.consumer = self;
    requested = true;
    const locks = (globalThis.navigator as Navigator | undefined)?.locks;
    if (!locks) {
      becomeLeader();
      return;
    }
    void locks.request(`workflow-world-browser:${name}:queue`, () => {
      if (stopped) return;
      becomeLeader();
      // Hold the lock until stop() or the tab goes away.
      return new Promise<void>((resolve) => {
        releaseLock = resolve;
      });
    });
  }

  async function enqueue(
    queueName: ValidQueueName,
    payload: unknown,
    opts: { delaySeconds?: number; idempotencyKey?: string } = {}
  ) {
    const message: QueuedMessage = {
      messageId: `msg_${ulid()}` as MessageId,
      queueName,
      payload,
      availableAt: Date.now() + (opts.delaySeconds ?? 0) * 1000,
      attempt: 0,
      ...(opts.idempotencyKey ? { idempotencyKey: opts.idempotencyKey } : {}),
    };
    const id = await transact(db, [STORES.queue], 'readwrite', async (tx) => {
      const store = tx.objectStore(STORES.queue);
      if (opts.idempotencyKey) {
        const existing = await req(store.index('idempotencyKey').get(opts.idempotencyKey));
        if (existing) return existing.messageId as MessageId;
      }
      store.put(message);
      return message.messageId;
    });
    bus.emit({ type: 'queued' });
    return { messageId: id };
  }

  async function pump(): Promise<void> {
    if (!leader || stopped) return;
    clearTimeout(timer);
    const all: QueuedMessage[] = await req(db.transaction(STORES.queue).objectStore(STORES.queue).getAll());
    const now = Date.now();
    const due = all.filter((m) => m.availableAt <= now && !inflight.has(m.messageId));
    due.sort((a, b) => a.availableAt - b.availableAt);
    for (const message of due.slice(0, Math.max(0, concurrency - inflight.size))) {
      void deliver(message);
    }
    const next = all.filter((m) => m.availableAt > now).sort((a, b) => a.availableAt - b.availableAt)[0];
    if (next) {
      timer = setTimeout(() => void pump(), Math.min(next.availableAt - now, 2_147_000_000));
      (timer as { unref?: () => void }).unref?.();
    }
  }

  async function send(message: QueuedMessage): Promise<unknown> {
    const meta = { attempt: message.attempt + 1, queueName: message.queueName, messageId: message.messageId };
    const url = flowUrl();
    if (url) {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-wwb-attempt': String(meta.attempt),
          'x-wwb-queue-name': meta.queueName,
          'x-wwb-message-id': meta.messageId,
        },
        body: wire.stringify(message.payload),
      });
      const text = await response.text();
      if (!response.ok) throw new Error(`workflow route answered ${response.status}: ${text.slice(0, 500)}`);
      return text ? wire.parse(text) : undefined;
    }
    const handler = [...realm.handlers].find(([prefix]) => message.queueName.startsWith(prefix))?.[1];
    if (!handler) throw new Error(`no handler registered for ${message.queueName}`);
    return handler(message.payload, meta);
  }

  async function deliver(message: QueuedMessage) {
    inflight.add(message.messageId);
    let reschedule: number | undefined;
    try {
      const result = (await send(message)) as { timeoutSeconds?: number } | undefined;
      if (result && typeof result.timeoutSeconds === 'number') reschedule = result.timeoutSeconds * 1000;
    } catch (err) {
      console.error('[workflow-world-browser] delivery failed, will retry:', err);
      reschedule = Math.min(30_000, 250 * 2 ** message.attempt);
    }
    try {
      await transact(db, [STORES.queue], 'readwrite', async (tx) => {
        const store = tx.objectStore(STORES.queue);
        if (reschedule === undefined) store.delete(message.messageId);
        else store.put({ ...message, attempt: message.attempt + 1, availableAt: Date.now() + reschedule });
      });
    } finally {
      // Only now: a pump between the delivery and this write would still see
      // the message as due and deliver it a second time.
      inflight.delete(message.messageId);
    }
    void pump();
  }

  const unsubscribe = bus.on((m) => {
    if (m.type === 'queued') void pump();
  });

  return {
    async getDeploymentId() {
      return 'browser';
    },
    queue: (queueName, message, opts) => enqueue(queueName, message, opts),
    createQueueHandler(prefix: QueuePrefix, handler: Handler) {
      realm.handlers.set(prefix, handler);
      if (isBrowser) ensureConsuming();
      // In a tab, deliveries call `handler` directly. Under a server, the
      // queue POSTs to the app's workflow route, which lands here.
      return async (request: Request) => {
        const body = wire.parse(await request.text());
        const result = await handler(body, {
          attempt: Number(request.headers.get('x-wwb-attempt') ?? 1),
          queueName: (request.headers.get('x-wwb-queue-name') ?? prefix) as ValidQueueName,
          messageId: (request.headers.get('x-wwb-message-id') ?? `msg_${ulid()}`) as MessageId,
        });
        return new Response(wire.stringify(result ?? null), {
          headers: { 'content-type': 'application/json' },
        });
      };
    },
    start() {
      ensureConsuming();
    },
    stop() {
      stopped = true;
      leader = false;
      clearTimeout(timer);
      clearInterval(sweep);
      unsubscribe();
      if (realm.consumer === self) realm.consumer = undefined;
      releaseLock?.();
    },
  };
}
