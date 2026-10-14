import { eventIdToSlot, mintedSpecVersion, SPEC_VERSION_SUPPORTS_SLOT_IDENTITY, type ValidQueueName } from '@workflow/world';
import { IDBFactory } from 'fake-indexeddb';
import 'fake-indexeddb/auto';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { type BrowserWorld, createWorld } from '../src/index.ts';

const worlds: BrowserWorld[] = [];
afterEach(async () => {
  for (const w of worlds.splice(0)) await w.close();
});

let counter = 0;
async function open(factory: IDBFactory, name = `test-${counter++}`) {
  const world = await createWorld({ indexedDB: factory, name });
  worlds.push(world);
  return world;
}

async function until<T>(fn: () => Promise<T | undefined>, timeoutMs = 10_000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const value = await fn();
    if (value !== undefined) return value;
    if (Date.now() - start > timeoutMs) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 20));
  }
}

const input = new Uint8Array([1, 2, 3]);

describe('spec version', () => {
  afterEach(() => vi.unstubAllEnvs());

  it('declares what the SDK mints, and follows WORKFLOW_SEALED_LOG', async () => {
    expect((await open(new IDBFactory())).specVersion).toBe(mintedSpecVersion({}));
    vi.stubEnv('WORKFLOW_SEALED_LOG', '0');
    const world = await open(new IDBFactory());
    expect(world.specVersion).toBe(SPEC_VERSION_SUPPORTS_SLOT_IDENTITY);
    const runId = 'wrun_01TESTSPEC';
    const created: any = await world.events.create(runId, {
      eventType: 'run_created',
      eventData: { deploymentId: 'browser', workflowName: 'w', input },
    } as never);
    expect(created.run.specVersion).toBe(SPEC_VERSION_SUPPORTS_SLOT_IDENTITY);
  });

  it('creates a World where there is no process, as in a browser tab', async () => {
    vi.stubGlobal('process', undefined);
    try {
      const world = await open(new IDBFactory());
      expect(world.specVersion).toBe(mintedSpecVersion({}));
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe('storage', () => {
  it('allocates dense slots and reports what a stale writer missed', async () => {
    const world = await open(new IDBFactory());
    const runId = 'wrun_01TESTSLOTS';
    await world.events.create(runId, {
      eventType: 'run_created',
      eventData: { deploymentId: 'browser', workflowName: 'w', input },
    } as never);
    await world.events.create(runId, { eventType: 'run_started' } as never, { eventCount: 1 });
    const writes: any[] = await Promise.all(
      [0, 1, 2, 3].map((i) =>
        world.events.create(
          runId,
          { eventType: 'step_started', correlationId: `step_${i}`, eventData: { stepName: 's', input } } as never,
          { eventCount: 2 }
        )
      )
    );
    // Each born-running start also commits a synthetic step_created.
    expect(writes.map((w) => eventIdToSlot(w.event.eventId)).sort((a: any, b: any) => a - b)).toEqual([4, 6, 8, 10]);
    expect(writes.map((w) => w.events?.length ?? 0).sort()).toEqual([0, 2, 4, 6]);
    const list = await world.events.list({ runId, pagination: { sortOrder: 'asc' } } as never);
    expect(list.data.map((e) => eventIdToSlot(e.eventId))).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it('treats run_started as idempotent and rejects work on a finished run', async () => {
    const world = await open(new IDBFactory());
    const runId = 'wrun_01TESTTERM';
    await world.events.create(runId, {
      eventType: 'run_created',
      eventData: { deploymentId: 'browser', workflowName: 'w', input },
    } as never);
    await world.events.create(runId, { eventType: 'run_started' } as never);
    const again: any = await world.events.create(runId, { eventType: 'run_started' } as never);
    expect(again.event).toBeUndefined();
    expect(again.run.status).toBe('running');
    await world.events.create(runId, { eventType: 'run_completed', eventData: { output: input } } as never);
    await expect(world.events.create(runId, { eventType: 'run_started' } as never)).rejects.toThrow(/terminal/);
    await expect(
      world.events.create(runId, { eventType: 'step_created', correlationId: 'step_x', eventData: { stepName: 's', input } } as never)
    ).rejects.toThrow(/terminal/);
  });

  it('filters runs by one status or a list of them', async () => {
    const world = await open(new IDBFactory());
    for (const runId of ['wrun_01S1', 'wrun_01S2', 'wrun_01S3']) {
      await world.events.create(runId, {
        eventType: 'run_created',
        eventData: { deploymentId: 'browser', workflowName: 'w', input },
      } as never);
    }
    await world.events.create('wrun_01S2', { eventType: 'run_started' } as never);
    await world.events.create('wrun_01S3', { eventType: 'run_cancelled' } as never);
    const ids = async (status: unknown) =>
      (await world.runs.list({ status } as never)).data.map((r) => r.runId).sort();
    expect(await ids('pending')).toEqual(['wrun_01S1']);
    expect(await ids(['running', 'cancelled'])).toEqual(['wrun_01S2', 'wrun_01S3']);
    expect(await ids([])).toEqual([]);
    expect(await ids(undefined)).toEqual(['wrun_01S1', 'wrun_01S2', 'wrun_01S3']);
  });

  it('claims hook tokens across runs', async () => {
    const world = await open(new IDBFactory());
    for (const runId of ['wrun_01A', 'wrun_01B']) {
      await world.events.create(runId, {
        eventType: 'run_created',
        eventData: { deploymentId: 'browser', workflowName: 'w', input },
      } as never);
      await world.events.create(runId, { eventType: 'run_started' } as never);
    }
    const a: any = await world.events.create('wrun_01A', { eventType: 'hook_created', correlationId: 'hook_a', eventData: { token: 't' } } as never);
    expect(a.event.eventType).toBe('hook_created');
    const b: any = await world.events.create('wrun_01B', { eventType: 'hook_created', correlationId: 'hook_b', eventData: { token: 't' } } as never);
    expect(b.event.eventType).toBe('hook_conflict');
    expect((await world.hooks.getByToken('t')).runId).toBe('wrun_01A');
    // Finishing the owner releases the token.
    await world.events.create('wrun_01A', { eventType: 'run_cancelled' } as never);
    await expect(world.hooks.getByToken('t')).rejects.toThrow();
  });
});

describe('queue', () => {
  it('delivers to a handler registered in another instance of the same World', async () => {
    const factory = new IDBFactory();
    const name = `shared-${counter++}`;
    const producer = await open(factory, name);
    const handlerHost = await open(factory, name);
    const seen: unknown[] = [];
    handlerHost.createQueueHandler('__wkf_workflow_', async (message) => {
      seen.push(message);
    });
    await producer.start?.();
    await producer.queue('__wkf_workflow_test' as ValidQueueName, { runId: 'wrun_1', bytes: input } as never);
    await until(async () => (seen.length ? true : undefined));
    expect(seen).toEqual([{ runId: 'wrun_1', bytes: input }]);
  });

  it('keeps a message until its delivery returns, so a closed tab does not lose it', async () => {
    const factory = new IDBFactory();
    const name = `durable-${counter++}`;
    const tabA = await open(factory, name);
    let started = false;
    tabA.createQueueHandler('__wkf_workflow_', () => {
      started = true;
      return new Promise(() => {}); // the tab dies mid-delivery
    });
    await tabA.start?.();
    await tabA.queue('__wkf_workflow_test' as ValidQueueName, { runId: 'wrun_2' } as never);
    await until(async () => (started ? true : undefined));
    await tabA.close();

    const tabB = await open(factory, name);
    const seen: unknown[] = [];
    tabB.createQueueHandler('__wkf_workflow_', async (message) => {
      seen.push(message);
    });
    await tabB.start?.();
    await until(async () => (seen.length ? true : undefined));
    expect(seen).toEqual([{ runId: 'wrun_2' }]);
  });

  it('redelivers after the delay a handler asks for', async () => {
    const world = await open(new IDBFactory());
    const times: number[] = [];
    world.createQueueHandler('__wkf_workflow_', async () => {
      times.push(Date.now());
      return times.length === 1 ? { timeoutSeconds: 1 } : undefined;
    });
    await world.start?.();
    await world.queue('__wkf_workflow_test' as ValidQueueName, { runId: 'wrun_3' } as never);
    await until(async () => (times.length === 2 ? true : undefined));
    expect(times[1]! - times[0]!).toBeGreaterThanOrEqual(950);
  });
});
