import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  TooEarlyError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type { Event, Storage } from '@workflow/world';
import {
  eventIdToSlot,
  getMaxEventsPerRun,
  slotToEventId,
  stripEventDataRefs,
} from '@workflow/world';
import { req, STORES, transact } from './idb.ts';

// Entity rows are loosely typed: they are exactly what the World contract's
// entity schemas describe, stored as structured-cloned objects.
type Row = Record<string, any>;

const TERMINAL_RUN = new Set(['completed', 'failed', 'cancelled']);
const TERMINAL_STEP = new Set(['completed', 'failed', 'cancelled']);
const CHILD_CREATION = new Set(['step_created', 'hook_created', 'wait_created']);

/**
 * `getMaxEventsPerRun` reads `process.env.WORKFLOW_MAX_EVENTS`, and a browser
 * tab has no `process`: fall back to the World contract's default there.
 */
const maxEventsPerRun = () => (typeof process === 'undefined' ? 25_000 : getMaxEventsPerRun());

/**
 * `status` is one status or a list of them. An empty list matches nothing,
 * like SQL `IN ()`, which is not the same as leaving the filter out.
 */
function matchesStatus(status: string, filter?: string | string[]) {
  if (filter === undefined) return true;
  return Array.isArray(filter) ? filter.includes(status) : status === filter;
}

function page<T>(items: T[], limit = 100, cursor?: string | null) {
  const start = cursor ? Number(cursor) : 0;
  const data = items.slice(start, start + limit);
  const next = start + data.length;
  const hasMore = next < items.length;
  return { data, cursor: hasMore ? String(next) : null, hasMore };
}

function stripRun(row: Row, resolveData?: string) {
  const { eventCount: _eventCount, ...run } = row;
  if (resolveData === 'none') {
    delete run.input;
    delete run.output;
    delete run.error;
  }
  return run;
}

function stripData(row: Row, resolveData?: string) {
  if (resolveData !== 'none') return row;
  const { input: _i, output: _o, error: _e, ...rest } = row;
  return rest;
}

function stripHook(row: Row, resolveData?: string) {
  if (resolveData !== 'none') return row;
  const { metadata: _m, ...rest } = row;
  return rest;
}

function toEvent(row: Row): Event {
  const { slot, runId: _r, ...event } = row;
  return {
    ...event,
    runId: row.runId,
    eventId: slotToEventId(slot),
    createdAt: new Date(row.createdAt),
  } as Event;
}

function projectEvent(event: Event, resolveData?: string): Event {
  if (!resolveData || resolveData === 'all') return event;
  return stripEventDataRefs(event, resolveData as 'none' | 'skip-step-inputs');
}

/**
 * Storage over IndexedDB. Every `events.create` is one IndexedDB
 * transaction over the run, its log, and the entities it touches, so slot
 * allocation is atomic across tabs: IndexedDB serializes overlapping
 * readwrite transactions, which is exactly the "allocate at the commit"
 * property the World contract asks for. Event semantics (idempotent starts,
 * terminal-state rules, hook token claims, lazy step starts) follow
 * `@workflow/world-local`, the reference implementation.
 */
export function createStorage(
  db: IDBDatabase,
  worldSpecVersion: number,
  onEvent: (runId: string) => void
): Storage {
  async function runEvents(runId: string): Promise<Row[]> {
    const tx = db.transaction(STORES.events, 'readonly');
    const range = IDBKeyRange.bound([runId, 0], [runId, Number.MAX_SAFE_INTEGER]);
    return req(tx.objectStore(STORES.events).getAll(range));
  }

  async function listEvents(params: Row) {
    const rows = await runEvents(params.runId);
    let events = rows.map(toEvent);
    const desc = params.pagination?.sortOrder === 'desc';
    if (desc) events = events.reverse();
    const limit = params.pagination?.limit ?? Number.MAX_SAFE_INTEGER;
    // The cursor is the last slot returned, so a reader that folds appends
    // can resume exactly where it stopped.
    const after = params.pagination?.cursor ? Number(params.pagination.cursor) : undefined;
    const remaining =
      after === undefined
        ? events
        : events.filter((e) => {
            const slot = eventIdToSlot(e.eventId)!;
            return desc ? slot < after : slot > after;
          });
    const data = remaining.slice(0, limit).map((e) => projectEvent(e, params.resolveData));
    const last = data.at(-1);
    return {
      data,
      cursor: last ? String(eventIdToSlot(last.eventId)) : (params.pagination?.cursor ?? null),
      hasMore: remaining.length > limit,
    };
  }

  const storage = {
    runs: {
      async get(id: string, params?: { resolveData?: string }) {
        const row = await req(db.transaction(STORES.runs).objectStore(STORES.runs).get(id));
        if (!row) throw new WorkflowRunNotFoundError(id);
        return stripRun(row, params?.resolveData);
      },
      async list(params: Row = {}) {
        const rows: Row[] = await req(db.transaction(STORES.runs).objectStore(STORES.runs).getAll());
        let runs = rows.filter(
          (r) =>
            (!params.workflowName || r.workflowName === params.workflowName) &&
            matchesStatus(r.status, params.status) &&
            (!params.deploymentId || r.deploymentId === params.deploymentId)
        );
        runs.sort((a, b) => (a.runId < b.runId ? -1 : 1));
        if (params.pagination?.sortOrder !== 'asc') runs = runs.reverse();
        return page(
          runs.map((r) => stripRun(r, params.resolveData)),
          params.pagination?.limit,
          params.pagination?.cursor
        );
      },
    },

    steps: {
      async get(runId: string, stepId: string, params?: { resolveData?: string }) {
        const row = await req(db.transaction(STORES.steps).objectStore(STORES.steps).get([runId, stepId]));
        if (!row) throw new WorkflowWorldError(`Step "${stepId}" not found`, { status: 404 });
        return stripData(row, params?.resolveData);
      },
      async list(params: Row) {
        const rows: Row[] = await req(
          db.transaction(STORES.steps).objectStore(STORES.steps).index('runId').getAll(params.runId)
        );
        rows.sort((a, b) => (a.createdAt < b.createdAt ? -1 : 1));
        if (params.pagination?.sortOrder === 'desc') rows.reverse();
        return page(
          rows.map((r) => stripData(r, params.resolveData)),
          params.pagination?.limit,
          params.pagination?.cursor
        );
      },
    },

    hooks: {
      async get(hookId: string, params?: { resolveData?: string }) {
        const row = await req(db.transaction(STORES.hooks).objectStore(STORES.hooks).get(hookId));
        if (!row) throw new HookNotFoundError(hookId);
        return stripHook(row, params?.resolveData);
      },
      async getByToken(token: string, params?: { resolveData?: string }) {
        const row = await req(db.transaction(STORES.hooks).objectStore(STORES.hooks).index('token').get(token));
        if (!row) throw new HookNotFoundError(token);
        return stripHook(row, params?.resolveData);
      },
      async list(params: Row = {}) {
        const store = db.transaction(STORES.hooks).objectStore(STORES.hooks);
        const rows: Row[] = params.runId ? await req(store.index('runId').getAll(params.runId)) : await req(store.getAll());
        rows.sort((a, b) => (a.createdAt < b.createdAt ? -1 : 1));
        if (params.pagination?.sortOrder === 'desc') rows.reverse();
        return page(
          rows.map((r) => stripHook(r, params.resolveData)),
          params.pagination?.limit,
          params.pagination?.cursor
        );
      },
    },

    events: {
      async create(runId: string | null, data: Row, params: Row = {}) {
        const id = runId ?? data.runId;
        if (!id) throw new WorkflowWorldError('runId is required', { status: 400 });
        const result = await transact(
          db,
          [STORES.runs, STORES.events, STORES.steps, STORES.hooks, STORES.waits],
          'readwrite',
          (tx) => applyEvent(tx, id, data, params, worldSpecVersion)
        );
        onEvent(id);
        // Reads that the runtime asks to ride along with a write.
        const extra: Row = {};
        if (result.preload) {
          const all = await listEvents({ runId: id, pagination: { sortOrder: 'asc' }, resolveData: params.resolveData });
          Object.assign(extra, { events: all.data, cursor: all.cursor, hasMore: false });
        } else if (typeof params.sinceCursor === 'string') {
          const delta = await listEvents({
            runId: id,
            pagination: { sortOrder: 'asc', cursor: params.sinceCursor },
            resolveData: params.resolveData,
          });
          Object.assign(extra, { events: delta.data, cursor: delta.cursor, hasMore: false });
        }
        const { preload: _preload, ...rest } = result;
        const event = rest.event ? projectEvent(rest.event, params.resolveData) : undefined;
        return { ...rest, ...(event ? { event } : {}), ...extra };
      },

      list: (params: Row) => listEvents(params),

      async get(runId: string, eventId: string, params?: { resolveData?: string }) {
        const slot = eventIdToSlot(eventId);
        const row = slot === null ? undefined : await req(db.transaction(STORES.events).objectStore(STORES.events).get([runId, slot]));
        if (!row) throw new WorkflowWorldError(`Event "${eventId}" not found`, { status: 404 });
        return projectEvent(toEvent(row), params?.resolveData);
      },

      async listByCorrelationId(params: Row) {
        const rows = await runEvents(params.runId);
        const events = rows.filter((r) => r.correlationId === params.correlationId).map(toEvent);
        if (params.pagination?.sortOrder === 'desc') events.reverse();
        return page(
          events.map((e) => projectEvent(e, params.resolveData)),
          params.pagination?.limit ?? 1000,
          params.pagination?.cursor
        );
      },
    },
  };
  return storage as unknown as Storage;
}

/** The one place the log's semantics live: validate, materialize, append. */
async function applyEvent(tx: IDBTransaction, runId: string, data: Row, params: Row, worldSpecVersion: number) {
  const runs = tx.objectStore(STORES.runs);
  const events = tx.objectStore(STORES.events);
  const steps = tx.objectStore(STORES.steps);
  const hooks = tx.objectStore(STORES.hooks);
  const waits = tx.objectStore(STORES.waits);
  const now = new Date();
  const eventData: Row = data.eventData ?? {};
  const specVersion = data.specVersion ?? worldSpecVersion;
  const type: string = data.eventType;

  let run: Row | undefined = await req(runs.get(runId));
  const appended: Row[] = [];
  const append = (event: Row) => {
    run!.eventCount = (run!.eventCount ?? 0) + 1;
    const occurredAt = params.occurredAt instanceof Date ? params.occurredAt : now;
    const row = { runId, slot: run!.eventCount, createdAt: occurredAt.getTime(), specVersion, ...event };
    events.put(row);
    appended.push(row);
    return row;
  };
  let entity: Row = {};
  let preload = false;
  const lifecycle = () => ({ run: stripRun(run!), maxEvents: maxEventsPerRun() });

  const createRun = (source: Row) => ({
    runId,
    status: 'pending',
    deploymentId: source.deploymentId,
    workflowName: source.workflowName,
    specVersion,
    executionContext: source.executionContext,
    input: source.input,
    attributes: source.attributes ?? {},
    encryptionPublicKey: source.encryptionPublicKey,
    createdAt: now,
    updatedAt: now,
    eventCount: 0,
  });

  // ---- run existence ------------------------------------------------------
  if (type === 'run_created') {
    if (run) throw new EntityConflictError(`Workflow run "${runId}" already exists`);
    run = createRun(eventData);
  } else if (!run) {
    // Resilient start: the queue can bootstrap a run whose run_created was lost.
    if (type === 'run_started' && eventData.deploymentId && eventData.workflowName && eventData.input !== undefined) {
      run = createRun(eventData);
      append({ eventType: 'run_created', eventData: { ...eventData } });
    } else if (type === 'run_started' || type === 'run_failed' || type === 'attr_set') {
      throw new WorkflowRunNotFoundError(runId);
    } else {
      throw new WorkflowRunNotFoundError(runId);
    }
  }
  const reportFrom = (typeof params.eventCount === 'number' ? params.eventCount : run.eventCount ?? 0) + 1;
  const runTerminal = TERMINAL_RUN.has(run.status);
  const lazyStart = type === 'step_started' && eventData.input !== undefined && eventData.stepName !== undefined;

  // ---- terminal run rules ---------------------------------------------------
  if (runTerminal) {
    if (type === 'run_cancelled' && run.status === 'cancelled') {
      append({ eventType: 'run_cancelled', eventData });
      runs.put(run);
      return finish(lifecycle());
    }
    if (type === 'run_started') {
      throw new RunExpiredError(`Workflow run "${runId}" is already in terminal state "${run.status}"`);
    }
    if (type.startsWith('run_')) {
      throw new EntityConflictError(`Cannot transition run from terminal state "${run.status}"`);
    }
    if (CHILD_CREATION.has(type) || lazyStart) {
      throw new EntityConflictError(`Cannot create new entities on run in terminal state "${run.status}"`);
    }
    if (type === 'attr_set') {
      throw new EntityConflictError(`Cannot set attributes on run in terminal state "${run.status}"`);
    }
    if (type === 'hook_received') {
      throw new RunExpiredError(`Workflow run "${runId}" is already in a terminal state`);
    }
  }

  // ---- step preconditions ---------------------------------------------------
  let step: Row | undefined;
  if (type.startsWith('step_') && type !== 'step_created') {
    step = await req(steps.get([runId, data.correlationId]));
    if (!step && !lazyStart) {
      throw new WorkflowWorldError(`Step "${data.correlationId}" not found`, { status: 404 });
    }
    if (step && lazyStart) {
      // The lazy-start exactly-once gate: a concurrent handler created it.
      throw new EntityConflictError(`Step "${data.correlationId}" already created`);
    }
    if (step) {
      if (TERMINAL_STEP.has(step.status)) {
        throw new EntityConflictError(`Cannot modify step in terminal state "${step.status}"`);
      }
      if (runTerminal && (step.status !== 'running' || type === 'step_started')) {
        throw new RunExpiredError(
          `Cannot ${type === 'step_started' ? 'start' : 'modify non-running'} step on run in terminal state "${run.status}"`
        );
      }
    }
  }

  switch (type) {
    case 'run_created':
      append({ eventType: 'run_created', eventData });
      entity = { run: stripRun(run) };
      break;

    case 'run_started': {
      if (run.status === 'running') {
        // Idempotent: concurrent invocations each "start" the run.
        runs.put(run);
        return finish({ ...lifecycle(), preload: !params.skipPreload, noEvent: true });
      }
      run.status = 'running';
      run.startedAt ??= now;
      append({ eventType: 'run_started' });
      entity = lifecycle();
      preload = !params.skipPreload;
      break;
    }

    case 'run_completed':
    case 'run_failed':
    case 'run_cancelled': {
      run.status = type.slice('run_'.length);
      run.completedAt = now;
      if (type === 'run_completed') run.output = eventData.output;
      if (type === 'run_failed') {
        run.error = eventData.error;
        run.errorCode = eventData.errorCode;
      }
      append({ eventType: type, eventData });
      // A finished run's hooks and waits are released.
      const owned: Row[] = await req(hooks.index('runId').getAll(runId));
      for (const hook of owned) hooks.delete(hook.hookId);
      const pendingWaits = await req(waits.getAll(IDBKeyRange.bound([runId, ''], [runId, '￿'])));
      for (const wait of pendingWaits as Row[]) waits.delete([runId, wait.waitId]);
      entity = lifecycle();
      break;
    }

    case 'attr_set': {
      const next = { ...(run.attributes ?? {}) };
      for (const change of eventData.changes ?? []) {
        if (change.value === null) delete next[change.key];
        else next[change.key] = change.value;
      }
      run.attributes = next;
      append({ eventType: 'attr_set', ...(data.correlationId ? { correlationId: data.correlationId } : {}), eventData });
      entity = { run: stripRun(run) };
      break;
    }

    case 'step_created': {
      if (await req(steps.get([runId, data.correlationId]))) {
        throw new EntityConflictError(`Step "${data.correlationId}" already created`);
      }
      const created = newStep(runId, data.correlationId, eventData, specVersion, now);
      steps.put(created);
      append({ eventType: 'step_created', correlationId: data.correlationId, eventData });
      entity = { step: created };
      break;
    }

    case 'step_started': {
      let stepCreated: true | undefined;
      if (!step) {
        // Born running: materialize the step and a synthetic step_created
        // ahead of the start, so replay still observes the creation.
        step = newStep(runId, data.correlationId, eventData, specVersion, now);
        append({
          eventType: 'step_created',
          correlationId: data.correlationId,
          eventData: { stepName: eventData.stepName, input: eventData.input },
        });
        stepCreated = true;
      } else if (step.retryAfter && new Date(step.retryAfter).getTime() > Date.now()) {
        throw new TooEarlyError(
          `Cannot start step "${data.correlationId}": retryAfter timestamp has not been reached yet`,
          { retryAfter: Math.ceil((new Date(step.retryAfter).getTime() - Date.now()) / 1000) }
        );
      }
      step.status = 'running';
      step.attempt += 1;
      step.startedAt ??= now;
      step.retryAfter = undefined;
      step.updatedAt = now;
      steps.put(step);
      const { input: _input, ...meta } = eventData;
      append({ eventType: 'step_started', correlationId: data.correlationId, eventData: meta });
      entity = { step, ...(stepCreated ? { stepCreated } : {}) };
      break;
    }

    case 'step_completed':
    case 'step_failed':
    case 'step_retrying': {
      const s = step!;
      if (type === 'step_completed') {
        s.status = 'completed';
        s.output = eventData.result;
        s.completedAt = now;
      } else if (type === 'step_failed') {
        s.status = 'failed';
        s.error = eventData.error;
        s.completedAt = now;
      } else {
        s.status = 'pending';
        s.error = eventData.error;
        s.retryAfter = eventData.retryAfter;
      }
      s.updatedAt = now;
      steps.put(s);
      append({ eventType: type, correlationId: data.correlationId, eventData });
      entity = { step: s };
      break;
    }

    case 'hook_created': {
      const own: Row | undefined = await req(hooks.get(data.correlationId));
      if (own) throw new EntityConflictError(`Hook "${data.correlationId}" already created`);
      const holder: Row | undefined = await req(hooks.index('token').get(eventData.token));
      if (holder) {
        // The token belongs to another live hook: record the conflict in
        // this run's log instead of failing the write.
        append({
          eventType: 'hook_conflict',
          correlationId: data.correlationId,
          eventData: { token: eventData.token, conflictingRunId: holder.runId },
        });
        break;
      }
      const hook = {
        runId,
        hookId: data.correlationId,
        token: eventData.token,
        ownerId: 'browser',
        projectId: 'browser',
        environment: 'browser',
        metadata: eventData.metadata,
        isWebhook: eventData.isWebhook ?? false,
        ...(eventData.isSystem ? { isSystem: true } : {}),
        createdAt: now,
        specVersion,
      };
      hooks.put(hook);
      append({ eventType: 'hook_created', correlationId: data.correlationId, eventData });
      entity = { hook };
      break;
    }

    case 'hook_received': {
      const hook = await req(hooks.get(data.correlationId));
      if (!hook) throw new HookNotFoundError(data.correlationId);
      if (params.resumeId) {
        // (runId, resumeId) dedup: repeated writers of one resume converge.
        const prior = ((await req(events.getAll(IDBKeyRange.bound([runId, 0], [runId, Number.MAX_SAFE_INTEGER])))) as Row[]).find(
          (e) => e.eventType === 'hook_received' && e.resumeId === params.resumeId
        );
        if (prior) {
          if (prior.correlationId !== data.correlationId) {
            throw new EntityConflictError(`hook_received resumeId "${params.resumeId}" already recorded for a different hook`);
          }
          runs.put(run);
          return finish({ existing: prior });
        }
      }
      append({
        eventType: 'hook_received',
        correlationId: data.correlationId,
        eventData,
        ...(params.resumeId ? { resumeId: params.resumeId } : {}),
      });
      break;
    }

    case 'hook_disposed': {
      const disposed = ((await req(events.getAll(IDBKeyRange.bound([runId, 0], [runId, Number.MAX_SAFE_INTEGER])))) as Row[]).some(
        (e) => e.eventType === 'hook_disposed' && e.correlationId === data.correlationId
      );
      if (disposed) throw new EntityConflictError(`Hook "${data.correlationId}" already disposed`);
      hooks.delete(data.correlationId);
      append({ eventType: 'hook_disposed', correlationId: data.correlationId, eventData });
      break;
    }

    case 'wait_created': {
      if (await req(waits.get([runId, data.correlationId]))) {
        throw new EntityConflictError(`Wait "${data.correlationId}" already exists`);
      }
      const wait = {
        runId,
        waitId: data.correlationId,
        status: 'waiting',
        resumeAt: eventData.resumeAt,
        createdAt: now,
        updatedAt: now,
        specVersion,
      };
      waits.put(wait);
      append({ eventType: 'wait_created', correlationId: data.correlationId, eventData });
      entity = { wait: { ...wait, waitId: `${runId}-${data.correlationId}` } };
      break;
    }

    case 'wait_completed': {
      const existing: Row | undefined = await req(waits.get([runId, data.correlationId]));
      if (!existing) throw new WorkflowWorldError(`Wait "${data.correlationId}" not found`, { status: 404 });
      if (existing.status === 'completed') {
        throw new EntityConflictError(`Wait "${data.correlationId}" already completed`);
      }
      const wait = { ...existing, status: 'completed', completedAt: now, updatedAt: now };
      waits.put(wait);
      append({ eventType: 'wait_completed', correlationId: data.correlationId, eventData });
      entity = { wait: { ...wait, waitId: `${runId}-${data.correlationId}` } };
      break;
    }

    default:
      throw new WorkflowWorldError(`Unsupported event type "${type}"`, { status: 400 });
  }

  run.updatedAt = now;
  runs.put(run);
  return finish({ ...entity, preload });

  async function finish(result: Row) {
    const { existing, noEvent, ...rest } = result;
    if (existing) return { event: toEvent(existing), preload: false };
    const ours = appended.at(-1);
    // Bump and report: the events between the writer's view and our commit.
    const firstOurs = appended[0]?.slot ?? run!.eventCount + 1;
    let skipped: Event[] = [];
    if (!noEvent && firstOurs - 1 >= reportFrom) {
      const range = IDBKeyRange.bound([runId, reportFrom], [runId, firstOurs - 1]);
      skipped = ((await req(events.getAll(range))) as Row[]).map(toEvent);
    }
    return {
      ...(ours && !noEvent ? { event: toEvent(ours) } : {}),
      ...rest,
      ...(skipped.length ? { events: skipped, cursor: String(firstOurs - 1), hasMore: false } : {}),
    };
  }
}

function newStep(runId: string, stepId: string, eventData: Row, specVersion: number, now: Date): Row {
  return {
    runId,
    stepId,
    stepName: eventData.stepName,
    status: 'pending',
    input: eventData.input,
    attempt: 0,
    createdAt: now,
    updatedAt: now,
    specVersion,
  };
}
