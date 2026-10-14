# workflow-world-browser

A [Workflow SDK](https://workflow-sdk.dev) World that runs in the browser. Durable
runs, steps, hooks, waits, and streams live in IndexedDB, and the queue is
consumed by whichever tab holds a Web Lock, so a workflow keeps going across
reloads and tab closes with no server.

- **Storage:** IndexedDB. Each `events.create` is one IndexedDB transaction
  over the run, its log, and the entities it touches. IndexedDB serializes
  overlapping transactions across tabs, which gives the World contract's
  dense, commit-time slot allocation (bump and report included) without a
  server.
- **Queue:** durable rows in IndexedDB. A message is deleted only after its
  delivery returns, so closing the tab mid-delivery leaves it for the next
  tab. One tab consumes at a time (`navigator.locks`); the others enqueue.
- **Streams:** IndexedDB rows, tailed live through `BroadcastChannel` hints.
- **Semantics** follow `@workflow/world-local`, the reference World:
  idempotent `run_started`, terminal-state rules, born-running `step_started`,
  hook token claims and `hook_conflict`, `(runId, resumeId)` resume dedup,
  `retryAfter`, `sinceCursor` deltas, and preloaded logs on `run_started`.

## Try it

**Live demo: https://pranaygp.github.io/workflow-world-browser/**

Press **Start a run**, then **Crash this tab** while a step is running: the
page reloads, re-acquires the Web Lock, redelivers the in-flight queue
message, and the step runs again as attempt 2 before the run completes. Or
press **Open a second tab**, start a run there, and close the driving tab
mid-step: the second tab takes over. The demo shows the event log, the queue
rows, and the lock state as it goes.

The demo drives the World with a small runner written against the World
interface, not the Workflow SDK runtime, which does not run in a browser.
[examples/demo](examples/demo) explains why and lists the exact steps.

## Install

```bash
npm install workflow-world-browser
```

## Use it in a tab

```ts
import { createWorld } from 'workflow-world-browser';

const world = await createWorld({ name: 'my-app' }); // one IndexedDB database = one World
world.createQueueHandler('__wkf_workflow_', async (message, meta) => {
  // your runtime's queue consumer
});
await world.start();
```

In a tab, deliveries call the registered handler directly. Model calls or any
other secrets-bearing work should go through your own server; the World itself
never needs one.

## Use it under a server (tests, SSR)

Set `WORKFLOW_TARGET_WORLD=workflow-world-browser`. Outside a browser the World
falls back to [`fake-indexeddb`](https://github.com/dumbmatter/fakeIndexedDB),
which is **in-memory**: data lasts as long as the process. The queue then
delivers over HTTP to the app's workflow route (`/.well-known/workflow/v1/flow`
on `http://localhost:$PORT`, or `WORKFLOW_BROWSER_BASE_URL`), the same way
`@workflow/world-local` does.

| Variable | Default | Meaning |
| --- | --- | --- |
| `WORKFLOW_BROWSER_DB_NAME` | `workflow-world` | IndexedDB database name (outside a browser; in a tab pass `name`). |
| `WORKFLOW_BROWSER_BASE_URL` | `http://localhost:$PORT` | Where the queue delivers when not in a browser. |

## Compatibility

`pnpm test` builds the package and runs the Workflow SDK's World compatibility
suite, [`@workflow/world-testing`](https://github.com/vercel/workflow/tree/main/packages/world-testing)
(pinned to `5.0.5`), against `dist/`, plus unit tests for slot
allocation, hook tokens, and queue durability. All 14 compatibility tests pass; the 2 in-band fence tests are skipped because the World does not advertise that capability.

Tested with `@workflow/world@5.0.4` and `@workflow/core@5.2.2` (`workflow@5.2.2`)
on Node.js 22. The World declares `mintedSpecVersion()`: spec version 8 by
default, 6 with `WORKFLOW_SEALED_LOG=0`. Not tested against Workflow 4.x.

Not implemented: encryption (`getEncryptionKeyForRun`), hook retention, the
`invoke` capability, `createBatch`, and `runs.waitForTerminalStatus`. The
runtime feature-detects all of them.

## License

Apache-2.0
