# agent-bus

Two files that ran a 56-daemon agent fabric for two months on one Mac mini, with no broker,
no framework, and no dependencies beyond the Python standard library.

| File | Lines that matter | What it is |
|---|---|---|
| `comms.py` | `atomic_write`, `send_directive`, `drain_inbox`, `respond`, `request_token` | The bus. A message is a JSON file delivered by atomic rename. Request/response with correlation ids and a timeout in 22 lines. |
| `agent_base.py` | `BaseAgent.tick()` | The loop every agent ran. Inbox, four universal directives, circuit breaker, audit, idle work, dream, heartbeat. |

| `bd.py` | stub | Four no-op functions standing in for the private bead ledger so `agent_base.py` imports standalone. |

## What it carried

| | |
|---|---|
| Long-running daemons | 56 |
| Agents heartbeating at once | 47 |
| Messages surfaced to the operator, May 27 to Sep 23 2026 | 16,083 |
| Shared audit files | 190 |
| Bus latency in the selection shootout (2026-06-02) | p95 0.402 ms, p99 0.508 ms |
| Crash survival in that shootout | 50 / 50 |
| Fine-tuned model versions driven end to end | v92 to v110 |

The work it drove: fine-tuning question generators on Fireworks, judging them against an external
evaluator, chaining the next run from the result, and refusing to sit idle. The daemons that did that
(chain conductor, continuity watchdog, token manager, mesh doctor) are private; the two files here are
what they all had in common.

## Why files

Every alternative brought a process to run, a port to bind, credentials to manage, and a new single
point of failure. `rename()` is atomic on POSIX, a crashed writer leaves only an orphaned `.tmp`, and a
directory tree in iCloud made two machines share one bus for free. The shootout was run before the
choice, and its numbers sit in the module docstring.

## Run

```sh
python3 -c "import comms; print(comms.new_event_id())"
```

`agent_base.py` needs `comms.py` and the bead-ledger module `bd`; the included `bd.py` is a labelled no-op
stub standing in for the private ledger. Subclass `BaseAgent`, override `handle()` and `idle_cycle()`, call `run()`.
