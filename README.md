# agent-bus

Two files that ran a multi-agent fabric of dozens of long-running daemons on one machine, with no broker,
no framework, and no dependencies beyond the Python standard library.

| File | Lines that matter | What it is |
|---|---|---|
| `comms.py` | `atomic_write`, `send_directive`, `drain_inbox`, `respond`, `request_token` | The bus. A message is a JSON file delivered by atomic rename. Request/response with correlation ids and a timeout in 22 lines. |
| `agent_base.py` | `BaseAgent.tick()` | The loop every agent ran. Inbox, four universal directives, circuit breaker, audit, idle work, dream, heartbeat. |
| `bd.py` | stub | Four no-op functions standing in for the private bead ledger so `agent_base.py` imports standalone. |

## What it carried

The work it drove: continuous model fine-tuning and evaluation loops, where each result chose the next run
and no agent was allowed to sit idle. The daemons that did that
(chain conductor, continuity watchdog, token manager, mesh doctor) are private; the two files here are
what they all had in common.

## Why files

Every alternative brought a process to run, a port to bind, credentials to manage, and a new single
point of failure. `rename()` is atomic on POSIX, a crashed writer leaves only an orphaned `.tmp`, and a
directory tree in iCloud made two machines share one bus for free. The bus won a latency and crash-survival
shootout against brokered alternatives before it was chosen.

## Run

```sh
python3 -c "import comms; print(comms.new_event_id())"
```

`agent_base.py` needs `comms.py` and the bead-ledger module `bd`; the included `bd.py` is a labelled no-op
stub standing in for the private ledger. Subclass `BaseAgent`, override `handle()` and `idle_cycle()`, call `run()`.
