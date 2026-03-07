# Scheduler

A portable, offline Python task scheduler. Clone the repo, double-click a `.bat` file, and it runs your scripts on a schedule -- forever.

---

## Why This Exists

This project was built around a specific set of constraints:

- **The scheduler laptop has no internet.** It sits on a restrictive company network (no PyPI, no downloads). Everything it needs must be bundled in the repo.
- **The laptop has no Python installed.** No Python, no pip, no libraries. The repo ships its own interpreters and packages.
- **No database is accessible.** IT restrictions block database access, so all state and memory management uses flat JSON files committed to the repo.
- **The scheduler never stops.** It runs 24/7 in daemon mode (a program that loops forever in the background). If the laptop crashes or restarts for an OS update, it recovers automatically -- picking up where it left off using persisted state.
- **The laptop must not overheat.** The scheduler uses zero CPU while idle (event-driven sleep with OS-level blocking waits). It only wakes when a task is due or a command arrives.
- **Developers push code remotely.** They have internet access and push scripts to the remote repo. The scheduler laptop pulls changes on a timer.
- **It must be easy.** Write a script, add one entry to `schedule.yaml`, push. Done.

## Features

- **Fully offline** -- all dependencies pre-downloaded as `.whl` files in `vendor/`, installs without internet
- **Fully portable** -- clone on any Windows laptop and run, nothing to install (Python + package manager bundled)
- **Event-driven sleep** -- calculates next job time and sleeps with zero CPU, wakes instantly on monitor commands via UDP
- **Crash recovery** -- persists state to `sequencer_state.json`, resumes in-progress tasks after restart
- **Smart git sync** -- smart pull (fetch + check, only pulls when remote has changes), instant push after each task completes
- **Parallel execution** -- runs tasks concurrently with auto-profiled CPU/RAM cost balancing
- **Auto-retry with exponential backoff** -- failed tasks retry with doubling delays (60s, 120s, 240s, ..., capped at 30 min)
- **Task dependencies** -- `depends_on` ensures tasks run only after their dependencies succeed
- **Task timeout** -- `timeout_minutes` kills scripts that hang longer than expected
- **Pause/resume from dashboard** -- pause and resume individual tasks from the monitor without editing config
- **Run now from dashboard** -- trigger any task to run immediately from the monitor, regardless of schedule
- **Multi-Python** -- subprojects can use different Python versions and isolated dependencies
- **Live dashboard** -- real-time terminal monitor showing task status, profiling, today's schedule, and git sync state
- **Email alerts** -- optional failure notifications and heartbeat emails

## Quick Start

### For Developers (write and push scripts)

```
git clone <repo URL>
developer_prep.bat              # one-time: vendors wheels + downloads Python
```

Then write your script, add it to `schedule.yaml`, and push:

```
git add .
git commit -m "add my script"
git push
```

### For the Scheduler Laptop (runs scripts)

```
git clone <repo URL>
run_sequencer.bat               # bootstraps automatically, runs forever
```

That's it. No Python install, no pip, no setup.

## Project Structure

```
repo/
  sequencer.py            # The scheduler engine (daemon mode)
  monitor.py              # Live terminal dashboard
  schedule.yaml           # What to run and when
  settings.yaml           # Global config (parallelism, git sync, email)
  pyproject.toml          # Root project dependencies + Python version
  developer_prep.bat      # Dev setup: vendors wheels + downloads Python
  run_sequencer.bat       # Starts the scheduler
  run_monitor.bat         # Starts the dashboard
  bin/
    uv.exe                # Bundled package manager (no Python needed to run it)
    python/               # Bundled portable Python interpreters
  vendor/                 # Pre-downloaded .whl packages (offline install)
  logs/                   # Daily log files (auto-pushed to remote)
  sequencer_state.json    # Runtime state (auto-pushed to remote)
```

Scripts can live at the root (e.g. `test1.py`) or in subprojects with their own dependencies (e.g. `test2_project/`).

## How It Works

```
DEVELOPER                      REMOTE REPO                SCHEDULER LAPTOP
---------                      -----                      ----------------

Write scripts         --->   git push   --->          Smart pull (fetch + check)
Update schedule.yaml                                    Re-sync packages
Vendor new wheels                                       Run tasks on schedule

                                                        Push after each task completes
git pull              <---   git pull   <---          Push state + logs
Check logs/
Check state
```

The scheduler uses event-driven sleep: it calculates exactly when the next task fires and sleeps until then (zero CPU while idle). The monitor communicates with the sequencer via UDP on `127.0.0.1:19876` for instant command delivery. Git uses **smart pull** (fetches first, only pulls when remote has new commits) and **pushes immediately after each task completes** so developers see results in near-real-time. For the full breakdown, see **[HOW_IT_WORKS.md](HOW_IT_WORKS.md)**.

## Requirements

**Just git.** Everything else is in the repo.

| Component | Bundled in repo | Notes |
|-----------|----------------|-------|
| Python    | `bin/python/`  | Portable interpreters, no system install needed |
| Package manager | `bin/uv.exe` | Replaces pip, written in Rust, runs without Python |
| Libraries | `vendor/*.whl` | Pre-downloaded wheels, installed offline |
