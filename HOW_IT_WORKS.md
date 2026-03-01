# How the Scheduler Works

There are two sides to this system: **developers** who write and push scripts, and a **scheduler laptop** that pulls and runs them on a schedule. Both sides have **git installed** and a **cloned copy of the repo** hosted on **ASICA**.

---

## The Repo (on ASICA)

The repo is the single source of truth. It contains:

```
repo/
  sequencer.py          # The scheduler engine
  monitor.py            # Live dashboard
  schedule.yaml         # What to run and when
  settings.yaml         # Global config (parallelism, git intervals, email)
  pyproject.toml        # Root project dependencies (see "Why pyproject.toml?" below)
  developer_prep.bat    # One-time dev setup script
  run_sequencer.bat     # Starts the scheduler
  run_monitor.bat       # Starts the dashboard
  bin/
    uv.exe              # Bundled package manager (no pip needed)
    python/             # Bundled Python interpreters (portable)
  vendor/               # Pre-downloaded .whl packages (offline install)
  logs/                 # Daily log files (auto-pushed by scheduler)
  sequencer_state.json  # Runtime state (auto-pushed by scheduler)
  test1.py              # Example task
  test2_project/        # Example subproject with its own dependencies
    test2.py
    pyproject.toml
    vendor/
```

---

## Side A: The Developer (Coder)

Developers write Python scripts, configure when they run, and push to ASICA. They never need to run the scheduler themselves.

### First-Time Setup

1. Clone the repo from ASICA:
   ```
   git clone <ASICA repo URL>
   ```

2. Run the prep script (only needed once, or when adding dependencies):
   ```
   developer_prep.bat
   ```
   This script:
   - Uses the bundled `bin/uv.exe` (no global Python/pip required)
   - Reads `pyproject.toml` to find the required Python version
   - Downloads portable Python interpreters into `bin/python/`
   - Downloads all dependency wheels into `vendor/` (and subproject `vendor/` folders)
   - Everything is committed to the repo so the scheduler laptop can install offline

### Daily Workflow

1. **Write your script** -- any Python file. It can use libraries listed in `pyproject.toml`.

2. **If your script needs a new library**, add it to `pyproject.toml` and re-run:
   ```
   developer_prep.bat
   ```
   This re-vendors the wheels so they're available offline on the scheduler laptop.

3. **If your script is a subproject** (its own folder with its own `pyproject.toml` and `vendor/`), the prep script handles it automatically. Subprojects can have different Python versions and dependencies.

4. **Add your script to `schedule.yaml`**:
   ```yaml
   tasks:
     - id: "My Daily Report"
       path: "scripts/daily_report.py"
       week_day: 1,2,3,4,5        # Mon-Fri
       start_hour: 9
       frequency_min: 60           # Every hour
       end_hour: 17
   ```

   Available scheduling options:
   | Field           | Description                                      |
   |-----------------|--------------------------------------------------|
   | `id`            | Unique name for the task                         |
   | `path`          | Path to the Python script (relative to repo root)|
   | `month`         | 1-12, comma-separated (default: all)             |
   | `month_day`     | 1-31, comma-separated (default: all)             |
   | `week_day`      | 1-7 where 1=Mon, 7=Sun (ignored if month_day set)|
   | `start_hour`    | 0-23 (if blank, runs every tick on matching days)|
   | `start_minute`  | 0-59 (default: 0)                                |
   | `frequency_min` | Repeat every N minutes (if blank, runs once)     |
   | `end_hour`      | 0-23 (if omitted, repeats non-stop)              |
   | `end_minute`    | 0-59 (default: 0)                                |

5. **Push to ASICA**:
   ```
   git add .
   git commit -m "add daily report script"
   git push
   ```
   The scheduler laptop will pick up changes on its next `git pull` cycle.

### What the Developer Commits

- Their Python scripts
- `schedule.yaml` (updated with new tasks)
- `pyproject.toml` (if dependencies changed)
- `vendor/*.whl` files (so the scheduler can install offline)
- `bin/python/` (portable interpreters, if a new version was needed)

### What the Developer Does NOT Touch

- `sequencer_state.json` -- managed by the scheduler
- `logs/` -- written by the scheduler
- `settings.yaml` -- usually set once, rarely changed

---

## Side B: The Scheduler Laptop

The scheduler laptop runs the sequencer 24/7. It does not need Python or any libraries pre-installed -- everything is bundled in the repo.

### First-Time Setup

1. Clone the repo from ASICA:
   ```
   git clone <ASICA repo URL>
   ```

2. Double-click `run_sequencer.bat`. That's it.

   On first launch, the sequencer automatically:
   - Creates a `.venv` using the bundled `bin/uv.exe` and `bin/python/` interpreters
   - Installs all dependencies from `vendor/*.whl` files (fully offline, no internet needed)
   - Does the same for every subproject that has its own `pyproject.toml`
   - Starts the daemon loop

### What Happens When the Scheduler Runs

The sequencer runs in **daemon mode** (`sequencer.py --daemon`), which means it never stops. Here is what happens on each cycle:

```
   START
     |
     v
  [Bootstrap]
  Create .venv if missing, install vendored wheels
  Do the same for each subproject
     |
     v
  +--------------------------+
  |   EVERY MINUTE TICK      | <-- loops forever
  |                          |
  |  1. Git Pull Check       |
  |     Every N minutes (default 10), or immediately
  |     if a .git_pull_now trigger file exists:
  |       - git pull from ASICA
  |       - Re-sync vendored packages into .venv
  |         (picks up any new wheels the devs pushed)
  |                          |
  |  2. Reload Config        |
  |     Re-read schedule.yaml and settings.yaml
  |     (picks up any new tasks the devs added)
  |                          |
  |  3. Run Scheduler Pass   |
  |     For each task in schedule.yaml:
  |       - Check if current time matches the schedule
  |       - Check if it already ran in this time slot
  |       - If it should run: execute the script
  |         - Find the right Python interpreter
  |           (subproject venv > root venv > system)
  |         - Run via subprocess, capture output
  |         - Profile CPU% and RAM% usage
  |         - Log result (success/failure) to daily log
  |         - If failed: auto-retry after 60 seconds
  |                          |
  |  4. Git Push Check       |
  |     Every N minutes (default 10), or immediately
  |     if a .git_push_now trigger file exists:
  |       - git add sequencer_state.json logs/
  |       - git commit -m "auto: sequencer state + logs"
  |       - git push to ASICA
  |                          |
  |  5. Wait for next minute |
  +--------------------------+
```

### Parallel Execution

When `use_workers: 1` in `settings.yaml`, tasks run in parallel using a thread pool. Each task has a **worker cost** (learned from CPU/RAM profiling). The total budget defaults to `(CPU_cores - 2) * 100`. For example on an 8-core laptop: budget = 600. A task that uses ~1 core costs 100, so up to 6 such tasks can run simultaneously.

### What the Scheduler Pushes Back to ASICA

Only two things:
- `sequencer_state.json` -- task run times, profiling data, in-progress state
- `logs/` -- daily log files with task output and timestamps

This lets developers check task results by pulling from ASICA.

### The Monitor Dashboard

Optionally, open a second terminal and run `run_monitor.bat` to see a live dashboard showing:
- Task statuses (running, succeeded, failed, waiting)
- CPU/RAM profiling per task
- Git pull/push timestamps
- Interactive controls: press `p` to force a pull, `u` to force a push, `h` for help

### Running on Any Laptop

The scheduler is fully portable. To move it to a different laptop:

1. Clone the repo from ASICA
2. Run `run_sequencer.bat`

No installs needed. The repo carries:
- `bin/uv.exe` -- package manager
- `bin/python/` -- Python interpreters
- `vendor/` -- all dependency wheels

The bootstrap runs automatically on first launch.

---

## The Two-Way Git Flow

```
  DEVELOPER                    ASICA                    SCHEDULER LAPTOP
  ---------                    -----                    ----------------

  Write scripts       --->   git push   --->        git pull (every 10 min)
  Update schedule.yaml                                Re-sync packages
  Update pyproject.toml                               Run tasks on schedule
  Vendor new wheels

                                                      git push (every 10 min)
  git pull            <---   git pull   <---        Push state + logs
  Check logs/
  Check state
```

Developers push **code, config, and wheels**. The scheduler pushes back **state and logs**. Both sides stay in sync through ASICA.

---

## Why `pyproject.toml`?

`pyproject.toml` is the Python standard (PEP 621) for declaring project metadata and dependencies. We use it instead of a plain `requirements.txt` because it also carries the required Python version (`requires-python = ">=3.12"`). The bootstrap reads this to pick the right bundled interpreter from `bin/python/`. A `requirements.txt` cannot do that.

## Why `.yaml` for config?

YAML is human-friendly. `schedule.yaml` and `settings.yaml` are meant to be edited by developers by hand. YAML supports comments (lines starting with `#`), which lets us document scheduling options right inside the file. JSON does not allow comments, making it a poor choice for config files that humans need to read and edit.

## Why `.json` for state?

`sequencer_state.json` is never edited by humans -- it is read and written by the sequencer programmatically. JSON is the natural choice here because Python's built-in `json` module handles it with no extra dependencies, and it round-trips data types (numbers, booleans, lists) without ambiguity. It is also easy to inspect when debugging.

## Why `.bat` scripts?

The goal is to run the scheduler on any Windows laptop with zero setup. `.bat` files are native to Windows -- double-click to run, no interpreter needed. They handle the bootstrapping (finding `uv.exe`, creating `.venv`, launching the sequencer) so that the user never has to open a terminal or type commands.

## What is `vendor/`?

`vendor/` holds pre-downloaded `.whl` (wheel) files -- Python packages in their installable form. When a developer runs `developer_prep.bat`, it downloads every dependency listed in `pyproject.toml` as a `.whl` file into `vendor/`. These wheels are committed to the repo.

This is what makes the scheduler laptop work **without internet**. On first launch (or after a `git pull` brings new wheels), the bootstrap installs packages from `vendor/` using `uv pip install --no-index --find-links vendor/` -- fully offline, no PyPI access needed.

Subprojects can have their own `vendor/` folder (e.g. `test2_project/vendor/`) for dependencies specific to that subproject.

## What is `.venv`?

`.venv` is a **virtual environment** -- an isolated folder where Python and its installed packages live. Each machine (developer laptop, scheduler laptop) creates its own `.venv` locally. It is **not committed to the repo** because:

1. It contains compiled files and symlinks that are tied to the specific machine and OS
2. It is large and would bloat the repo unnecessarily
3. It can be recreated at any time from `vendor/` + `pyproject.toml`

The bootstrap creates `.venv` automatically on first launch using `uv venv`, then installs packages into it from the vendored wheels. If `.venv` gets deleted or corrupted, just re-run the sequencer or `developer_prep.bat` -- it will be rebuilt from scratch.

## What is `bin/`?

`bin/` contains everything needed to set up Python on a machine that has nothing installed:

- **`uv.exe`** -- explained below in "What is `uv`?"
- **`python/`** -- portable Python interpreters downloaded by `developer_prep.bat`. These are standalone copies of Python that do not require a system-wide install.

We bundle Python inside `bin/python/` because the scheduler laptop **has no Python installed**. When the bootstrap needs to create a `.venv`, `uv` picks the right interpreter from `bin/python/` based on the version declared in `pyproject.toml` (e.g. `>=3.12`). This way the entire toolchain lives inside the repo -- clone and run, nothing else to install.

## What is `uv`?

`uv` (by [Astral](https://github.com/astral-sh/uv)) is a single `.exe` file that replaces three tools Python developers normally install separately:

| Traditional tool | What it does | `uv` equivalent |
|------------------|-------------|------------------|
| `python` installer | Install Python on the system | `uv python install 3.12` |
| `python -m venv` | Create a virtual environment | `uv venv` |
| `pip install` | Install packages | `uv pip install` |

The key advantage: **`uv` itself does not need Python to run**. It is a compiled binary written in Rust. This solves the chicken-and-egg problem -- you normally need Python to install Python packages, but `uv` can do it all from scratch.

In this project, `uv` is used for:
1. **`developer_prep.bat`**: downloads portable Python interpreters into `bin/python/`, then downloads dependency wheels into `vendor/`
2. **Bootstrap (on scheduler startup)**: creates `.venv` using the bundled Python from `bin/python/`, then installs packages from `vendor/` -- all offline, no internet needed

## What is a subproject?

A subproject is a folder inside the repo that has its own `pyproject.toml` (and optionally its own `vendor/`). Example: `test2_project/`.

Use a subproject when your script needs **different dependencies or a different Python version** than the root project. For example, if the root requires Python 3.12 but your script needs a library that only works on 3.11, put it in its own folder with its own `pyproject.toml` declaring `requires-python = ">=3.11"`.

If your script is fine with the root dependencies, just place it next to `test1.py` -- no subproject needed.

The scheduler handles subprojects automatically: it creates a separate `.venv` for each one and uses the correct interpreter when running their scripts.

## What happens when a task fails?

A task "fails" when the Python script exits with a non-zero exit code (e.g. an unhandled exception, `sys.exit(1)`, etc.).

When a task fails:
1. The failure is logged to the daily log file in `logs/`
2. The task is automatically retried after `retry_delay_seconds` (default: 60 seconds)
3. Retries continue indefinitely until the task succeeds or the schedule window ends
4. If `failure_email` is enabled in `settings.yaml`, an email is sent on each failure

Developers can check task results by:
- Pulling from ASICA and reading `logs/`
- Pulling from ASICA and checking `sequencer_state.json` for task statuses
- Running `run_monitor.bat` on the scheduler laptop to see live status
