"""Interactive manual runner — lets users pick tasks from schedule.yaml and run them sequentially."""

import datetime as dt
import json
import re
import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEDULE = ROOT / "schedule.yaml"
STATE_FILE = ROOT / "sequencer_state.json"


def _find_python(script_path: Path) -> str:
    """Resolve the best Python interpreter for a script.

    Priority: subproject .venv > root .venv > system python.
    """
    # Check if script lives in a subproject with its own venv
    script_dir = script_path.resolve().parent
    check = script_dir
    while check != ROOT and check != check.parent:
        venv_py = check / ".venv" / "Scripts" / "python.exe"
        if venv_py.is_file():
            return str(venv_py)
        check = check.parent

    # Fallback to root venv
    root_venv = ROOT / ".venv" / "Scripts" / "python.exe"
    if root_venv.is_file():
        return str(root_venv)

    return sys.executable


def parse_tasks() -> list[dict]:
    """Parse schedule.yaml with regex (no PyYAML dependency)."""
    if not SCHEDULE.is_file():
        print(f"[Error] {SCHEDULE} not found.")
        sys.exit(1)

    text = SCHEDULE.read_text(encoding="utf-8")

    # Remove full-line comments (lines starting with #)
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]
    text = "\n".join(lines)

    # Find the tasks: block
    match = re.search(r"^tasks\s*:", text, re.MULTILINE)
    if not match:
        print("[Error] No 'tasks:' section found in schedule.yaml")
        sys.exit(1)

    tasks_block = text[match.end():]

    # Split into individual task entries (each starts with "- ")
    entries = re.split(r"\n\s*-\s+", tasks_block)
    tasks = []
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        id_match = re.search(r'id\s*:\s*["\']?(.+?)["\']?\s*$', entry, re.MULTILINE)
        path_match = re.search(r'path\s*:\s*["\']?(.+?)["\']?\s*$', entry, re.MULTILINE)
        if id_match and path_match:
            dep_match = re.search(r'depends_on\s*:\s*["\']?(.+?)["\']?\s*$', entry, re.MULTILINE)
            deps = []
            if dep_match:
                deps = [d.strip() for d in dep_match.group(1).split(",") if d.strip()]
            tasks.append({"id": id_match.group(1), "path": path_match.group(1), "depends_on": deps})

    return tasks


def _topo_sort(tasks: list[dict]) -> list[dict]:
    """Topological sort so dependencies run before dependents."""
    by_id = {t["id"]: t for t in tasks}
    order = []
    visited: set[str] = set()

    def visit(tid: str):
        if tid in visited:
            return
        visited.add(tid)
        task = by_id.get(tid)
        if not task:
            return
        for dep in task["depends_on"]:
            if dep in by_id:
                visit(dep)
        order.append(task)

    for t in tasks:
        visit(t["id"])
    return order


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            text = STATE_FILE.read_text(encoding="utf-8").strip()
            return json.loads(text) if text else {}
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _update_state(task_id: str, ok: bool) -> None:
    """Record task outcome in sequencer_state.json."""
    state = _load_state()
    slots = state.setdefault("last_triggered_slot", {})
    slots[task_id] = {
        "slot": "manual",
        "last_run": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "outcome": "success" if ok else "failure",
        "retry_count": 0,
    }
    _save_state(state)


def run_script(task_id: str, script_path: Path) -> bool:
    """Run a single script and return True on success."""
    python = _find_python(script_path)
    print(f"\n{'='*60}")
    print(f"  Running: {task_id}")
    print(f"  Script:  {script_path}")
    print(f"  Python:  {python}")
    print(f"{'='*60}\n")

    try:
        result = subprocess.run(
            [python, str(script_path)],
            cwd=str(script_path.parent),
        )
        if result.returncode == 0:
            print(f"\n  [OK] {task_id} finished successfully.")
            return True
        else:
            print(f"\n  [FAIL] {task_id} exited with code {result.returncode}.")
            return False
    except FileNotFoundError:
        print(f"\n  [ERROR] Python not found: {python}")
        return False
    except Exception as exc:
        print(f"\n  [ERROR] {task_id}: {exc}")
        return False


def main():
    tasks = parse_tasks()
    if not tasks:
        print("No tasks found in schedule.yaml.")
        sys.exit(0)

    # Display menu
    print()
    print("=" * 60)
    print("  MANUAL SCRIPT RUNNER")
    print("=" * 60)
    print()
    for i, t in enumerate(tasks, 1):
        script = ROOT / t["path"]
        exists = "  " if script.is_file() else " [MISSING]"
        dep_info = f"  (after: {', '.join(t['depends_on'])})" if t["depends_on"] else ""
        print(f"  {i}. {t['id']:<30} {t['path']}{exists}{dep_info}")

    print()
    print(f"  A. Run ALL ({len(tasks)} tasks sequentially)")
    print(f"  Q. Quit")
    print()

    choice = input("  Choose tasks (e.g. 1,3 or A for all): ").strip()

    if not choice or choice.upper() == "Q":
        print("  Cancelled.")
        return

    # Determine which tasks to run
    if choice.upper() == "A":
        selected = list(range(len(tasks)))
    else:
        selected = []
        for part in choice.split(","):
            part = part.strip()
            if part.isdigit():
                idx = int(part) - 1
                if 0 <= idx < len(tasks):
                    selected.append(idx)
                else:
                    print(f"  [Warning] #{part} is out of range, skipping.")
            else:
                print(f"  [Warning] '{part}' is not a valid number, skipping.")

    if not selected:
        print("  No valid tasks selected.")
        return

    # Verify scripts exist before running
    missing = []
    for idx in selected:
        script = ROOT / tasks[idx]["path"]
        if not script.is_file():
            missing.append(tasks[idx])

    if missing:
        print()
        for t in missing:
            print(f"  [WARNING] Script not found: {t['path']}")
        proceed = input("\n  Continue anyway? [y/N]: ").strip()
        if proceed.upper() != "Y":
            print("  Cancelled.")
            return

    # Topologically sort selected tasks so dependencies run first
    selected_tasks = _topo_sort([tasks[idx] for idx in selected])

    # Also pull in any unselected dependencies that are needed
    selected_ids = {t["id"] for t in selected_tasks}
    all_by_id = {t["id"]: t for t in tasks}
    extras = []
    for t in selected_tasks:
        for dep in t["depends_on"]:
            if dep not in selected_ids and dep in all_by_id:
                extras.append(all_by_id[dep])
                selected_ids.add(dep)
    if extras:
        print()
        for e in extras:
            print(f"  [Auto-added dependency] {e['id']}")
        selected_tasks = _topo_sort(extras + selected_tasks)

    # Run sequentially, skipping tasks whose dependencies failed
    results = []
    succeeded: set[str] = set()
    for t in selected_tasks:
        script = ROOT / t["path"]
        # Check if all dependencies succeeded
        blocked_by = [d for d in t["depends_on"] if d in selected_ids and d not in succeeded]
        if blocked_by:
            print(f"\n  [SKIP] {t['id']} — dependency failed: {', '.join(blocked_by)}")
            results.append((t["id"], False))
            _update_state(t["id"], False)
            continue
        ok = run_script(t["id"], script)
        results.append((t["id"], ok))
        _update_state(t["id"], ok)
        if ok:
            succeeded.add(t["id"])

    # Summary
    print()
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    for name, ok in results:
        status = "OK" if ok else "FAIL"
        print(f"  [{status:>4}] {name}")
    print()

    passed = sum(1 for _, ok in results if ok)
    failed = len(results) - passed
    print(f"  {passed} passed, {failed} failed out of {len(results)} total.")
    print()


if __name__ == "__main__":
    main()
