"""Helper for developer_prep.bat — avoids batch escaping issues."""
import re
import sys
from pathlib import Path


def parse_version(pyproject: Path) -> str:
    m = re.search(r'requires-python\s*=\s*"[^0-9]*([0-9]+\.[0-9]+)', pyproject.read_text("utf-8"))
    return m.group(1) if m else ""


def cmd_root_version():
    """Print the Python version from the root pyproject.toml."""
    print(parse_version(Path("pyproject.toml")))


def cmd_all_versions():
    """Print all unique Python versions needed (root + subprojects), one per line."""
    versions = set()
    for p in Path(".").rglob("pyproject.toml"):
        # Skip anything inside .venv, bin, vendor, __pycache__
        parts = p.parts
        if any(skip in parts for skip in (".venv", "bin", "vendor", "__pycache__")):
            continue
        v = parse_version(p)
        if v:
            versions.add(v)
    print("\n".join(sorted(versions)))


def cmd_subprojects():
    """Print subfolder names that have their own pyproject.toml, one per line."""
    for child in sorted(Path(".").iterdir()):
        if child.is_dir() and child.name not in (".venv", "bin", "vendor", "__pycache__", "logs"):
            if (child / "pyproject.toml").exists():
                print(child)


def cmd_subproject_version(folder: str):
    """Print the Python version for a specific subproject."""
    p = Path(folder) / "pyproject.toml"
    if p.exists():
        print(parse_version(p))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "root-version":
        cmd_root_version()
    elif cmd == "all-versions":
        cmd_all_versions()
    elif cmd == "subprojects":
        cmd_subprojects()
    elif cmd == "subproject-version" and len(sys.argv) > 2:
        cmd_subproject_version(sys.argv[2])
    else:
        print(f"Usage: {sys.argv[0]} root-version|all-versions|subprojects|subproject-version <folder>", file=sys.stderr)
        sys.exit(1)
