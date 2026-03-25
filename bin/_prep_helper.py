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


def cmd_clean_vendor(vendor_dir: str, requirements_file: str):
    """Remove .whl files from vendor_dir that aren't in requirements_file."""
    vendor = Path(vendor_dir)
    if not vendor.is_dir():
        return

    # Parse required package names + versions from requirements.txt
    # Lines look like: requests==2.31.0
    required = set()
    req_path = Path(requirements_file)
    if not req_path.is_file():
        return
    for line in req_path.read_text("utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        # Normalize: "requests==2.31.0" -> "requests-2.31.0"
        # Wheel filenames use - not _ and lowercase
        name_ver = re.split(r"[;@]", line)[0].strip()  # drop markers
        name_ver = re.sub(r"==", "-", name_ver)
        name_ver = name_ver.replace("_", "-").lower()
        required.add(name_ver)

    # Check each .whl file — name format: {name}-{ver}-{tags}.whl
    removed = 0
    for whl in vendor.glob("*.whl"):
        parts = whl.stem.split("-")
        if len(parts) >= 2:
            whl_name_ver = f"{parts[0]}-{parts[1]}".replace("_", "-").lower()
            if whl_name_ver not in required:
                whl.unlink()
                print(f"  Removed stale: {whl.name}")
                removed += 1
    if removed:
        print(f"  Cleaned {removed} stale wheel(s) from {vendor_dir}")


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
    elif cmd == "clean-vendor" and len(sys.argv) > 3:
        cmd_clean_vendor(sys.argv[2], sys.argv[3])
    else:
        print(f"Usage: {sys.argv[0]} root-version|all-versions|subprojects|subproject-version|clean-vendor", file=sys.stderr)
        sys.exit(1)
