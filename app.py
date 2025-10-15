#!/usr/bin/env python3
import os
import sys
import time
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

# -----------------------------
# Config via environment
# -----------------------------
WATCH_DIR = Path(os.getenv("WATCH_DIR", "/data"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/data/mzML"))

# How long bytes must remain unchanged before we consider a directory "quiet"
QUIET_SECONDS = int(os.getenv("QUIET_SECONDS", "20"))
# How often we re-check byte size while waiting
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "5"))

# Bootstrap pass over existing *.d at startup (1=yes, 0=no)
BOOTSTRAP = os.getenv("BOOTSTRAP", "1") not in ("0", "false", "False")

# Output format: mzML or mzXML (default mzML)
FORMAT = os.getenv("FORMAT", "mzML").lower()
if FORMAT not in {"mzml", "mzxml"}:
    print("ERROR: FORMAT must be 'mzML' or 'mzXML'", file=sys.stderr)
    sys.exit(2)

# Compress output (1=yes, 0=no)
GZIP = os.getenv("GZIP", "1") not in ("0", "false", "False")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("pwiz-watchdog")

# Debounce tracker to avoid duplicate converts
_pending: Dict[Path, float] = {}


def ts_utc() -> str:
    """Return UTC timestamp like 20251013T141500Z."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def dir_size_bytes(p: Path) -> int:
    """Total apparent size of a directory tree (follows regular files)."""
    total = 0
    if not p.exists():
        return 0
    for root, dirs, files in os.walk(p):
        for fn in files:
            try:
                fp = Path(root) / fn
                total += fp.stat().st_size
            except FileNotFoundError:
                # Race with writer; ignore
                pass
    return total


def wait_for_quiet(dpath: Path, quiet_s: int, interval_s: int) -> None:
    """Block until directory size is unchanged for quiet_s seconds."""
    log.info("Waiting for directory to go quiet: %s", dpath)
    stable_for = 0
    last_size = -1
    while True:
        size = dir_size_bytes(dpath)
        if size == last_size:
            stable_for += interval_s
        else:
            stable_for = 0
            last_size = size

        if stable_for >= quiet_s:
            log.info("Directory is quiet: %s (stable %ss)", dpath, quiet_s)
            return
        time.sleep(interval_s)


def output_name(base: str) -> str:
    """Construct output *stem* with timestamp (no extension)."""
    return f"{base}-{ts_utc()}"


def already_converted(base: str) -> bool:
    """Best-effort check for any existing output for this base, case-insensitive on extension."""
    if not OUTPUT_DIR.exists():
        return False
    if FORMAT == "mzml":
        exts = ["mzml", "mzML"]
    else:
        exts = ["mzxml", "mzXML"]
    for ext in exts:
        if list(OUTPUT_DIR.glob(f"{base}-*.{ext}")):
            return True
        if list(OUTPUT_DIR.glob(f"{base}-*.{ext}.gz")):
            return True
    return False


def msconvert(dpath: Path, outfile_stem: str) -> int:
    """Run msconvert for a .d directory; outfile_stem has no extension.
    msconvert will add .mzML/.mzXML and .gz automatically when requested.
    """
    fmt_flag = "--mzML" if FORMAT == "mzml" else "--mzXML"
    predicted_ext = "mzML" if FORMAT == "mzml" else "mzXML"
    predicted_name = f"{outfile_stem}.{predicted_ext}{'.gz' if GZIP else ''}"
    predicted_path = OUTPUT_DIR / predicted_name

    cmd = [
        "wine",
        "msconvert",
        str(dpath),
        fmt_flag,
        "--outdir",
        str(OUTPUT_DIR),
        "--outfile",
        outfile_stem,
    ]
    if GZIP:
        cmd.append("--gzip")

    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log.error(
            "msconvert failed (%s):\nSTDOUT:\n%s\nSTDERR:\n%s",
            result.returncode,
            result.stdout,
            result.stderr,
        )
    else:
        log.info("msconvert OK. Output (predicted): %s", predicted_path)

    return result.returncode


def convert_dir(dpath: Path) -> None:
    """Convert one .d directory if not already done."""
    if not dpath.exists():
        log.warning("Path disappeared before convert: %s", dpath)
        return

    base = dpath.name[:-2] if dpath.name.endswith(".d") else dpath.name
    if already_converted(base):
        log.info("Skipping: output for '%s' already exists.", base)
        return

    # Wait for quiet to avoid partial files
    wait_for_quiet(dpath, QUIET_SECONDS, CHECK_INTERVAL)

    # Ensure output dir
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    outfile_stem = output_name(base)

    rc = msconvert(dpath, outfile_stem)
    if rc == 0:
        final_ext = "mzML" if FORMAT == "mzml" else "mzXML"
        final_name = f"{outfile_stem}.{final_ext}{'.gz' if GZIP else ''}"
        log.info("Done: %s", OUTPUT_DIR / final_name)
    else:
        log.error("Conversion failed for: %s", dpath)


def handle_candidate(path: Path) -> None:
    """Debounce and schedule a convert for a candidate .d dir."""
    if not path.name.endswith(".d"):
        return
    if not path.is_dir():
        return

    # Debounce: if we saw it in the last few seconds, ignore duplicate event
    now = time.time()
    last = _pending.get(path, 0)
    if now - last < 2.0:
        return
    _pending[path] = now

    log.info("Detected .d directory: %s", path)
    try:
        convert_dir(path)
    finally:
        _pending.pop(path, None)


class DWatcher(FileSystemEventHandler):
    def on_created(self, event: FileCreatedEvent):
        # Top-level only; we care about directories ending with .d
        p = Path(event.src_path)
        if p.parent == WATCH_DIR:
            handle_candidate(p)

    def on_moved(self, event: FileMovedEvent):
        # A move into the watch dir counts as created
        p = Path(event.dest_path)
        if p.parent == WATCH_DIR:
            handle_candidate(p)


def bootstrap_scan():
    log.info("Bootstrap scanning %s for *.d", WATCH_DIR)
    for p in WATCH_DIR.iterdir():
        if p.is_dir() and p.name.endswith(".d"):
            handle_candidate(p)


def main():
    if not WATCH_DIR.exists():
        log.info("Creating watch dir: %s", WATCH_DIR)
        WATCH_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== ProteoWizard auto-converter (Python watchdog) ===")
    log.info(
        "Watch: %s | Output: %s | Quiet: %ss | Interval: %ss | GZIP: %s | FORMAT: %s",
        WATCH_DIR,
        OUTPUT_DIR,
        QUIET_SECONDS,
        CHECK_INTERVAL,
        GZIP,
        FORMAT.upper(),
    )

    if BOOTSTRAP:
        bootstrap_scan()

    event_handler = DWatcher()
    observer = Observer()
    # Top-level watch only (no recursion): we only care about *.d dropped into WATCH_DIR
    observer.schedule(event_handler, str(WATCH_DIR), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping watcher...")
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()
