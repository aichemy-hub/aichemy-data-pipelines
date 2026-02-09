from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import os, time, tarfile, shutil
from typing import List, Dict
import logging
import contextlib

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ---------------------------
# Config via Airflow Variables (Admin → Variables)
# ---------------------------

# Key configuration parameters
HOST_DATA_DIR = Path(
    Variable.get("MS_HOST_DATA_DIR", default_var="/ABS/PATH/TO/host_data")
)
WATCH_DIR = Path(Variable.get("MS_WATCH_DIR", default_var="/data"))
OUTPUT_DIR = Path(Variable.get("MS_OUTPUT_DIR", default_var="/data/mzML"))
ARCHIVE_DIR = Path(
    Variable.get("MS_ARCHIVE_DIR", default_var=str(WATCH_DIR / "archives"))
)
FORMAT = Variable.get("MS_FORMAT", default_var="mzML").lower()  # mzml|mzxml

# Fine-tuning options for file handling
GZIP_OUT = Variable.get("MS_GZIP", default_var="1") in ("1", "true", "True")
ARCHIVE_ORIG = Variable.get("MS_ARCHIVE_ORIGINAL", default_var="1") in (
    "1",
    "true",
    "True",
)
ARCHIVE_GZIP = Variable.get("MS_ARCHIVE_GZIP", default_var="1") in ("1", "true", "True")
DELETE_ORIG = Variable.get("MS_DELETE_ORIG", default_var="1") in ("1", "true", "True")
ARCHIVE_POLICY = Variable.get(
    "MS_ARCHIVE_EXISTS_POLICY", default_var="skip"
).lower()  # skip|replace

# Fine-tuning options for other behaviors
QUIET_S = int(Variable.get("MS_QUIET_SECONDS", default_var="120"))
CHECK_INT_S = int(Variable.get("MS_CHECK_INTERVAL", default_var="5"))
PWIZ_IMAGE = Variable.get(
    "MS_PWIZ_IMAGE",
    default_var="proteowizard/pwiz-skyline-i-agree-to-the-vendor-licenses:latest",
)
PRIVILEGED = Variable.get("MS_DOCKER_PRIVILEGED", default_var="true").lower() in (
    "1",
    "true",
    "yes",
)
POOL_NAME = Variable.get("MS_POOL", default_var="msconvert")  # controls concurrency
RUN_UID = int(Variable.get("MS_RUN_UID", default_var="50000"))
RUN_GID = int(Variable.get("MS_RUN_GID", default_var="0"))
HOST_WINE_CACHE = Path(
    Variable.get("MS_HOST_WINECACHE_DIR", default_var="/var/lib/msconvert/wineprefix64")
)

# Logging
log = logging.getLogger("msconvert.archive")


# ---------------------------
# Helpers
# ---------------------------
def ts_utc() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


# Get total size of all files in a directory tree
def dir_size_bytes(p: Path) -> int:
    tot = 0
    if not p.exists():
        return 0
    for root, _, files in os.walk(p):
        for fn in files:
            try:
                tot += (Path(root) / fn).stat().st_size
            except FileNotFoundError:
                pass
    return tot


# Wait until directory size is stable for quiet_s seconds
def wait_for_quiet(p: Path, quiet_s: int, check_s: int):
    stable_for, last = 0, -1
    while stable_for < quiet_s:
        size = dir_size_bytes(p)
        if size == last:
            stable_for += check_s
        else:
            stable_for, last = 0, size
        time.sleep(check_s)


# Check if a base name has already been converted (within a plate subdirectory)
def already_converted(base: str, plate_rel: Path) -> bool:
    outdir = OUTPUT_DIR / plate_rel
    if not outdir.exists():
        return False
    exts = ["mzml", "mzML"] if FORMAT == "mzml" else ["mzxml", "mzXML"]
    for e in exts:
        if list(outdir.glob(f"{base}-*.{e}")) or list(outdir.glob(f"{base}-*.{e}.gz")):
            return True
    return False


# Generate output file stem with timestamp
def outfile_stem(base: str) -> str:
    return f"{base}-{ts_utc()}"


# ---------------------------
# DAG
# ---------------------------
with DAG(
    dag_id="msconvert_watch_simple",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",  # poll every 5 minutes
    catchup=False,
    max_active_runs=1,  # one discovery cycle at a time
    default_args={"retries": 0},
    description="Discover .d, wait-for-quiet, convert via ProteoWizard, optionally archive originals",
    tags=["mass-spec", "msconvert", "mzml", "mzxml"],
) as dag:

    @task
    def discover_new_runs() -> List[str]:
        """
        Discover new runs in a one-level plate layout:

          WATCH_DIR/
            <plate dir>/
              *.d/

        Plate directory names may contain spaces.
        """
        WATCH_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

        pending: List[str] = []

        # Only scan one level of plate directories under WATCH_DIR.
        for plate in WATCH_DIR.iterdir():
            if not plate.is_dir():
                continue

            # Avoid scanning output/archive trees if they live under WATCH_DIR
            if plate.name in (OUTPUT_DIR.name, ARCHIVE_DIR.name):
                continue

            plate_rel = plate.relative_to(WATCH_DIR)

            # Runs are directories ending with `.d` directly inside the plate directory.
            for run in plate.iterdir():
                if not (run.is_dir() and run.name.endswith(".d")):
                    continue

                base = run.name[:-2]
                if not already_converted(base, plate_rel):
                    pending.append(str(run))

        pending.sort()
        log.info("Discovered new runs: %s", pending)
        return pending

    @task
    def wait_until_quiet(dpath: str) -> Dict[str, str]:
        p = Path(dpath)
        if not p.exists():
            log.warning("Watch directory disappeared: %s", dpath)
            raise AirflowSkipException(f"{dpath} disappeared")
        wait_for_quiet(p, QUIET_S, CHECK_INT_S)
        base = p.name[:-2] if p.name.endswith(".d") else p.name
        stem = outfile_stem(base)
        ext = "mzML" if FORMAT == "mzml" else "mzXML"
        outfile = f"{stem}.{ext}{'.gz' if GZIP_OUT else ''}"
        plate_rel = p.parent.relative_to(WATCH_DIR)
        outdir = OUTPUT_DIR / plate_rel
        return {
            "IN": dpath,
            "BASE": base,
            "STEM": stem,
            "OUTFILE": outfile,
            "PLATE_REL": plate_rel.as_posix(),
            "OUTDIR": str(outdir),
            "WINEDEBUG": "-all",
        }

    discovered = discover_new_runs()
    prepared = wait_until_quiet.expand(dpath=discovered)

    # One conversion task per directory using DockerOperator
    convert = DockerOperator.partial(
        task_id="convert_one",
        image=PWIZ_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=str(HOST_DATA_DIR), target="/data", type="bind", read_only=False
            ),
            Mount(
                source=str(HOST_WINE_CACHE),
                target="/wineprefix_cached",
                type="bind",
                read_only=False,
            ),
        ],
        privileged=PRIVILEGED,
        pool=POOL_NAME,
        auto_remove="success",
        user=f"{RUN_UID}:{RUN_GID}",
        command=[
            "bash",
            "-lc",
            """
            set -euo pipefail

            in="$IN"
            stem="$STEM"
            outdir="$OUTDIR"
            fmt="{{ 'mzML' if var.value.get('MS_FORMAT', 'mzML').lower()=='mzml' else 'mzXML' }}"
            gzip="{{ var.value.get('MS_GZIP', '1') }}"

            # Use the preseeded prefix mounted at /wineprefix_cached
            export WINEARCH=win64
            export WINEDEBUG=-all
            export WINEPREFIX="/wineprefix_cached"

            # Writable HOME to avoid touching /root (cheap, local tmp)
            export HOME="/tmp/home_${STEM}"
            mkdir -p "$HOME"

            # Make sure prefix is valid (idempotent, cheap)
            wineboot -u || true

            # msconvert.exe lives inside the cached prefix we mounted
            MS_EXE="/wineprefix_cached/drive_c/pwiz/msconvert.exe"
            if [ ! -f "$MS_EXE" ]; then
            echo "ERROR: msconvert.exe not found at $MS_EXE"
            ls -l /wineprefix_cached/drive_c/pwiz 2>/dev/null || true
            exit 1
            fi

            args=()
            if [ "$fmt" = "mzML" ]; then args+=(--mzML); else args+=(--mzXML); fi
            if [ "$gzip" = "1" ] || [ "$gzip" = "true" ] || [ "$gzip" = "True" ]; then args+=(--gzip); fi

            mkdir -p "$outdir"
            ls -ld "$in" || { echo "ERROR: input not readable: $in"; exit 1; }

            echo "Running: wine \"$MS_EXE\" \"$in\" ${args[*]} --outdir \"$outdir\" --outfile \"$stem\""
            wine "$MS_EXE" "$in" "${args[@]}" --outdir "$outdir" --outfile "$stem"
            """,
        ],
    ).expand(
        # Map the operator by providing one environment dict per item
        environment=prepared
    )

    @task
    def archive_original(payload: Dict[str, str]):
        if not ARCHIVE_ORIG:
            log.debug("ARCHIVE_ORIG disabled; skipping archive step.")
            return
        dpath = Path(payload["IN"])
        base = payload["BASE"]
        log.info(
            "Archive task starting | dpath=%s base=%s ARCHIVE_DIR=%s DELETE_ORIG=%s GZIP=%s POLICY=%s",
            dpath,
            base,
            ARCHIVE_DIR,
            DELETE_ORIG,
            ARCHIVE_GZIP,
            ARCHIVE_POLICY,
        )

        # guard to ensure the expected output exists before archiving
        outdir = Path(payload.get("OUTDIR", str(OUTPUT_DIR)))
        out = outdir / payload.get("OUTFILE", "")
        if not out.exists():
            log.warning("Expected output file is missing; will skip archive. out=%s", out)
            return
        else:
            try:
                log.debug(
                    "Confirmed output exists: %s (size=%d bytes)",
                    out,
                    out.stat().st_size,
                )
            except FileNotFoundError:
                log.warning("Race on output stat; skipping archive. out=%s", out)
                return

        plate_rel = Path(payload.get("PLATE_REL", "."))
        plate_archive_dir = ARCHIVE_DIR / plate_rel

        # policy: skip or replace prior archives of this base
        plate_archive_dir.mkdir(parents=True, exist_ok=True)
        if ARCHIVE_POLICY == "replace":
            log.debug(
                "Archive policy=replace: removing any existing archives for base=%s",
                base,
            )
            for p in list(plate_archive_dir.glob(f"{base}-*.tar")) + list(
                plate_archive_dir.glob(f"{base}-*.tar.gz")
            ):
                try:
                    p.unlink()
                    log.debug("Removed old archive: %s", p)
                except Exception as e:
                    log.warning("Failed to remove existing archive %s: %s", p, e)

        src_bytes = dir_size_bytes(dpath)
        log.debug(
            "Archiving source dir %s (size=%.2f MB) into %s",
            dpath,
            src_bytes / (1024**2),
            plate_archive_dir,
        )

        mode = "w:gz" if ARCHIVE_GZIP else "w"
        suffix = ".tar.gz" if ARCHIVE_GZIP else ".tar"
        tmp = plate_archive_dir / f"{base}-{ts_utc()}{suffix}.partial"
        final = Path(str(tmp).removesuffix(".partial"))

        try:
            with tarfile.open(tmp, mode) as tf:
                tf.add(dpath, arcname=dpath.name)
            tmp.replace(final)
            arc_size = final.stat().st_size
            saved_pct = (1 - (arc_size / src_bytes)) * 100 if src_bytes > 0 else 0.0
            log.debug(
                "Archive complete: %s (%.2f MB). Saved ~%.1f%% vs source.",
                final,
                arc_size / (1024**2),
                saved_pct,
            )

            if DELETE_ORIG:
                try:
                    shutil.rmtree(dpath, ignore_errors=False)
                    log.info("Deleted original directory after archive: %s", dpath)
                except Exception as e:
                    log.warning("Failed to delete original %s: %s", dpath, e)
        except Exception as e:
            log.exception("Archiving failed for %s -> %s: %s", dpath, final, e)
            with contextlib.suppress(Exception):
                tmp.unlink(missing_ok=True)
            # Let the task fail so we can see it in the UI
            raise

    finalized = archive_original.expand(payload=prepared)

    prepared >> convert >> finalized
