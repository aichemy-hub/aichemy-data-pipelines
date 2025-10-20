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
WATCH_DIR = Path(Variable.get("MS_WATCH_DIR", default_var="/data"))
OUTPUT_DIR = Path(Variable.get("MS_OUTPUT_DIR", default_var="/data/mzML"))
FORMAT = Variable.get("MS_FORMAT", default_var="mzML").lower()  # mzml|mzxml
GZIP_OUT = Variable.get("MS_GZIP", default_var="1") in ("1", "true", "True")

QUIET_S = int(Variable.get("MS_QUIET_SECONDS", default_var="20"))
CHECK_INT_S = int(Variable.get("MS_CHECK_INTERVAL", default_var="5"))

ARCHIVE_ORIG = Variable.get("MS_ARCHIVE_ORIGINAL", default_var="1") in (
    "1",
    "true",
    "True",
)
ARCHIVE_DIR = Path(
    Variable.get("MS_ARCHIVE_DIR", default_var=str(OUTPUT_DIR / "archives"))
)
ARCHIVE_GZIP = Variable.get("MS_ARCHIVE_GZIP", default_var="1") in ("1", "true", "True")
DELETE_ORIG = Variable.get("MS_DELETE_ORIG", default_var="1") in ("1", "true", "True")
ARCHIVE_POLICY = Variable.get(
    "MS_ARCHIVE_EXISTS_POLICY", default_var="skip"
).lower()  # skip|replace

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
HOST_DATA_DIR = Path(Variable.get("MS_HOST_DATA_DIR", default_var="/ABS/PATH/TO/host_data"))

RUN_UID = int(Variable.get("MS_RUN_UID", default_var="50000"))
RUN_GID = int(Variable.get("MS_RUN_GID", default_var="0"))

# Logging
log = logging.getLogger("msconvert.archive")

# ---------------------------
# Helpers
# ---------------------------
def ts_utc() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


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


def wait_for_quiet(p: Path, quiet_s: int, check_s: int):
    stable_for, last = 0, -1
    while stable_for < quiet_s:
        size = dir_size_bytes(p)
        if size == last:
            stable_for += check_s
        else:
            stable_for, last = 0, size
        time.sleep(check_s)


def already_converted(base: str) -> bool:
    if not OUTPUT_DIR.exists():
        return False
    exts = ["mzml", "mzML"] if FORMAT == "mzml" else ["mzxml", "mzXML"]
    for e in exts:
        if list(OUTPUT_DIR.glob(f"{base}-*.{e}")) or list(
            OUTPUT_DIR.glob(f"{base}-*.{e}.gz")
        ):
            return True
    return False


def outfile_stem(base: str) -> str:
    return f"{base}-{ts_utc()}"


# ---------------------------
# DAG
# ---------------------------
with DAG(
    dag_id="msconvert_watch_simple",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",  # poll every 2 minutes
    catchup=False,
    max_active_runs=1,  # one discovery cycle at a time
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Discover .d, wait-for-quiet, convert via ProteoWizard, optionally archive originals",
    tags=["mass-spec", "msconvert", "mzml", "mzxml"],
) as dag:

    @task
    def discover_new_runs() -> List[str]:
        WATCH_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        pending = []
        for p in WATCH_DIR.iterdir():
            if p.is_dir() and p.name.endswith(".d"):
                base = p.name[:-2]
                if not already_converted(base):
                    pending.append(str(p))
        pending.sort()
        return pending

    @task
    def wait_until_quiet(dpath: str) -> Dict[str, str]:
        p = Path(dpath)
        if not p.exists():
            raise AirflowSkipException(f"{dpath} disappeared")
        wait_for_quiet(p, QUIET_S, CHECK_INT_S)
        base = p.name[:-2] if p.name.endswith(".d") else p.name
        stem = outfile_stem(base)
        ext = "mzML" if FORMAT == "mzml" else "mzXML"
        outfile = f"{stem}.{ext}{'.gz' if GZIP_OUT else ''}"
        return {
            "IN": dpath,
            "BASE": base,
            "STEM": stem,
            "OUTFILE": outfile,
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
        mounts=[Mount(source=str(HOST_DATA_DIR), target="/data", type="bind", read_only=False)],
        privileged=PRIVILEGED,
        pool=POOL_NAME,
        auto_remove=True,
        user=f"{RUN_UID}:{RUN_GID}",
        command=[
            "bash",
            "-lc",
            """
            set -euo pipefail

            in="$IN"
            stem="$STEM"
            outdir="{{ var.value.get('MS_OUTPUT_DIR', '/data/mzML') }}"
            fmt="{{ 'mzML' if var.value.get('MS_FORMAT', 'mzML').lower()=='mzml' else 'mzXML' }}"
            gzip="{{ var.value.get('MS_GZIP', '1') }}"

            args=()
            if [ "$fmt" = "mzML" ]; then args+=(--mzML); else args+=(--mzXML); fi
            if [ "$gzip" = "1" ] || [ "$gzip" = "true" ] || [ "$gzip" = "True" ]; then args+=(--gzip); fi

            mkdir -p "$outdir"
            echo "Running: wine msconvert \"$in\" ${args[*]} --outdir \"$outdir\" --outfile \"$stem\""
            wine msconvert "$in" "${args[@]}" --outdir "$outdir" --outfile "$stem"
            """,
        ],
    ).expand(
        # Map the operator by providing one environment dict per item
        environment=prepared
    )

    @task
    def archive_original(payload: Dict[str, str]):
        if not ARCHIVE_ORIG:
            log.info("ARCHIVE_ORIG disabled; skipping archive step.")
            return
        dpath = Path(payload["IN"])
        base  = payload["BASE"]
        log.info("Archive task starting | dpath=%s base=%s ARCHIVE_DIR=%s DELETE_ORIG=%s GZIP=%s POLICY=%s",
         dpath, base, ARCHIVE_DIR, DELETE_ORIG, ARCHIVE_GZIP, ARCHIVE_POLICY)
        
        # guard to ensure the expected output exists before archiving
        out = OUTPUT_DIR / payload.get("OUTFILE", "")
        if not out.exists():
            log.warning("Expected output file is missing; will skip archive. out=%s", out)
            return
        else:
            try:
                log.info("Confirmed output exists: %s (size=%d bytes)", out, out.stat().st_size)
            except FileNotFoundError:
                log.warning("Race on output stat; skipping archive. out=%s", out)
                return
        
        # policy: skip or replace prior archives of this base
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        if ARCHIVE_POLICY == "replace":
            log.info("Archive policy=replace: removing any existing archives for base=%s", base)
            for p in list(ARCHIVE_DIR.glob(f"{base}-*.tar")) + list(ARCHIVE_DIR.glob(f"{base}-*.tar.gz")):
                try:
                    p.unlink()
                    log.info("Removed old archive: %s", p)
                except Exception as e:
                    log.warning("Failed to remove existing archive %s: %s", p, e)

        src_bytes = dir_size_bytes(dpath)
        log.info("Archiving source dir %s (size=%.2f MB) into %s", dpath, src_bytes / (1024**2), ARCHIVE_DIR)
        
        mode = "w:gz" if ARCHIVE_GZIP else "w"
        suffix = ".tar.gz" if ARCHIVE_GZIP else ".tar"
        tmp = ARCHIVE_DIR / f"{base}-{ts_utc()}{suffix}.partial"
        final = Path(str(tmp).removesuffix(".partial"))

        try:
            with tarfile.open(tmp, mode) as tf:
                tf.add(dpath, arcname=dpath.name)
            tmp.replace(final)
            arc_size = final.stat().st_size
            saved_pct = (1 - (arc_size / src_bytes)) * 100 if src_bytes > 0 else 0.0
            log.info("Archive complete: %s (%.2f MB). Saved ~%.1f%% vs source.",
                    final, arc_size / (1024**2), saved_pct)

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
