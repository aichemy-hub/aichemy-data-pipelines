from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import os, time, tarfile, shutil
from typing import List, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.providers.docker.operators.docker import DockerOperator

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
        return {"dpath": dpath, "base": base, "stem": stem, "outfile": outfile}

    discovered = discover_new_runs()
    prepared = wait_until_quiet.expand(dpath=discovered)

    # One conversion task per directory using DockerOperator
    convert = DockerOperator.partial(
        task_id="convert_one",
        image=PWIZ_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        volumes=[f"{WATCH_DIR}:/data:rw"],  # host /data into container /data
        environment={"WINEDEBUG": "-all"},
        privileged=PRIVILEGED,
        pool=POOL_NAME,  # cap parallelism via pool size
    ).expand(
        command=lambda ctx: [
            "bash",
            "-lc",
            """
            set -euo pipefail
            in="{{ ti.xcom_pull(task_ids='wait_until_quiet', key='return_value')['dpath'] }}"
            stem="{{ ti.xcom_pull(task_ids='wait_until_quiet', key='return_value')['stem'] }}"
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
        ]
    )

    @task
    def archive_original(payload: Dict[str, str]):
        if not ARCHIVE_ORIG:
            return
        dpath = Path(payload["dpath"])
        base = payload["base"]
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        # policy: skip or replace prior archives of this base
        if ARCHIVE_POLICY == "replace":
            for p in list(ARCHIVE_DIR.glob(f"{base}-*.tar")) + list(
                ARCHIVE_DIR.glob(f"{base}-*.tar.gz")
            ):
                try:
                    p.unlink()
                except Exception:
                    pass

        mode = "w:gz" if ARCHIVE_GZIP else "w"
        suffix = ".tar.gz" if ARCHIVE_GZIP else ".tar"
        tmp = ARCHIVE_DIR / f"{base}-{ts_utc()}{suffix}.partial"
        final = Path(str(tmp).removesuffix(".partial"))

        import contextlib

        try:
            with tarfile.open(tmp, mode) as tf:
                tf.add(dpath, arcname=dpath.name)
            tmp.replace(final)
            if DELETE_ORIG:
                shutil.rmtree(dpath, ignore_errors=False)
        except Exception:
            with contextlib.suppress(Exception):
                tmp.unlink(missing_ok=True)

    finalized = archive_original.expand(payload=prepared)

    prepared >> convert >> finalized
