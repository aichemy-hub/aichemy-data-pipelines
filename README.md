# AIchemy Data Pipelines with Airflow

This repository provides [Apache Airflow](https://airflow.apache.org/)-based data-processing pipelines.

Airflow pipelines are defined as **DAGs** (Directed Acyclic Graphs) in Python, allowing for flexible, modular, and maintainable workflows.

Currently we have one pipeline for automated **mass spectrometry file conversion** using [ProteoWizard](https://proteowizard.sourceforge.io/tools.shtml).  
It is designed to watch a shared storage directory for new raw `.d` directories, convert them to open formats (`mzML` or `mzXML`), and optionally archive the original data.

---

## 🧱 Repository Structure

```plaintext
.
├── dags/                    # All DAGs live here
│   └── msconvert_dag.py     # Main conversion DAG
├── docker-compose.yml       # Airflow deployment configuration
├── Dockerfile               # Optional image for testing / debugging
├── requirements.txt
└── README.md
```

- **`dags/`** – each DAG is a self-contained Python file. Airflow automatically loads valid DAGs from here.  
- **`docker-compose.yml`** – sets up Airflow components and mounts required directories.  
- **`Dockerfile`** – optional base image for testing conversion locally.  
- **`requirements.txt`** – Python dependencies for optional utilities.

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.
- A host directory with incoming mass spectrometry data in `.d` format.
- A .env file to set environment variables needed by Airflow. This needs minimally to have `FERNET_KEY` and `SECRET_KEY` defined.
- A pre-seeded Wine prefix with `msconvert.exe` installed. run:

```bash
docker run --rm -d --name seed_wine proteowizard/pwiz-skyline-i-agree-to-the-vendor-licenses sleep infinity
sudo docker cp seed_wine:/wineprefix64 /var/lib/msconvert/wineprefix64
```

### 1. Start Airflow

```bash
sudo docker compose up -d
```

Then open [http://localhost:8080](http://localhost:8080) to access the Airflow UI.  
(Default credentials are defined in `docker-compose.yml` as `admin` / `admin`.)

### 2. Required Airflow Variables

Before running the DAG, set the following Airflow Variables (in **Admin → Variables** or via CLI):

| Variable Name             | Example Value                              | Description                                   |
|---------------------------|---------------------------------------------|-----------------------------------------------|
| `MS_HOST_DATA_DIR`        | `/mnt/aichemyrds/live/roar_ms_data`         | Host path to the watched data directory       |
| `MS_WATCH_DIR`            | `/data`                                    | Path inside container to watch for `.d` dirs |
| `MS_OUTPUT_DIR`           | `/data/mzML`                               | Output directory for converted files         |
| `MS_ARCHIVE_DIR`          | `/data/archives`                           | Where to store archived originals            |
| `MS_HOST_WINECACHE_DIR`   | `/var/lib/msconvert/wineprefix64`          | Pre-seeded wineprefix with msconvert.exe     |
| `MS_FORMAT`               | `mzML`                                     | Output format (`mzML` or `mzXML`)            |

Other optional variables exist (e.g. concurrency pool, archiving policy).  
Check `dags/msconvert_dag.py` for the full list.

---

## 🧪 Running the Conversion DAG

The main DAG is called **`msconvert_watch_simple`**.

1. Ensure your input data directory (e.g. `/mnt/aichemyrds/live/roar_ms_data`) is mounted and contains `.d` directories.  
2. Enable the DAG in the Airflow UI.  
3. Either let it run on its **2-minute polling schedule** or trigger it manually.  
4. Monitor logs and task progress in the UI.

---

## ⏩ DAG Workflow

The `msconvert_watch_simple` DAG performs:

1. **Discover New Runs** – scans the watch directory for `.d` directories not yet converted.  
2. **Wait Until Quiet** – ensures the directory is no longer being written to.  
3. **Convert One** – uses `DockerOperator` to launch a container running `msconvert.exe` under Wine, outputting `.mzML` or `.mzXML`.  
4. **Archive Original** *(optional)* – tars and gzips the source directory, then optionally deletes it.

---

## ➕ Adding Another DAG

To add new pipelines:

1. Create a new Python file in `dags/` (e.g. `new_pipeline_dag.py`).
2. Define a DAG object following Airflow conventions.
3. Airflow will auto-discover the new DAG within a minute or after a scheduler restart.
4. Set any required Variables.
5. Trigger via UI or API.

👉 [Airflow DAG Authoring Guide](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html)

---

## 🛠️ Useful Commands

```bash
# Bring stack up / down
sudo docker compose up -d
sudo docker compose down

# View container logs
sudo docker compose logs -f airflow-scheduler
sudo docker compose logs -f airflow-webserver

# Manage Airflow Variables
sudo docker compose exec airflow-scheduler airflow variables list
```

---

## 📚 References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [ProteoWizard msconvert](https://proteowizard.sourceforge.io/tools.shtml)
- [WineHQ](https://wiki.winehq.org/)

---

✅ **Note:** This deployment uses the **LocalExecutor** with a single scheduler. For higher throughput, you can scale using CeleryExecutor or KubernetesExecutor.
