FROM proteowizard/pwiz-skyline-i-agree-to-the-vendor-licenses

# Install Python + pip (Debian-based pwiz image) and essentials
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3 python3-pip tzdata ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Place the watcher
COPY app.py /usr/local/bin/app.py

# Working directory (bind-mount here)
WORKDIR /data

# Defaults (overridable at runtime)
ENV WATCH_DIR=/data \
    OUTPUT_DIR=/data/mzML \
    QUIET_SECONDS=20 \
    CHECK_INTERVAL=5 \
    BOOTSTRAP=1 \
    FORMAT=mzML \
    GZIP=1 \
    LOG_LEVEL=INFO

ENTRYPOINT ["python3", "/usr/local/bin/app.py"]