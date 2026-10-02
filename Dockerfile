FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin app

WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
# Keep the application-owned, exactly pinned requirements; do not install Node/UI extras.
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# .dockerignore excludes runtime secrets, but intentionally keeps the committed .git.
COPY --chown=10001:10001 . /app
# WORKDIR can create /app as root; make the directory itself writable too.
RUN chown 10001:10001 /app
USER 10001:10001
# Fail clearly rather than manufacturing a repository or an uncommitted update baseline.
RUN test -d /app/.git \
    && test "$(git symbolic-ref --short HEAD)" = main \
    && git rev-parse --verify HEAD >/dev/null \
    && test -f /app/app/supervisor.py \
    && mkdir -p /app/data /app/logs /app/runtime/keys

EXPOSE 8000
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3); assert r.status == 200"
CMD ["python", "-m", "app.supervisor"]
