FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

# Drop privileges: run as a non-root user. The data directory is created
# and handed to the runtime user so SQLite can open its WAL-mode files.
ARG MCM_UID=1000
ARG MCM_GID=1000
RUN groupadd --system --gid ${MCM_GID} mcm \
 && useradd --system --uid ${MCM_UID} --gid mcm --home-dir /app --shell /usr/sbin/nologin mcm \
 && mkdir -p /app/data \
 && chown -R mcm:mcm /app

USER mcm

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
