FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

# Drop privileges: run as a non-root user. The data directory is created
# and handed to the runtime user so SQLite can open its WAL-mode files.
ARG JETTISON_UID=1000
ARG JETTISON_GID=1000
RUN groupadd --system --gid ${JETTISON_GID} jettison \
 && useradd --system --uid ${JETTISON_UID} --gid jettison --home-dir /app --shell /usr/sbin/nologin jettison \
 && mkdir -p /app/data \
 && chown -R jettison:jettison /app

USER jettison

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
