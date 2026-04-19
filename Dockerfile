FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

# Drop privileges: run as a non-root user. The data directory is created
# and handed to the runtime user so SQLite can open its WAL-mode files.
ARG RECLAIMER_UID=1000
ARG RECLAIMER_GID=1000
RUN groupadd --system --gid ${RECLAIMER_GID} reclaimer \
 && useradd --system --uid ${RECLAIMER_UID} --gid reclaimer --home-dir /app --shell /usr/sbin/nologin reclaimer \
 && mkdir -p /app/data \
 && chown -R reclaimer:reclaimer /app

USER reclaimer

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
