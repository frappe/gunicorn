FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install gunicorn from source.  setproctitle is required so dirty-arbiter
# and dirty-worker processes get distinguishable names that the tests use
# to count workers via pgrep.
COPY . /gunicorn-src/
RUN pip install --no-cache-dir /gunicorn-src/ setproctitle

# Copy test app
COPY tests/docker/dirty_ttin_ttou/app.py /app/
COPY tests/docker/dirty_ttin_ttou/gunicorn_conf.py /app/

CMD ["gunicorn", "-c", "gunicorn_conf.py", "app:app"]
