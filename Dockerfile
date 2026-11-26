FROM python:3.12-slim@sha256:191083cdf6c7bda8c76158983163ead5d5cf7b77cf3033e805bb0f5296f51b4b

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/bot

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

COPY requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir --require-hashes -r /app/requirements.lock && pip check

COPY . /app
RUN groupadd --gid 10001 tgteacher && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin tgteacher
ENV HOME=/home/tgteacher
RUN mkdir -p /app/runtime_cache /app/bot/exports /app/families && chown 10001:10001 /app/runtime_cache /app/bot/exports /app/families
USER 10001:10001

CMD ["python", "bot/main.py"]

