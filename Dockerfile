FROM python:3.12-slim@sha256:191083cdf6c7bda8c76158983163ead5d5cf7b77cf3033e805bb0f5296f51b4b AS base

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/bot

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends libzstd1 \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' libzstd1)" ge '1.5.7+dfsg-1+deb13u1' \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir --require-hashes -r /app/requirements.lock && pip check

COPY bot /app/bot
RUN groupadd --gid 10001 tgteacher && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin tgteacher
ENV HOME=/home/tgteacher
RUN mkdir -p /app/runtime_cache /app/bot/exports /app/families && chown 10001:10001 /app/runtime_cache /app/bot/exports /app/families
FROM base AS migrator
USER 10001:10001
CMD ["python", "-m", "tgteacher_bot.db.provision_roles"]

FROM base AS bot-runtime
USER 10001:10001
CMD ["python", "bot/main.py"]

# Media-authoring services need FFmpeg for non-MP3 TTS responses.
# Keep the default target compatible with existing standalone builds.
FROM base AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
USER 10001:10001
CMD ["python", "bot/main.py"]

