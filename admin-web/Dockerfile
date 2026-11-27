FROM node:22-alpine@sha256:1314fb30ec37592d5845e5a160d522c0e680972aa534549940b8d73bde88481a AS ui
WORKDIR /ui
COPY admin-web/package.json admin-web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY admin-web/ ./
RUN npm run build

FROM python:3.12-slim@sha256:191083cdf6c7bda8c76158983163ead5d5cf7b77cf3033e805bb0f5296f51b4b
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/bot
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends libzstd1 \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' libzstd1)" ge '1.5.7+dfsg-1+deb13u1' \
    && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock && pip check
COPY bot/tgteacher_bot /app/bot/tgteacher_bot
COPY --from=ui /ui/dist /app/admin-dist
RUN groupadd --gid 10001 tgteacher && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin tgteacher
ENV HOME=/home/tgteacher
USER 10001:10001
CMD ["python", "-m", "tgteacher_bot.web_admin"]
