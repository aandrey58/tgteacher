FROM node:22-alpine AS ui
WORKDIR /ui
COPY admin-web/package.json ./
RUN npm install --no-audit --no-fund
COPY admin-web/ ./
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/bot
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot/tgteacher_bot /app/bot/tgteacher_bot
COPY --from=ui /ui/dist /app/admin-dist
CMD ["python", "-m", "tgteacher_bot.web_admin"]
