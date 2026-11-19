FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    megatools \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && (pip install --no-cache-dir tgcrypto-pyrofork \
        || pip install --no-cache-dir tgcrypto \
        || echo "tgcrypto not installed (the bot still works, just a bit slower)")

COPY bot.py .
CMD ["python", "bot.py"]
