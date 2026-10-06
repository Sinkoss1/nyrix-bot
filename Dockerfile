FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py app.html style.css ./

ENV PYTHONUNBUFFERED=1
ENV DB_PATH=/data/database.json

# volume mount point for persistent JSON DB
RUN mkdir -p /data

EXPOSE 8080

CMD ["python", "bot.py"]
