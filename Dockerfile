FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY config.yaml .

CMD ["python", "src/main.py", "--config", "config.yaml"]
