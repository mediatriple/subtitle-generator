FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install -r requirement.txt

CMD ["python3", "app.py"]
