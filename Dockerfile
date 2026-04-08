FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip && \
    pip install -r requirement.txt

CMD ["python3", "app.py"]
