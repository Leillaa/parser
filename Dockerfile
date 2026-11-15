FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Europe/Moscow

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kinomax_parser.py db.py ./
COPY db ./db

CMD ["python", "kinomax_parser.py", "--daemon", "--quiet"]
