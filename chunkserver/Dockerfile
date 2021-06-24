FROM python:3-slim

RUN pip install gunicorn flask schedule requests

RUN mkdir /app /data
WORKDIR /app
COPY src /app

ENV DATA_DIR "/data"

ENV PYTHONUNBUFFERED=true

CMD [ "gunicorn", "--access-logfile", "-", "--error-logfile", "-", "-b", "0.0.0.0:80", "node:app", "--log-level", "info"]
