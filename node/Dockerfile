FROM python:3

RUN pip install gunicorn flask schedule requests

RUN mkdir /app
WORKDIR /app
COPY src /app

# CMD [ "gunicorn", "-b", "0.0.0.0:80", "node:app" ]

RUN mkdir /data

ENV DATA_DIR "/data"

ENV PYTHONUNBUFFERED=true

CMD [ "gunicorn", "--access-logfile", "-", "--error-logfile", "-", "-b", "0.0.0.0:80", "node:app", "-c", "gunicorn-config.py", "--log-level", "debug"]
