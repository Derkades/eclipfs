FROM python:3-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc pkg-config libfuse3-dev

RUN pip3 install \
        requests \
        requests-toolbelt \
        urllib3 \
        pyfuse3 \
        pycryptodome

RUN mkdir /app
WORKDIR /app
COPY src/ /app

ENTRYPOINT python -u mount.py /mnt
