FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y \
    python3-tk \
    tk-dev \
    clang \
    python3-pip \
    python3-venv \
    libagg-dev \
    libgl1 \
    libglib2.0-0 \
    ffmpeg \
    && apt-get clean

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt /tmp/
RUN pip install --upgrade pip wheel
RUN pip install -r /tmp/requirements.txt