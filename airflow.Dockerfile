FROM python:3.7.3

LABEL maintainer='passeidireto-de-candidate'

WORKDIR /pd-de-challenge

ARG AIRFLOW_VERSION=2.0.0
ENV PYTHONIOENCODING=utf-8 \
    AIRFLOW_HOME=/pd-de-challenge \
    PYTHONPATH=":/pd-de-challenge" \
    SLUGIFY_USES_TEXT_UNIDECODE=yes \
    AIRFLOW_GPL_UNIDECODE=yes

RUN apt-get update && \
    apt-get install -y \
    python-pip \
    python-dev \
    python-setuptools \
    build-essential \
    autoconf \
    libtool \
    vim \
    git \
    jq \
    locales

COPY requirements_local.txt .

RUN python3 -m pip install --upgrade pip && \
    pip install -r requirements_local.txt && \
    locale-gen --purge pt_BR.UTF-8

COPY . .

RUN chmod +x start.sh

ENTRYPOINT /pd-de-challenge/start.sh

EXPOSE 8080