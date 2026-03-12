# syntax=docker/dockerfile:1

FROM ghcr.io/linuxserver/baseimage-alpine:3.23

ARG BUILD_DATE
ARG VERSION
LABEL build_version="abs-tracked single-container version:- ${VERSION} Build-date:- ${BUILD_DATE}"
LABEL maintainer="abs-tracked"

ENV MYSQL_DIR="/config" \
    DATADIR="/config/databases" \
    ABS_CONFIG_DIR="/config/app" \
    LSIO_FIRST_PARTY="false"

RUN \
  echo "**** install runtime packages ****" && \
  apk add --no-cache \
    bash \
    ca-certificates \
    curl \
    jq \
    logrotate \
    mariadb \
    mariadb-backup \
    mariadb-client \
    mariadb-common \
    mariadb-server-utils \
    netcat-openbsd \
    python3 \
    py3-pip && \
  printf "abs-tracked version: %s\nBuild-date: %s\n" "${VERSION}" "${BUILD_DATE}" > /build_version && \
  echo "**** cleanup ****" && \
  rm -rf \
    /tmp/* \
    "$HOME/.cache"

COPY ui/abs-tracked-ui/requirements.txt /opt/abs-tracked/ui/requirements.txt
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r /opt/abs-tracked/ui/requirements.txt

COPY ui/abs-tracked-ui/app.py /opt/abs-tracked/ui/app.py
COPY ui/abs-tracked-ui/templates /opt/abs-tracked/ui/templates
COPY ui/abs-tracked-ui/static /opt/abs-tracked/ui/static

COPY root/ /

EXPOSE 3306 8080
VOLUME /config
