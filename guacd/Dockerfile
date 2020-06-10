ARG DEBIAN_VERSION=stable
FROM debian:${DEBIAN_VERSION} AS builder

ARG PREFIX_DIR=/usr/local/guacamole

ARG BUILD_DIR=/tmp/guacd-docker-BUILD
ARG BUILD_DEPENDENCIES="              \
        autoconf                      \
        automake                      \
        freerdp2-dev                  \
        gcc                           \
        libcairo2-dev                 \
        libjpeg62-turbo-dev           \
        libossp-uuid-dev              \
        libpango1.0-dev               \
        libpulse-dev                  \
        libssh2-1-dev                 \
        libssl-dev                    \
        libtelnet-dev                 \
        libtool                       \
        libvncserver-dev              \
        libwebsockets-dev             \
        libwebp-dev                   \
        git                           \
        make"

RUN apt-get update                         && \
    apt-get install -y $BUILD_DEPENDENCIES && \
    rm -rf /var/lib/apt/lists/*

RUN git clone --single-branch --branch staging/1.2.0 https://github.com/apache/guacamole-server.git /tmp/guacd/ && \
    mkdir -p "${PREFIX_DIR}/bin/" && \
    mkdir -p "$BUILD_DIR" && \
    cp -r /tmp/guacd/src/guacd-docker/bin/* "${PREFIX_DIR}/bin/" && \
    mv /tmp/guacd/* "$BUILD_DIR"

RUN ${PREFIX_DIR}/bin/build-guacd.sh "$BUILD_DIR" "$PREFIX_DIR"

RUN ${PREFIX_DIR}/bin/list-dependencies.sh    \
        ${PREFIX_DIR}/sbin/guacd              \
        ${PREFIX_DIR}/lib/libguac-client-*.so \
        ${PREFIX_DIR}/lib/freerdp2/guac*.so   \
        > ${PREFIX_DIR}/DEPENDENCIES

FROM debian:${DEBIAN_VERSION}-slim

ARG PREFIX_DIR=/usr/local/guacamole

ENV LC_ALL=C.UTF-8
ENV LD_LIBRARY_PATH=${PREFIX_DIR}/lib
ENV GUACD_LOG_LEVEL=info

ARG RUNTIME_DEPENDENCIES="            \
        ca-certificates               \
        ghostscript                   \
        fonts-liberation              \
        fonts-dejavu                  \
        xfonts-terminus"

COPY --from=builder ${PREFIX_DIR} ${PREFIX_DIR}

RUN apt-get update                                          && \
    apt-get install -y $RUNTIME_DEPENDENCIES                && \
    apt-get install -y $(cat "${PREFIX_DIR}"/DEPENDENCIES)  && \
    apt remove libavcodec58 libfreerdp-client2-2 libswresample3 libwinpr2-2 libavutil56 -y && \
    rm -rf /var/lib/apt/lists/*

RUN ${PREFIX_DIR}/bin/link-freerdp-plugins.sh \
        ${PREFIX_DIR}/lib/freerdp2/libguac*.so

EXPOSE 4822

CMD /usr/local/guacamole/sbin/guacd -b 0.0.0.0 -L $GUACD_LOG_LEVEL -f
