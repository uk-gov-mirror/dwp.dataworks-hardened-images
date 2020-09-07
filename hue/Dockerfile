FROM alpine:3.12 AS builder

RUN apk update && \
    apk upgrade && \
    apk add git bash

WORKDIR /tmp
RUN git clone --depth 1 --branch release-4.7.1 https://github.com/cloudera/hue.git

WORKDIR /tmp/hue

ENV PYTHON_VER=python3.8
ARG S3FS_VERSION=v1.86
ENV MNT_POINT /usr/share/hue/s3fs

RUN apk add rsync make g++ python3=3.8.5-r0 python3-dev=3.8.5-r0 \
        mariadb mariadb-dev	libffi-dev krb5-dev openldap-dev \
        libsasl sqlite-dev libxml2-dev libxslt-dev npm


RUN rm desktop/conf/* && \
    cp desktop/conf.dist/* desktop/conf

RUN rm -rf desktop/core/ext-py

RUN PREFIX=/usr/share make install


FROM alpine:3.12

COPY --from=builder /usr/share/hue /usr/share/hue

COPY --from=builder /tmp/hue/tools/docker/hue/conf3 /usr/share/hue/desktop/conf
COPY hue.ini /usr/share/hue/desktop/conf
COPY startup.sh /usr/share/hue

RUN addgroup hue && \
    adduser --system --uid 1001 --no-create-home --ingroup hue -s /bin/sh hue && \
    chown -R hue /usr/share/hue && \
    chmod a+x /usr/share/hue/startup.sh

RUN apk add python3=3.8.5-r0 libsasl libstdc++ krb5 libxslt fuse alpine-sdk automake autoconf libxml2-dev fuse-dev curl-dev git py-pip linux-pam; \
    git clone https://github.com/s3fs-fuse/s3fs-fuse.git; \
    cd s3fs-fuse; \
    git checkout tags/${S3FS_VERSION}; \
    ./autogen.sh; \
    ./configure --prefix=/usr; \
    make; \
    make install; \
    make clean; \
    rm -rf /var/cache/apk/*; \
    apk del git automake autoconf;

RUN mkdir /usr/share/hue/s3fs

# Install database connectors e.g.:
# RUN /usr/share/hue/build/env/bin/pip install simplepam
# RUN /usr/share/hue/build/env/bin/pip install pam
RUN /usr/share/hue/build/env/bin/pip install python-pam

WORKDIR /usr/share/hue

USER root

EXPOSE 8888

CMD ["./startup.sh"]
