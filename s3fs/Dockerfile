FROM alpine:3.12 AS builder

ENV S3FS_VERSION 1.86

RUN apk update && apk upgrade && apk add git automake autoconf alpine-sdk fuse libxml2-dev fuse-dev curl-dev

RUN git clone --depth 1 --branch v${S3FS_VERSION} https://github.com/s3fs-fuse/s3fs-fuse.git

RUN cd s3fs-fuse && \
    ./autogen.sh && \
    ./configure --prefix=/opt/s3fs-fuse && \
    make && \
    make install

FROM alpine:3.12

COPY --from=builder /opt/s3fs-fuse /opt/s3fs-fuse

RUN apk update && apk upgrade && apk add fuse libxml2 libstdc++ curl

COPY startup.sh /

RUN chmod +x startup.sh

ENTRYPOINT ["/startup.sh"]
