#!/bin/sh

set -eux

# Mount entire S3 bucket with home KMS key and set permission metadata for user home dir

mkdir -p /mnt/tmp

/opt/s3fs-fuse/bin/s3fs ${S3_BUCKET} /mnt/tmp \
    -o ecs \
    -o endpoint=eu-west-2 \
    -o url=https://s3.amazonaws.com \
    -o use_sse=kmsid:${KMS_HOME}

mkdir -p /mnt/tmp/home/${USER}

chmod -R a-r /mnt/tmp/home/${USER} || true # || true Needed as this will fail if there are some files encrypted with a different KMS key

fusermount -u /mnt/tmp

# Mount entire S3 bucket with shared KMS key and set permission metadata for shared dir

/opt/s3fs-fuse/bin/s3fs ${S3_BUCKET} /mnt/tmp \
    -o ecs \
    -o endpoint=eu-west-2 \
    -o url=https://s3.amazonaws.com \
    -o use_sse=kmsid:${KMS_SHARED}


mkdir -p /mnt/tmp/shared/${TEAM};
chmod -R a-r /mnt/tmp/shared/${TEAM} || true # || true Needed as this will fail if there are some files encrypted with a different KMS key

fusermount -u /mnt/tmp

rm -rf /mnt/tmp

fusermount -u /mnt/s3fs/s3-home && fusermount -u /mnt/s3fs/s3-shared # in case cleanup failed on shutdown

mkdir -p /mnt/s3fs/s3-home && mkdir -p /mnt/s3fs/s3-shared

nohup /opt/s3fs-fuse/bin/s3fs ${S3_BUCKET}:/home/${USER} /mnt/s3fs/s3-home -f \
    -o allow_other \
    -o ecs \
    -o endpoint=eu-west-2 \
    -o url=https://s3.amazonaws.com \
    -o use_sse=kmsid:${KMS_HOME} \
    -o uid=1001 \
    -o use_cache=/tmp/${USER}-cache \
    -o enable_noobj_cache \
    -o nocopyapi \
    &> /var/log/s3fs-home &

nohup /opt/s3fs-fuse/bin/s3fs ${S3_BUCKET}:/shared/${TEAM} /mnt/s3fs/s3-shared -f \
    -o allow_other \
    -o ecs \
    -o endpoint=eu-west-2 \
    -o url=https://s3.amazonaws.com \
    -o use_sse=kmsid:${KMS_SHARED} \
    -o uid=1001 \
    &> /var/log/s3fs-shared &

cleanup() {
    echo "Container stopped, performing cleanup..."
    fusermount -u /mnt/s3fs/s3-home
    fusermount -u /mnt/s3fs/s3-shared
    rm -rf /mnt/s3fs/*
}

trap 'cleanup' SIGTERM

tail -f /var/log/s3fs-home -f /var/log/s3fs-shared &
wait $!
