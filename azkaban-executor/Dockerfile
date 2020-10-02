FROM gradle:jdk8 AS gradle-builder

ENV AZK_VERSION 3.81.0

USER root

RUN curl -k -LSs --output /tmp/azkaban.tar.gz https://github.com/azkaban/azkaban/archive/$AZK_VERSION.tar.gz && \
    tar -C /tmp -zoxf /tmp/azkaban.tar.gz && \
    /tmp/azkaban-$AZK_VERSION/gradlew -p /tmp/azkaban-$AZK_VERSION assemble && \
    mv /tmp/azkaban-$AZK_VERSION/azkaban-exec-server/build/distributions/azkaban-exec-server-0.1.0-SNAPSHOT.tar.gz /tmp/azkaban-exec-server.tar.gz && \
    mv /tmp/azkaban-$AZK_VERSION/az-exec-util/src/main/c/execute-as-user.c /tmp/execute-as-user.c

FROM openjdk:8-alpine AS binary-builder

COPY --from=gradle-builder /tmp/execute-as-user.c /tmp/execute-as-user.c

RUN apk add --update --no-cache \
    alpine-sdk \
    fts-dev && \
    gcc -o /tmp/execute-as-user /tmp/execute-as-user.c && \
    chown root /tmp/execute-as-user && \
    chmod 6050 /tmp/execute-as-user

FROM openjdk:8-alpine

COPY --from=gradle-builder /tmp/azkaban-exec-server.tar.gz /tmp/azkaban-exec-server.tar.gz
COPY entrypoint.sh /bin/entrypoint.sh

RUN sed -i -e 's/v[[:digit:]]\..*\//edge\//g' /etc/apk/repositories && \
    apk upgrade --update-cache --available && \
    apk add --update --no-cache \
    bash \
    aws-cli \
    jq && \
    tar -C /tmp -zoxf /tmp/azkaban-exec-server.tar.gz && \
    mv /tmp/azkaban-exec-server-0.1.0-SNAPSHOT /azkaban-exec-server && \
    mkdir -p /azkaban-exec-server/plugins/jobtypes && \
    chown -R nobody:nogroup /azkaban-exec-server && \
    chmod 755 /bin/entrypoint.sh

COPY --from=binary-builder /tmp/execute-as-user /azkaban-exec-server/execute-as-user

ENTRYPOINT [ "/bin/entrypoint.sh" ]
