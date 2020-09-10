FROM gradle:jdk8 AS builder

ENV AZK_VERSION 3.81.0

USER root

RUN curl -k -LSs --output /tmp/azkaban.tar.gz https://github.com/azkaban/azkaban/archive/$AZK_VERSION.tar.gz && \
    tar -C /tmp -zoxf /tmp/azkaban.tar.gz && \
    /tmp/azkaban-$AZK_VERSION/gradlew -p /tmp/azkaban-$AZK_VERSION assemble && \
    mv /tmp/azkaban-$AZK_VERSION/azkaban-exec-server/build/distributions/azkaban-exec-server-0.1.0-SNAPSHOT.tar.gz /tmp/azkaban-exec-server.tar.gz

FROM openjdk:8-alpine

COPY --from=builder /tmp/azkaban-exec-server.tar.gz /tmp/azkaban-exec-server.tar.gz
COPY entrypoint.sh /bin/entrypoint.sh

RUN sed -i -e 's/v[[:digit:]]\..*\//edge\//g' /etc/apk/repositories && \
    apk upgrade --update-cache --available && \
    apk add --update --no-cache \
    bash \
    aws-cli && \
    tar -C /tmp -zoxf /tmp/azkaban-exec-server.tar.gz && \
    mv /tmp/azkaban-exec-server-0.1.0-SNAPSHOT /azkaban-exec-server && \
    mkdir -p /azkaban-exec-server/plugins/jobtypes && \
    chown -R nobody:nogroup /azkaban-exec-server && \
    chmod 755 /bin/entrypoint.sh

ENTRYPOINT [ "/bin/entrypoint.sh" ]
