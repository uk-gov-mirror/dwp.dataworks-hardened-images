FROM gradle:jdk8 AS gradle-builder

ENV AZK_VERSION 3.81.0

USER root

COPY azkaban-cognito-usermanager /tmp/azkaban-cognito-usermanager
COPY dist/LoginAbstractAzkabanServlet.java /tmp/LoginAbstractAzkabanServlet.java

RUN curl -k -LSs --output /tmp/azkaban.tar.gz https://github.com/azkaban/azkaban/archive/$AZK_VERSION.tar.gz && \
    tar -C /tmp -zoxf /tmp/azkaban.tar.gz && \
    mv -f /tmp/LoginAbstractAzkabanServlet.java /tmp/azkaban-$AZK_VERSION/azkaban-web-server/src/main/java/azkaban/webapp/servlet && \
    /tmp/azkaban-$AZK_VERSION/gradlew -p /tmp/azkaban-$AZK_VERSION assemble && \
    /tmp/azkaban-cognito-usermanager/gradlew -p /tmp/azkaban-cognito-usermanager assemble && \
    mv /tmp/azkaban-$AZK_VERSION/azkaban-web-server/build/distributions/azkaban-web-server-0.1.0-SNAPSHOT.tar.gz /tmp/azkaban-web-server.tar.gz && \
    mv /tmp/azkaban-$AZK_VERSION/azkaban-db/build/distributions/azkaban-db-0.1.0-SNAPSHOT.tar.gz /tmp/azkaban-db.tar.gz

FROM openjdk:8-alpine

COPY --from=gradle-builder /tmp/azkaban-web-server.tar.gz /tmp/azkaban-web-server.tar.gz
COPY --from=gradle-builder /tmp/azkaban-db.tar.gz /tmp/azkaban-db.tar.gz
COPY entrypoint.sh /bin/entrypoint.sh

RUN apk add --update --no-cache \
    bash \
    mysql-client \
    py3-pip && \
    pip3 install --upgrade pip && \
    pip3 install awscli && \
    tar -C /tmp -zoxf /tmp/azkaban-web-server.tar.gz && \
    tar -C /tmp -zoxf /tmp/azkaban-db.tar.gz && \
    mv /tmp/azkaban-web-server-0.1.0-SNAPSHOT /azkaban-web-server && \
    rm -f /azkaban-web-server/lib/jackson-core-2.2.2.jar && \
    mkdir -p /azkaban-web-server/plugins/jobtypes && \
    mkdir -p /azkaban-web-server/extlib && \
    mv /tmp/azkaban-db-0.1.0-SNAPSHOT /azkaban-db && \
    chown -R nobody:nogroup /azkaban-web-server && \
    chmod 755 /bin/entrypoint.sh

COPY dist/jackson-annotations-2.11.1.jar /azkaban-web-server/lib/jackson-annotations-2.11.1.jar
COPY dist/jackson-core-2.11.1.jar /azkaban-web-server/lib/jackson-core-2.11.1.jar
COPY dist/jackson-databind-2.11.1.jar /azkaban-web-server/lib/jackson-databind-2.11.1.jar

COPY --from=gradle-builder /tmp/azkaban-cognito-usermanager/build/libs/azkaban-cognito-usermanager-1.0.0-SNAPSHOT.jar /azkaban-web-server/extlib/azkaban-cognito-usermanager-1.0.0-SNAPSHOT.jar
    
ENTRYPOINT [ "/bin/entrypoint.sh" ]
