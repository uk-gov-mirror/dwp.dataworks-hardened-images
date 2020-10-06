FROM alpine:3.12 AS builder

RUN apk update && \
    apk upgrade && \
    apk add git bash

WORKDIR /tmp
RUN git clone --depth 1 --branch release-4.8.0 https://github.com/cloudera/hue.git

WORKDIR /tmp/hue

ENV PYTHON_VER=python3.8

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
COPY --from=builder /tmp/hue/tools/docker/hue/startup.sh /usr/share/hue

RUN addgroup hue && \
    adduser --system --uid 1001 --no-create-home --ingroup hue hue && \
    chown -R hue /usr/share/hue

RUN apk add python3=3.8.5-r0 libsasl libstdc++ krb5 libxslt gettext openssl \
        mariadb mariadb-connector-c mariadb-client

# Install database connectors e.g.:
# RUN ./build/env/bin/pip install git+https://github.com/gethue/PyHive

WORKDIR /usr/share/hue

USER hue

COPY --chown=hue hue-overrides.ini /tmp/
COPY --chown=hue startup.sh ./startup.sh
COPY --chown=hue settings.py /usr/share/hue/desktop/core/src/desktop/settings.py
COPY --chown=hue middleware.py /usr/share/hue/desktop/core/src/desktop/middleware.py

EXPOSE 8888

CMD ["./startup.sh"]
