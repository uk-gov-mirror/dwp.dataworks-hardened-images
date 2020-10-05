#!/bin/sh

cat /tmp/hue-overrides.ini | envsubst > ./desktop/conf/hue-overrides.ini

/usr/bin/openssl req -x509 -newkey rsa:4096 -keyout /usr/share/hue/key.pem -out /usr/share/hue/cert.pem -days 30 -nodes -subj '/CN=hue'

./build/env/bin/hue migrate
./build/env/bin/hue createsuperuser --noinput --username ${DJANGO_SUPERUSER_USERNAME:-hue} --email ${DJANGO_SUPERUSER_EMAIL:-hue@example.org}
./build/env/bin/supervisor

