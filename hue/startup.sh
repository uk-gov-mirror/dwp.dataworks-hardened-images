#!/bin/sh

cat /tmp/hue-overrides.ini | envsubst > ./desktop/conf/hue-overrides.ini

./build/env/bin/hue migrate
./build/env/bin/hue createsuperuser --noinput --username ${DJANGO_SUPERUSER_USERNAME:-hue} --email ${DJANGO_SUPERUSER_EMAIL:-hue@example.org}
./build/env/bin/supervisor

