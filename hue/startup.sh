#!/bin/sh

cat /tmp/hue-overrides.ini | envsubst > ./desktop/conf/hue-overrides.ini

./build/env/bin/hue migrate
./build/env/bin/supervisor

