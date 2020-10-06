#!/bin/sh

cat /tmp/hue-overrides.ini | envsubst > ./desktop/conf/hue-overrides.ini

/usr/bin/openssl req -x509 -newkey rsa:4096 -keyout /usr/share/hue/key.pem -out /usr/share/hue/cert.pem -days 30 -nodes -subj '/CN=hue'

mariadb-install-db --user=hue --datadir=/usr/share/hue/data --skip-test-db --innodb-log-file-size=10000000 --innodb-buffer-pool-size=5242880
echo Preparing to start DB server
sleep 5
mariadbd --user=hue --socket=/tmp/maria.sock --datadir=/usr/share/hue/data --innodb-log-file-size=10000000 --innodb-buffer-pool-size=5242880 &
echo Preparing to reset DB password
sleep 5
mariadb --socket=/tmp/maria.sock  << !EOF
    CREATE DATABASE hue COLLATE = 'utf8_general_ci';
    GRANT ALL ON hue.* TO 'hue'@'%' IDENTIFIED BY 'notsecret';
    SELECT * FROM INFORMATION_SCHEMA.SCHEMATA;
    QUIT
!EOF

./build/env/bin/hue migrate
./build/env/bin/hue createsuperuser --noinput --username ${DJANGO_SUPERUSER_USERNAME:-hue} --email ${DJANGO_SUPERUSER_EMAIL:-hue@example.org}
./build/env/bin/supervisor

