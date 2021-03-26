#!/bin/sh

update_session_backup_file() {
  mkdir -p /mnt/s3fs/s3-home/.huedb
  rm -f /mnt/s3fs/s3-home/.huedb/huedb_session_dump.sql
  mv /tmp/huedb_backup.sql /mnt/s3fs/s3-home/.huedb/huedb_session_dump.sql \
  && echo "Session data backed up successfully"
}

save_session_data() {
  echo "Container shutting down, saving session data now..."
  rm -f /tmp/huedb_backup.sql
  mariadb-dump --socket=/tmp/maria.sock --databases hue > /tmp/huedb_backup.sql \
  && update_session_backup_file || echo "Session data backup not successful"
}

trap save_session_data SIGTERM

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

if [ -f "/mnt/s3fs/s3-home/.huedb/huedb_session_dump.sql" ]; then
  echo restoring previous session data from huedb sql dump file...
  mariadb --socket=/tmp/maria.sock hue < /mnt/s3fs/s3-home/.huedb/huedb_session_dump.sql
else
  echo previous session sql file does not exist, not running mysql query to restore huedb...
fi

./build/env/bin/hue migrate
./build/env/bin/hue createsuperuser --noinput --username ${DJANGO_SUPERUSER_USERNAME:-hue} --email ${DJANGO_SUPERUSER_EMAIL:-hue@example.org}
./build/env/bin/supervisor &

while true; do
  sleep 1
done
