#!/bin/sh

./build/env/bin/hue migrate

HOME=/usr/share/hue
MNT_POINT=$HOME/s3fs

wget -O $HOME/raw.txt 169.254.170.2"$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"

ACCESS_KEY_ID=$(awk '{gsub(/.*"AccessKeyId":"|".*/,"");print}' $HOME/raw.txt)
SECRET_ACCESS_KEY=$(awk '{gsub(/.*"SecretAccessKey":"|".*/,"");print}' $HOME/raw.txt)

rm $HOME/raw.txt

cat "$ACCESS_KEY_ID:$SECRET_ACCESS_KEY" > passwd && chmod 600 passwd

s3fs "$S3_BUCKET" "$MNT_POINT"

sed -i 's*## livy_server_url=http://localhost:8998*livy_server_url=http://\'${EMR_HOST_NAME}':8998*g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's*## webhdfs_url=http://localhost*webhdfs_url=http://\'${EMR_HOST_NAME}'*g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's*## default_user=hue*default_user=\'${USER}'*g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's*## proxy_user=hue*proxy_user=\'${USER}'*g' /usr/share/hue/desktop/conf/hue.ini

./build/env/bin/supervisor
