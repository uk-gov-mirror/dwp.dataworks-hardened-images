#!/bin/sh

./build/env/bin/hue migrate

HOME=/usr/share/hue
MNT_POINT=$HOME/s3fs

adduser -D --uid 1002 --no-create-home --ingroup hue ${USER}
echo ${USER}:${USER} | chpasswd
echo "user = $USER"
echo "users on system = "
getent passwd

su hue
echo "hue user = $USER"

#wget -O $HOME/raw.txt 169.254.170.2"$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"
#
#ACCESS_KEY_ID=$(awk '{gsub(/.*"AccessKeyId":"|".*/,"");print}' $HOME/raw.txt)
#SECRET_ACCESS_KEY=$(awk '{gsub(/.*"SecretAccessKey":"|".*/,"");print}' $HOME/raw.txt)
#
#rm $HOME/raw.txt

echo "$ACCESS_KEY_ID:$SECRET_ACCESS_KEY" > passwd && chmod 600 passwd

# s3fs "$S3_BUCKET" "$MNT_POINT"

sed -i 's*## livy_server_url=http://localhost:8998*livy_server_url=http://\'${EMR_HOST_NAME}':8998*g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's*## webhdfs_url=http://localhost*webhdfs_url=http://\'${EMR_HOST_NAME}'*g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's*## default_user=hue*default_user=\'${USER}'*g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's*## proxy_user=*proxy_user=\'${USER}'*g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's*auth_username=hue*auth_username=\'${USER}'*g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's*auth_password=*auth_password=\'${USER}'*g' /usr/share/hue/desktop/conf/hue.ini
# sed -i 's/print authenticate(getpass.getuser(), getpass.getpass())/print(authenticate(getpass.getuser(), getpass.getpass()))/g' /usr/share/hue/build/env/lib/python3.8/site-packages/pam.py

echo "livy_server_url = $(grep livy_server_url= /usr/share/hue/desktop/conf/hue.ini)"
echo "webhdfs_url = $(grep webhdfs_url= /usr/share/hue/desktop/conf/hue.ini)"
echo "default_user= $(grep default_user= /usr/share/hue/desktop/conf/hue.ini)"
echo "proxy_user = $(grep proxy_user= /usr/share/hue/desktop/conf/hue.ini)"
echo "auth_username = $(grep auth_username= /usr/share/hue/desktop/conf/hue.ini)"
echo "auth_password = $(grep auth_password= /usr/share/hue/desktop/conf/hue.ini)"

./build/env/bin/supervisor
