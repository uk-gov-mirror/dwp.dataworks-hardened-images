#!/bin/bash

./build/env/bin/hue migrate
./build/env/bin/supervisor

CREDENTIALS=$(curl 169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI)
ACCESS_KEY_ID=$(echo $CREDENTIALS | jq '.AccessKeyId')
SECRET_ACCESS_KEY=$(echo $CREDENTIALS | jq '.SecretAccessKey')
TOKEN=$(echo $CREDENTIALS | jq '.Token')
REGION=$( [[ ! -z "$AWS_REGION" ]] && echo "$AWS_REGION" || echo "$AWS_DEFAULT_REGION" )
sed -i '/[[[default]]]/s/\s*##//g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's/## access_key_id=/access_key_id=\'${ACCESS_KEY_ID}'/g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's/## secret_access_key=/secret_access_key=\'${SECRET_ACCESS_KEY}'/g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's/## security_token=/security_token=\'${TOKEN}'/g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's/## region=us-east-1/region=\'${REGION}'/g' /usr/share/hue/desktop/conf/hue.ini
sed -i 's*## livy_server_url=http://localhost:8998*livy_server_url=http://\'${EMR_HOST_NAME}':8998*g' /usr/share/hue/desktop/conf/hue.ini
sed -i '/## security_enabled=false/s/\s*##//g' /usr/share/hue/desktop/conf/hue.ini
