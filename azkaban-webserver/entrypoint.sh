#!/bin/sh
set -e

echo "INFO: Checking container configuration..."
if [ -z "${AZKABAN_CONFIG_S3_BUCKET}" -o -z "${AZKABAN_CONFIG_S3_PREFIX}" ]; then
  echo "ERROR: AZKABAN_CONFIG_S3_BUCKET and AZKABAN_CONFIG_S3_PREFIX environment variables must be provided"
  exit 1
fi

S3_URI="s3://${AZKABAN_CONFIG_S3_BUCKET}/${AZKABAN_CONFIG_S3_PREFIX}"

# If either of the AWS credentials variables were provided, validate them
if [ -n "${AWS_ACCESS_KEY_ID}${AWS_SECRET_ACCESS_KEY}" ]; then
  if [ -z "${AWS_ACCESS_KEY_ID}" -o -z "${AWS_SECRET_ACCESS_KEY}" ]; then
    echo "ERROR: You must provide both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables if you want to use access key based authentication"
    exit 1
  else
    echo "INFO: Using supplied access key for authentication"
  fi

  # If either of the ASSUMEROLE variables were provided, validate them and configure a shared credentials fie
  if [ -n "${AWS_ASSUMEROLE_ACCOUNT}${AWS_ASSUMEROLE_ROLE}" ]; then
    if [ -z "${AWS_ASSUMEROLE_ACCOUNT}" -o -z "${AWS_ASSUMEROLE_ROLE}" ]; then
      echo "ERROR: You must provide both the AWS_ASSUMEROLE_ACCOUNT and AWS_ASSUMEROLE_ROLE variables if you want to assume role"
      exit 1
    else
      ASSUME_ROLE="arn:aws:iam::${AWS_ASSUMEROLE_ACCOUNT}:role/${AWS_ASSUMEROLE_ROLE}"
      echo "INFO: Configuring AWS credentials for assuming role to ${ASSUME_ROLE}..."
      mkdir ~/.aws
      cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id=${AWS_ACCESS_KEY_ID}
aws_secret_access_key=${AWS_SECRET_ACCESS_KEY}
[${AWS_ASSUMEROLE_ROLE}]
role_arn=${ASSUME_ROLE}
source_profile=default
EOF
      PROFILE_OPTION="--profile ${AWS_ASSUMEROLE_ROLE}"
    fi
  fi
  if [ -n "${AWS_SESSION_TOKEN}" ]; then
    sed -i -e "/aws_secret_access_key/a aws_session_token=${AWS_SESSION_TOKEN}" ~/.aws/credentials
  fi
else
  echo "INFO: Using attached IAM roles/instance profiles to authenticate with S3 as no AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY have been provided"
fi

if [ -n "$KEYSTORE_DATA" ]; then
  echo $KEYSTORE_DATA | base64 -d > /store.jwk
  export KEYSTORE_URL='file:////store.jwk'
fi

SECRETS=$(aws secretsmanager get-secret-value --secret-id /concourse/dataworks/workflow_manager --query SecretBinary --output text | base64 -d )
PASS=$(echo $SECRETS | jq -r .keystore_password)

/usr/bin/openssl req -x509 -newkey rsa:4096 -keyout $JAVA_HOME/jre/lib/security/key.pem -out $JAVA_HOME/jre/lib/security/cert.pem -days 30 -nodes -subj "/CN=azkaban"
keytool -keystore /azkaban-web-server/cacerts -storepass ${PASS} -noprompt -trustcacerts -importcert -alias self_signed -file $JAVA_HOME/jre/lib/security/cert.pem
openssl req -new -key $JAVA_HOME/jre/lib/security/key.pem -subj "/CN=azkaban" -out $JAVA_HOME/jre/lib/security/cert.csr
openssl pkcs12 -inkey $JAVA_HOME/jre/lib/security/key.pem -in $JAVA_HOME/jre/lib/security/cert.pem -export -out /azkaban-web-server/cacerts.pkcs12 -passout pass:${PASS}
keytool -importkeystore -srckeystore /azkaban-web-server/cacerts.pkcs12 -storepass ${PASS} -srcstorepass ${PASS} -srcstoretype PKCS12 -destkeystore /azkaban-web-server/cacerts

echo "INFO: Copying azkaban web-server configuration file(s) from ${S3_URI} to /azkaban-web-server/conf..."
aws ${PROFILE_OPTION} s3 sync ${S3_URI}/${AZKABAN_ROLE} /azkaban-web-server/conf
mv /azkaban-web-server/conf/start-web.sh /azkaban-web-server/bin/start-web.sh
mv /azkaban-web-server/conf/internal-start-web.sh /azkaban-web-server/bin/internal/internal-start-web.sh
chmod +x /azkaban-web-server/bin/start-web.sh
chmod +x /azkaban-web-server/bin/internal/internal-start-web.sh

while IFS='=' read -r prop val; do
  case $prop in
    jetty.password | jetty.keypassword | jetty.trustpassword)
      val=$(echo $SECRETS | jq -r .keystore_password)
      ;;
    jetty.port)
      val=$(echo $SECRETS | jq -r .ports.azkaban_webserver_port)
      ;;
    mysql.database)
      val=$(echo $SECRETS | jq -r .db_name)
      ;;
    mysql.user)
      val=$(echo $SECRETS | jq -r .db_username)
      ;;
    mysql.password)
      val=$(echo $SECRETS | jq -r .db_password)
      ;;
  esac
  printf '%s\n' "$prop=$val"
done < /azkaban-web-server/conf/azkaban.properties > file.tmp && mv file.tmp /azkaban-web-server/conf/azkaban.properties

echo "INFO: Starting azkaban web-server..."
exec /azkaban-web-server/bin/start-web.sh
