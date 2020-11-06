#!/bin/sh
set -e

echo "INFO: Checking container configuration...."
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
    sed -i -e "/aws_secret_access_key/aws_session_token=${AWS_SESSION_TOKEN}" ~/.aws/credentials
  fi
else
  echo "INFO: Using attached IAM roles/instance profiles to authenticate with S3 as no AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY have been provided"
fi

SECRETS=$(aws secretsmanager get-secret-value --secret-id /concourse/dataworks/workflow_manager --query SecretBinary --output text | base64 -d )
DB_SECRETS=$(aws secretsmanager get-secret-value --secret-id azkaban-executor-rds-password --query SecretString --output text)
PASS=$(echo $SECRETS | jq -r .keystore_password)
export DB_NAME=$(echo $DB_SECRETS | jq -r .dbInstanceIdentifier)
export DB_HOST=$(echo $DB_SECRETS | jq -r .host)
export DB_USERNAME=$(echo $DB_SECRETS | jq -r .username)
export DB_PASSWORD=$(echo $DB_SECRETS | jq -r .password)

/usr/bin/openssl req -x509 -newkey rsa:4096 -keyout $JAVA_HOME/jre/lib/security/key.pem -out $JAVA_HOME/jre/lib/security/cert.pem -days 30 -nodes -subj "/CN=azkaban"
keytool -keystore /azkaban-exec-server/cacerts -storepass ${PASS} -noprompt -trustcacerts -importcert -alias self_signed -file $JAVA_HOME/jre/lib/security/cert.pem
openssl req -new -key $JAVA_HOME/jre/lib/security/key.pem -subj "/CN=azkaban" -out $JAVA_HOME/jre/lib/security/cert.csr
openssl pkcs12 -inkey $JAVA_HOME/jre/lib/security/key.pem -in $JAVA_HOME/jre/lib/security/cert.pem -export -out /azkaban-exec-server/cacerts.pkcs12 -passout pass:${PASS}
keytool -importkeystore -srckeystore /azkaban-exec-server/cacerts.pkcs12 -storepass ${PASS} -srcstorepass ${PASS} -srcstoretype PKCS12 -destkeystore /azkaban-exec-server/cacerts

echo "INFO: Copying azkaban exec-server configuration file(s) from ${S3_URI} to /azkaban-exec-server/conf..."
aws ${PROFILE_OPTION} s3 sync ${S3_URI}/${AZKABAN_ROLE} /azkaban-exec-server/conf
mv /azkaban-exec-server/conf/start-exec.sh /azkaban-exec-server/bin/start-exec.sh
mv /azkaban-exec-server/conf/internal-start-executor.sh /azkaban-exec-server/bin/internal/internal-start-executor.sh
mv /azkaban-exec-server/conf/commonprivate.properties /azkaban-exec-server/plugins/jobtypes/commonprivate.properties
mv /azkaban-exec-server/conf/private.properties /azkaban-exec-server/plugins/jobtypes/emr/private.properties
chmod +x /azkaban-exec-server/bin/start-exec.sh
chmod +x /azkaban-exec-server/bin/internal/internal-start-executor.sh

echo "INFO: Parsing secret and setting values"

while IFS='=' read -r prop val; do
  case $prop in
    jetty.password | jetty.keypassword | jetty.trustpassword)
      val=$(echo $SECRETS | jq -r .keystore_password)
      ;;
    executor.port)
      val=$(echo $SECRETS | jq -r .ports.azkaban_executor_port)
      ;;
    azkaban.webserver.url)
      val="${val}:$(echo $SECRETS | jq -r .ports.azkaban_executor_port)"
      ;;
    mysql.database)
      val=$DB_NAME
      ;;
    mysql.user)
      val=$DB_USERNAME
      ;;
    mysql.password)
      val=$DB_PASSWORD
      ;;
  esac
  printf '%s\n' "$prop=$val"
done < /azkaban-exec-server/conf/azkaban.properties > file.tmp && mv file.tmp /azkaban-exec-server/conf/azkaban.properties

echo "INFO: Assuming role to read from cognito"

CREDS=$(aws sts assume-role --endpoint-url "https://sts.eu-west-2.amazonaws.com" --role-arn "$COGNITO_ROLE_ARN" --role-session-name EMR_Get_Users | jq .Credentials)
export AWS_ACCESS_KEY_ID=$(echo "$CREDS" | jq -r .AccessKeyId)
export AWS_SECRET_ACCESS_KEY=$(echo "$CREDS" | jq -r .SecretAccessKey)
export AWS_SESSION_TOKEN=$(echo "$CREDS" | jq -r .SessionToken)

echo "INFO: Fetching groups from cognito"

COGNITO_GROUPS=$(aws cognito-idp list-groups --user-pool-id $USER_POOL_ID | jq -r .Groups[].GroupName)

echo "INFO: Adding users onto container"

for GROUP in $COGNITO_GROUPS; 
do
  echo "Creating group $GROUP"
  addgroup $GROUP

  echo "Adding users for group $GROUP"
  USERS=$(aws cognito-idp list-users-in-group --user-pool-id "$USER_POOL_ID" --group-name "$GROUP" | jq '.Users[]' | jq -r '(.Attributes[] | if .Name =="preferred_username" then .Value else empty end) // .Username')
  USERDIR=$(aws cognito-idp list-users --user-pool-id "$USER_POOL_ID")

  for USER in $USERS;
  do
    USERNAME=$(echo $USERDIR \
            | jq ".Users[] as \$u | if ( (\$u.Attributes[] | if .Name ==\"preferred_username\" then .Value else empty end) // \$u.Username) == \"$USER\" then \$u else empty end " \
            | jq -r ".Attributes[] | if .Name == \"sub\" then \"$USER\" + (.Value | match(\"...\").string) else empty end")

    echo "Creating user $USERNAME"
    if id "$USERNAME" &>/dev/null; then
        echo "User already exists, skipping."
    else
        adduser $USERNAME -DH
    fi

    echo "Adding user $USERNAME to group $GROUP"
    addgroup $USERNAME $GROUP

  done
done

unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
unset AWS_SESSION_TOKEN

addgroup azkaban

echo "INFO: Starting azkaban exec-server..."
exec /azkaban-exec-server/bin/start-exec.sh
