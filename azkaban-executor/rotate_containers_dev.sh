#! /bin/sh

set -x

if [ -z "$1" ]
  then
  	echo "AWS profile name not provided (arg 1)"
    exit 1
else
  PROFILE=$1
fi

if [ -z "$2" ]
  then
  	echo "Lambda function name not provided (arg 2)"
    exit 1
else
  TRUNCATE_TABLE_FUNCTION_NAME=$2
fi

if [ -z "$3" ]
  then
  	echo "Cluster name not provided (arg 3)"
    exit 1
else
  CLUSTER=$3
fi

if [ -z "$4" ]
  then
  	echo "Task definition <family>:<revision> name not provided (arg 4)"
    exit 1
else
  FAMILY_REVISION=$4
fi

function get_running_count() {
        aws ecs describe-services --profile $PROFILE --cluster $CLUSTER --services $1 | jq -r '.services[0].runningCount'
}

aws ecs update-service --cluster $CLUSTER --service azkaban-executor --desired-count 0 --profile $PROFILE
aws ecs update-service --cluster $CLUSTER --service azkaban-webserver --force-new-deployment --desired-count 0 --profile $PROFILE

executor_running_count=$(get_running_count azkaban-executor)
webserver_running_count=$(get_running_count azkaban-webserver)

retries=0

while [ $executor_running_count -ne 0 ] || [ $webserver_running_count -ne 0 ]; do
        if [ "$retries" -eq 15 ]; then
                echo "ERROR: MAX RETRIES Exceeded, Azkaban executor or webserver taking too long to shut down. Exiting..."
                exit 1;
        fi

        executor_running_count=$(get_running_count azkaban-executor)
        webserver_running_count=$(get_running_count azkaban-webserver)

        retries=$((retries + 1))
        sleep 10;
done;

aws lambda invoke --function-name $TRUNCATE_TABLE_FUNCTION_NAME
                  --invocation-type RequestResponse \
                  --payload '{ "table_to_truncate": "executors" }' \
                  --cli-connect-timeout 600 \
                  --cli-read-timeout 600 output.json

aws ecs update-service --cluster $CLUSTER --service azkaban-executor --task-definition $FAMILY_REVISION --desired-count 1 --profile $PROFILE
retries=0

while [ "$executor_running_count" -eq 0 ]; do
        if [ "$retries" -eq 10 ]; then
                  echo "ERROR: MAX RETRIES Exceeded, Azkaban executor taking too long to start. Exiting..."
                  exit 1;
        fi

        executor_running_count=$(get_running_count azkaban-executor)

        retries=$((retries + 1))
        sleep 10;
done;

aws ecs update-service --cluster $CLUSTER --service azkaban-webserver --desired-count 1 --profile $PROFILE
