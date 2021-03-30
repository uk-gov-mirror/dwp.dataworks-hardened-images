# azkaban-executor

## A containerised Azkaban executor for use with EMR

## Description
Azkaban uses executors to carry out tasks that are requisitioned by Azkaban webservers. This implementation has the addition of a purpose built EMR step that allows for the running of scripts installed on EMR to be carried out with custom attributes passed in at run time.

## Development
It is key to understand the way that the containerised implementation of Azkaban works, in order to carry out development work and test the changes to the executor image. The webservers rely upon an RDS table in order to track the currently available executors that they can send tasks to. In order to ensure that the table is accurate, there is an order that must be followed:
1. Build and push your image to ECR - development images can be tagged with `debug`, so as not to affect higher environments:
   ```shell
    docker build -t <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/azkaban-executor:debug .
    docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/azkaban-executor:debug
    ```
1. Create a revised task definition for the service that points at the image tagged `debug`.
1. All active instances of both executors and webservers must be torn down. This can be done using the AWS console or AWS CLI:
    ```shell
    aws ecs update-service --cluster <CLUSTER_NAME> --service azkaban-executor --desired-count 0 
    aws ecs update-service --cluster <CLUSTER_NAME> --service azkaban-webserver --force-new-deployment --desired-count 0 
    ```
1. The `azkaban-truncate-table` lambda should be called to truncate the active executors table: 
    ```shell
    aws lambda invoke --function-name <FUNCTION_NAME>
                  --invocation-type RequestResponse \
                  --payload '{ "table_to_truncate": "executors" }' \
                  --cli-connect-timeout 600 \
                  --cli-read-timeout 600 output.json
    ```
1. The executor(s) should then be brought back up* before the webservers using the console or CLI:
    ```shell
    aws ecs update-service --cluster <CLUSTER_NAME> --service azkaban-executor --task-definition <TASK_DEFINITION_NAME:REVISION> --desired-count <INT> 
    executor_running_count=$(aws ecs describe-services --cluster <CLUSTER_NAME> --services azkaban-executor | jq -r '.services[0].runningCount')
    
    while [ "$executor_running_count" -eq 0 ]; do
            executor_running_count=$(aws ecs describe-services --cluster <CLUSTER_NAME> --services azkaban-executor | jq -r '.services[0].runningCount')
            sleep 10;
    done;

    aws ecs update-service --cluster <CLUSTER_NAME> --service azkaban-webserver --desired-count <INT> 
    ```
    *The new task definition will be used by default, if it is the latest active one but, it can be passed in to the cli command, if needed using `--task-definition <TASK_DEFINITION_NAME>:<REVISION_NUMBER>`

This should ensure that only the new version of the executor is registered with the webservers and all testing will be carried out on the updated image.

`./rotate_containers_dev.sh` is a script to carry out points 3-5 for you, it takes the following arguments:
1. AWS profile name
1. Lambda function (for truncating executor table) name
1. Cluster name
1. "<TASK_DEFINITION_NAME>:<REVISION_NUMBER>"

