# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

mwaa_environment_without_tags = {
    "AirflowConfigurationOptions": {},
    "AirflowVersion": "2.8.1",
    "Arn": "arn:aws:airflow:us-east-1:123456789999:environment/mwaa-2-8-1-public-primary",
    "CeleryExecutorQueue": "arn:aws:sqs:us-east-1:123456789888:airflow-celery-aaaaaaa-bbbb-1111-aa11-1112223334445",
    "CreatedAt": "2024-04-29 00:11:43+00:00",
    "DagS3Path": "dags",
    "DatabaseVpcEndpointService": "com.amazonaws.vpce.us-east-1.vpce-svc-xxxxxxxxxxxxxxxxxxx",
    "EndpointManagement": "SERVICE",
    "EnvironmentClass": "mw1.small",
    "ExecutionRoleArn": "arn:aws:iam::123456789999:role/mwaa-exec-role",
    "LastUpdate": {"CreatedAt": "2024-04-29 18:09:22+00:00", "Status": "SUCCESS"},
    "LoggingConfiguration": {
        "DagProcessingLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-DAGProcessing",
            "Enabled": True,
            "LogLevel": "WARNING",
        },
        "SchedulerLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-Scheduler",
            "Enabled": True,
            "LogLevel": "WARNING",
        },
        "TaskLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-Task",
            "Enabled": True,
            "LogLevel": "INFO",
        },
        "WebserverLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-WebServer",
            "Enabled": True,
            "LogLevel": "WARNING",
        },
        "WorkerLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-Worker",
            "Enabled": True,
            "LogLevel": "WARNING",
        },
    },
    "MaxWorkers": 10,
    "MinWorkers": 1,
    "Name": "mwaa-2-8-1-public-primary",
    "NetworkConfiguration": {
        "SecurityGroupIds": ["sg-xxxxxxxxxxxxxxxxxxx", "sg-xxxxxxxxxxxxxxxxxxx"],
        "SubnetIds": ["subnet-xxxxxxxxxxxxxxxxxxx", "subnet-xxxxxxxxxxxxxxxxxxx"],
    },
    "RequirementsS3ObjectVersion": "xxxxxxxxxxxxxxxxxxx",
    "RequirementsS3Path": "requirements.txt",
    "Schedulers": 2,
    "ServiceRoleArn": "arn:aws:iam::123456789999:role/aws-service-role/airflow.amazonaws.com/AWSServiceRoleForAmazonMWAA",
    "SourceBucketArn": "arn:aws:s3:::primary-bucket",
    "Status": "AVAILABLE",
    "Tags": {},
    "WebserverAccessMode": "PUBLIC_ONLY",
    "WebserverUrl": "aaaaaaaa-bbbb-1111-cc22-111222333444.a11.us-east-1.airflow.amazonaws.com",
    "WeeklyMaintenanceWindowStart": "FRI:12:00",
}

mwaa_environment_with_tags = {
    "AirflowConfigurationOptions": {},
    "AirflowVersion": "2.8.1",
    "Arn": "arn:aws:airflow:us-east-1:123456789999:environment/mwaa-2-8-1-public-primary",
    "CeleryExecutorQueue": "arn:aws:sqs:us-east-1:123456789888:airflow-celery-aaaaaaa-bbbb-1111-aa11-1112223334445",
    "CreatedAt": "2024-04-29 00:11:43+00:00",
    "DagS3Path": "dags",
    "DatabaseVpcEndpointService": "com.amazonaws.vpce.us-east-1.vpce-svc-xxxxxxxxxxxxxxxxxxx",
    "EndpointManagement": "SERVICE",
    "EnvironmentClass": "mw1.small",
    "ExecutionRoleArn": "arn:aws:iam::123456789999:role/mwaa-exec-role",
    "LastUpdate": {"CreatedAt": "2024-04-29 18:09:22+00:00", "Status": "SUCCESS"},
    "LoggingConfiguration": {
        "DagProcessingLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-DAGProcessing",
            "Enabled": True,
            "LogLevel": "WARNING",
        },
        "SchedulerLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-Scheduler",
            "Enabled": True,
            "LogLevel": "WARNING",
        },
        "TaskLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-Task",
            "Enabled": True,
            "LogLevel": "INFO",
        },
        "WebserverLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-WebServer",
            "Enabled": True,
            "LogLevel": "WARNING",
        },
        "WorkerLogs": {
            "CloudWatchLogGroupArn": "arn:aws:logs:us-east-1:123456789999:log-group:airflow-mwaa-2-8-1-public-primary-Worker",
            "Enabled": True,
            "LogLevel": "WARNING",
        },
    },
    "MaxWorkers": 10,
    "MinWorkers": 1,
    "Name": "mwaa-2-8-1-public-primary",
    "NetworkConfiguration": {
        "SecurityGroupIds": ["sg-xxxxxxxxxxxxxxxxxxx", "sg-xxxxxxxxxxxxxxxxxxx"],
        "SubnetIds": ["subnet-xxxxxxxxxxxxxxxxxxx", "subnet-xxxxxxxxxxxxxxxxxxx"],
    },
    "RequirementsS3ObjectVersion": "xxxxxxxxxxxxxxxxxxx",
    "RequirementsS3Path": "requirements.txt",
    "Schedulers": 2,
    "ServiceRoleArn": "arn:aws:iam::123456789999:role/aws-service-role/airflow.amazonaws.com/AWSServiceRoleForAmazonMWAA",
    "SourceBucketArn": "arn:aws:s3:::primary-bucket",
    "Status": "AVAILABLE",
    "Tags": {"Department": "DataEngineering"},
    "WebserverAccessMode": "PUBLIC_ONLY",
    "WebserverUrl": "aaaaaaaa-bbbb-1111-cc22-111222333444.a11.us-east-1.airflow.amazonaws.com",
    "WeeklyMaintenanceWindowStart": "FRI:12:00",
}


create_environment_response = {
    "ResponseMetadata": {
        "RequestId": "5c27cb79-0fa5-42e6-b6c6-f39b315492c5",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "date": "Mon, 29 Apr 2024 18:38:24 GMT",
            "content-type": "application/json",
            "content-length": "88",
            "connection": "keep-alive",
            "x-amzn-requestid": "5c27cb79-0fa5-42e6-b6c6-f39b315492c5",
            "x-amz-apigw-id": "XAFc_FC9iYcEtPA=",
            "x-amzn-trace-id": "Root=1-662fe91d-63cc089d101a6b025408e42f",
        },
        "RetryAttempts": 0,
    },
    "Arn": "arn:aws:airflow:us-east-2:123456789999:environment/mwaa-2-8-1-public-secondary",
}

get_environment_response = {
    "ResponseMetadata": {
        "RequestId": "a6ed8532-ece2-4375-a936-4c18c0e1a7f7",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "date": "Mon, 29 Apr 2024 19:03:35 GMT",
            "content-type": "application/json",
            "content-length": "2178",
            "connection": "keep-alive",
            "x-amzn-requestid": "a6ed8532-ece2-4375-a936-4c18c0e1a7f7",
            "x-amz-apigw-id": "XAJJPEFiCYcElgQ=",
            "x-amzn-trace-id": "Root=1-662fef07-1acab55336c678d614c6c39c",
        },
        "RetryAttempts": 0,
    },
    "Environment": mwaa_environment_with_tags,
}
