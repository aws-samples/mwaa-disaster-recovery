---
inclusion: fileMatch
fileMatchPattern: "lib/**/*.py"
---

# CDK Stack Patterns

## Stack Hierarchy

All stacks extend `MwaaBaseStack` (which extends `cdk.Stack`). The base stack provides:
- `get_vpc_info()` — Looks up VPC, subnets, and security groups from IDs.
- `create_sns_topic_with_email_subscriptions()` — Creates SNS topic from config emails.
- `setup_notification()` — Wires EventBridge rules for StepFunctions status changes to SNS.

## Lambda Function Pattern

Every Lambda in this project follows the same bundling pattern:

```python
_lambda.Function(
    self,
    conf.get_name("function-name"),
    runtime=_lambda.Runtime.PYTHON_3_11,
    code=_lambda.Code.from_asset(
        path="lib/functions",
        bundling=cdk.BundlingOptions(
            image=_lambda.Runtime.PYTHON_3_11.bundling_image,
            command=[
                "bash", "-c",
                "pip install --no-cache -r requirements.txt -t /asset-output && rsync -au . /asset-output",
            ],
        ),
    ),
    handler="<module_name>.handler",
    timeout=cdk.Duration.seconds(10),
)
```

All Lambda handlers are in `lib/functions/`. Dependencies are in `lib/functions/requirements.txt`.

## StepFunctions Patterns

- State machines use `DefinitionBody.from_chainable()` with a chain of states.
- DAG triggers use `WAIT_FOR_TASK_TOKEN` integration pattern — the Airflow DAG sends back success/failure via `sfn.send_task_success()` / `sfn.send_task_failure()`.
- Health checks poll CloudWatch `SchedulerHeartBeat` metrics from the primary region.

## Cross-Region Replication

- Internal buckets (created by the stack): Use `CfnBucket.replication_configuration` directly.
- External buckets (pre-existing DAGs bucket): Use `AwsCustomResource` with `putBucketReplication` SDK call.
- One-time replication of existing objects uses S3 Batch Operations via a StepFunctions workflow.

## Custom Resources

- `AirflowCli` construct (`lib/dr_constructs/airflow_cli.py`) is a CDK custom resource that executes Airflow CLI commands (set variables, unpause DAGs) during stack create/update/delete.
- Replication job trigger is an `AwsCustomResource` that starts the batch replication StepFunctions workflow on stack creation.
