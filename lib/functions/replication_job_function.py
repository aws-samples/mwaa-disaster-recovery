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

import json
import boto3

client = boto3.client("s3control")


def handler(event, context):
    print(f"Event: {json.dumps(event, default=str)}")

    account = event["account"]
    result = event.get("result")
    job_id = ""

    if not result:
        source_bucket = event["source_bucket"]
        report_bucket = event["report_bucket"]
        role_arn = event["replication_job_role"]

        result = client.create_job(
            AccountId=account,
            Operation={"S3ReplicateObject": {}},
            Report={
                "Bucket": report_bucket,
                "Format": "Report_CSV_20180820",
                "Enabled": True,
                "Prefix": "replication-report",
                "ReportScope": "AllTasks",
            },
            Priority=1,
            RoleArn=role_arn,
            ConfirmationRequired=False,
            ManifestGenerator={
                "S3JobManifestGenerator": {
                    "ExpectedBucketOwner": account,
                    "SourceBucket": source_bucket,
                    "EnableManifestOutput": False,
                    "Filter": {
                        "EligibleForReplication": True,
                        "ObjectReplicationStatuses": ["NONE", "FAILED"],
                    },
                }
            },
        )
        print(f"Create Job Result: {json.dumps(result, default=str)}")

    job_id = result["JobId"]
    result = client.describe_job(AccountId=account, JobId=job_id)

    output = json.dumps(result["Job"], default=str)
    print(f"Describe Job Result: {output}")
    return output
