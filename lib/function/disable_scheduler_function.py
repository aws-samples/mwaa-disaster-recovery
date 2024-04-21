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


def handler(event, context):
    print(f'Event: {json.dumps(event)}')

    schedule_name = event['schedule_name']
    client = boto3.client('scheduler')

    print(f'Getting schedule details for {schedule_name} ...')
    result = client.get_schedule(Name=schedule_name)
    result = json.dumps(result, default=str)
    print(f'Result: {result}')


    print(f'Disabling schedule {schedule_name} ...')
    result = json.loads(result)

    result['State'] = 'DISABLED'
    extras = ['ResponseMetadata', 'Arn', 'CreationDate', 'LastModificationDate']
    for extra in extras:
        del result[extra]

    result = client.update_schedule(**result)
    print(f'Result: {result}')    

    return result
