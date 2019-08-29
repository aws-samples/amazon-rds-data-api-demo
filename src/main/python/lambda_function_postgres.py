###############################################################################
# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
#
# For more information, see:
# https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html
#
# Prerequisites:
#  - Required permissions to Secrets Manager and RDS Data API
#  - Latest boto3 SDK in local package. Run the following in Cloud9 shell:
#     - `pip install boto3 -t boto3`
#
###############################################################################

import os
import os.path
import sys

# Use latest boto3 from local environment
LAMBDA_TASK_ROOT = os.environ["LAMBDA_TASK_ROOT"]
sys.path.insert(0, LAMBDA_TASK_ROOT+"/boto3")

# Required imports
import botocore
import boto3

# Imports for your app
from datetime import datetime

# Update your cluster and secret ARNs
cluster_arn = 'arn:aws:rds:us-east-1:0000000000:cluster:my-cluster' 
secret_arn = 'arn:aws:secretsmanager:us-east-1:0000000000:secret:my-secret'

def lambda_handler(event, context):
    if 'Records' not in event or 'Sns' not in event['Records'][0]:
        print('Not an SNS event!')
        print(str(event))
        return

    for record in event['Records']:
        call_rds_data_api(record['Sns']['Timestamp'], record['Sns']['Message'])


def call_rds_data_api(timestamp, message):
    rds_data = boto3.client('rds-data')

    sql = """
          INSERT INTO sample_table(received_at, message)
          VALUES(TO_TIMESTAMP(:time, 'YYYY-MM-DD HH24:MI:SS'), :message)
          """

    param1 = {'name':'time', 'value':{'stringValue': timestamp}}
    param2 = {'name':'message', 'value':{'stringValue': message}}
    param_set = [param1, param2]
 
    response = rds_data.execute_statement(
        resourceArn = cluster_arn, 
        secretArn = secret_arn, 
        database = 'tutorial', 
        sql = sql,
        parameters = param_set)
    
    print(str(response));
