#!/usr/bin/env python

import requests

# let's talk to our AWS Elasticsearch cluster
#from requests_aws4auth import AWS4Auth
#auth = AWS4Auth('GK31c2f218a2e44f485b94239e',
#                       'b892c0665f0ada8a4755dae98baa3b133590e11dae3bcc1f9d769d67f16c3835',
#                       'us-east-1',
#                       's3')

from aws_requests_auth.aws_auth import AWSRequestsAuth
auth = AWSRequestsAuth(aws_access_key='GK31c2f218a2e44f485b94239e',
        aws_secret_access_key='b892c0665f0ada8a4755dae98baa3b133590e11dae3bcc1f9d769d67f16c3835',
        aws_host='localhost:3812',
        aws_region='us-east-1',
        aws_service='k2v')


response = requests.get('http://localhost:3812/alex',
                        auth=auth)
print(response.content)
