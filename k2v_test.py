#!/usr/bin/env python

import requests
from datetime import datetime

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


print("-- ReadIndex")
response = requests.get('http://localhost:3812/alex',
                        auth=auth)
print(response.headers)
print(response.text)


print("-- Put initial (no CT)")
response = requests.put('http://localhost:3812/alex/root?sort_key=b',
                        auth=auth,
                        data='{}: Hello, world!'.format(datetime.timestamp(datetime.now())))
print(response.headers)
print(response.text)

print("-- Get")
response = requests.get('http://localhost:3812/alex/root?sort_key=b',
                        auth=auth)
print(response.headers)
print(response.text)
ct = response.headers["x-garage-causality-token"]

print("-- ReadIndex")
response = requests.get('http://localhost:3812/alex',
                        auth=auth)
print(response.headers)
print(response.text)

print("-- Put with CT")
response = requests.put('http://localhost:3812/alex/root?sort_key=b',
                        auth=auth,
                        headers={'x-garage-causality-token': ct},
                        data='{}: Good bye, world!'.format(datetime.timestamp(datetime.now())))
print(response.headers)
print(response.text)

print("-- Get")
response = requests.get('http://localhost:3812/alex/root?sort_key=b',
                        auth=auth)
print(response.headers)
print(response.text)

print("-- Put again with same CT (concurrent)")
response = requests.put('http://localhost:3812/alex/root?sort_key=b',
                        auth=auth,
                        headers={'x-garage-causality-token': ct},
                        data='{}: Concurrent value, oops'.format(datetime.timestamp(datetime.now())))
print(response.headers)
print(response.text)

print("-- Get")
response = requests.get('http://localhost:3812/alex/root?sort_key=b',
                        auth=auth)
print(response.headers)
print(response.text)
