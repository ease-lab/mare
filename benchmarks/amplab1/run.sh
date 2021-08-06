#!/bin/bash

set -x

export AWS_REGION=us-west-1
[ -z "$AWS_ACCESS_KEY_ID" ] && echo 'Must set AWS_ACCESS_KEY_ID' && exit 1;
[ -z "$AWS_SECRET_ACCESS_KEY" ] && echo 'Must set AWS_SECRET_ACCESS_KEY' && exit 1;

../../driver/bin/driver \
  -workerURL worker.default.192.168.1.240.sslip.io:80 \
  -interBack S3 -interHint s3://ease-lab-mare/workspaces/manual/ \
  -outputBack S3 -outputHint s3://ease-lab-mare/workspaces/manual/ \
  -inputResourceBackend S3 \
  $(aws s3api list-objects --bucket ease-lab-mare --prefix examples/amplab1/inputs/ | jq -r '.Contents[].Key' | grep '.*.tsv' | sed 's/^/s3:\/\/ease-lab-mare\//')
