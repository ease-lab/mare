#!/bin/bash

[ -z "$AWS_ACCESS_KEY_ID" ] && echo 'Must set AWS_ACCESS_KEY_ID' && exit 1;
[ -z "$AWS_SECRET_ACCESS_KEY" ] && echo 'Must set AWS_SECRET_ACCESS_KEY' && exit 1;

kn service apply --wait-timeout 60 --filename worker.yml \
  --env AWS_REGION=us-west-1 \
  --env AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --env AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --env ENABLE_TRACING='true'
