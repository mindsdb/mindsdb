#!/bin/bash
#
# This creates an AWS AMI of the MindsDB Project, this AMI when built will
# also automatically be marked as Public, so the rest of the world can utilize
# it on AWS.  Please note: you will need to build an AMI for every possible
# AWS region for full global distribution.
#
# The release must be a branch, tag, or sha sum that is pushed to Github
#
# NOTE: If running locally with a valid AWS bash profile then this script will
#       use your AWS_DEFAULT_PROFILE, but if you are running this on a CI
#       environment, make sure that the following environment variables are set
#       AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
#

# CLI Input validation
if [ -z "$1" ]; then
    echo "Error: no region specified for the first parameter"
    echo "Example: $0 us-west-1 v0.8.8"
    exit 1
fi
if [ -z "$2" ]; then
    echo "Error: no git tag/branch/ref specified for the second parameter"
    echo "Example: $0 us-west-1 v0.8.8"
    exit 1
fi

# Run packer
packer build \
    -var "region=$1" \
    -var "release=$2" \
    packer-mindsdb-aws.json

# Sample usage: ./build-release.sh us-west-1 v0.8.8
