#! /bin/bash

docker run --rm -v $(pwd):/io -t \
    --env GIT_REPO=https://github.com/neurodata/boss-export boss-export/lambda \
    bash /io/package.sh

aws s3 cp lambda.zip s3://boss-export-lambda-zip/ --profile=ben-boss-dev
echo "https://boss-export-lambda-zip.s3.amazonaws.com/lambda.zip"