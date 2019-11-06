#! /bin/bash

/bin/rm _build/boss-export.zip
mkdir _build
zip -r _build/boss-export.zip boss_export
cd venv-lambda/lib/python3.7/site-packages/
zip -gr ../../../../_build/boss-export.zip .
cd ../../../../
aws s3 cp _build/boss-export.zip s3://boss-export-lambda-zip/ --profile=ben-boss-dev
echo "https://boss-export-lambda-zip.s3.amazonaws.com/boss-export.zip"
