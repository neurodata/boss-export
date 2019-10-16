#! /bin/bash

rm _build/boss-export.zip
zip -r _build/boss-export.zip boss_export
mkdir _build
cd venv-lambda/lib/python3.7/site-packages/
zip -gr ../../../../_build/boss-export.zip .