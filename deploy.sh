#!/usr/bin/env bash
# Build the Lambda zip and upload it to an existing function.
# Usage: ./deploy.sh <function-name>
set -euo pipefail

FUNCTION=${1:?usage: ./deploy.sh <function-name>}
cd "$(dirname "$0")"

rm -rf build function.zip
uv pip install -r requirements.txt --target build --quiet \
    --python-platform x86_64-manylinux2014 --python-version 3.12
cp lambda_function.py build/
(cd build && zip -qr ../function.zip .)

aws lambda update-function-code --function-name "$FUNCTION" \
    --zip-file fileb://function.zip --query LastModified --output text
