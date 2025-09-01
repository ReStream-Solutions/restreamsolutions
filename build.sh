#!/usr/bin/env bash
set -euo pipefail

rm -rf ./dist
poetry build -f wheel
