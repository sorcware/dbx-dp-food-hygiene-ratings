#!/usr/bin/env bash
set -euo pipefail

TARGET=${1:-dev}

databricks bundle deploy \
  -p personal_dev \
  -t "${TARGET}"
