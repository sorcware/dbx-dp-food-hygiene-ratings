#!/usr/bin/env bash
set -euo pipefail

databricks bundle deploy \
  -p personal_dev \
  --var="raw_catalog=raw" \
  --var="raw_schema=fhrs" \
  --var="bronze_catalog=bronze" \
  --var="bronze_schema=fhrs" \
  --var="silver_catalog=silver" \
  --var="silver_schema=fhrs" \
  --var="gold_catalog=gold" \
  --var="gold_schema=fhrs"
