#!/usr/bin/env bash
# Only psql is needed: the database is hosted remotely, not on the runner.
set -euo pipefail

sudo apt-get update
sudo apt-get install --yes --no-install-recommends postgresql-client
psql --version
