#!/bin/bash

#
# Partition the install by group, so
# it won't run out of memory
#

echo "Installing common dependencies..."
poetry config installer.max-workers 10
poetry install --no-interaction  --no-root --no-ansi --only main,dev
