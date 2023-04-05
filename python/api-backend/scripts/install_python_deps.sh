#!/bin/bash

#
# Partition the instal by group, so
# it won't run out of memory
#

echo "Installing common dependencies..."
poetry install --no-interaction --no-root --no-ansi --only main,dev

echo "Installing utility dependencies..."
poetry install --no-interaction --no-root --no-ansi --only utility

echo "Installing graph dependencies..."
poetry install --no-interaction --no-root --no-ansi --only graph
