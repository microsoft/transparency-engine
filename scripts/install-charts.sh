#!/bin/sh
values_file=$1
backend_version=$(awk '/^version:[^\n]*$/ {split($0, a); print a[2]}' helm/Chart.yaml)

echo "Packaging transparency-engine..."
helm package helm/ --destination helm/dist

if [ -z "$values_file" ]; then
    echo "Installing transparency-engine with local values..."
    helm upgrade --install transparency-engine helm/dist/transparency-engine-$backend_version.tgz -f helm/values.local.yaml
else
    echo "Installing transparency-engine with values from file '$values_file'..."
    helm upgrade --install transparency-engine helm/dist/transparency-engine-$backend_version.tgz -f $values_file --wait
fi