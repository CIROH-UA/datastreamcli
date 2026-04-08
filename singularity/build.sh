#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$(realpath "$0")")
VERSIONS_FILE="${SCRIPT_DIR}/../versions.yml"

DEPS_VERSION=$(grep 'datastream-deps' "$VERSIONS_FILE" | sed 's/.*: *"\(.*\)"/\1/')
DS_VERSION=$(grep '^datastream:' "$VERSIONS_FILE" | sed 's/.*: *"\(.*\)"/\1/')

echo "Building DataStreamCLI Singularity images..."
echo "  datastream-deps version: ${DEPS_VERSION}"
echo "  datastream version:      ${DS_VERSION}"

sed "s|^From: awiciroh/datastream:.*|From: awiciroh/datastream:${DS_VERSION}|" \
    datastream.def > datastream_pinned.def

singularity build --fakeroot datastream.sif datastream_pinned.def
rm datastream_pinned.def

singularity build merkdir.sif docker://zwills/merkdir:latest
singularity build ngiabinabox.sif "docker://awiciroh/ciroh-ngen-image:v${DS_VERSION}"

echo "All Singularity images built successfully."
