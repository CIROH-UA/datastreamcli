#!/usr/bin/env bash
set -e

echo "Building DataStreamCLI Singularity images..."

# Main DataStreamCLI container
singularity build datastream.sif datastream.def

# Additional workflow containers (built from Docker images)
singularity build forcingprocessor.sif docker://awiciroh/forcingprocessor:latest
singularity build merkdir.sif docker://zwills/merkdir:latest
singularity build ngiabinabox.sif docker://awiciroh/ciroh-ngen-image:latest

echo "All Singularity images built successfully."

