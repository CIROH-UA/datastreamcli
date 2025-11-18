# Docker Configuration

This directory contains Docker configurations for building and running datastreamcli components. It is not required to manually build the containers to run datastreamcli. If the containers do not exist locally, datastreamcli will pull the `latest` (or `DS_TAG`,`FP_TAG`) containers from docker hub. 

## Files

- `docker-compose.yml` - Multi-service orchestration for datastream components
- `Dockerfile.datastream-deps` - Base dependencies image (Amazon Linux 2023)
- `Dockerfile.datastream` - Main datastream application image
- `config.json` - Docker daemon configuration with proxy settings

## Services

### datastream-deps
Base image with system dependencies and PROJ library for geospatial operations.

### datastream
Main application containing python_tools and configs directories.

## Usage

```bash
# Build all services
docker compose -f docker/docker-compose.yml build

# Build specific service
docker compose -f docker/docker-compose.yml build datastream

# Set architecture (x86 or aarch64)
ARCH=aarch64 docker -f docker/docker-compose.yml compose build

# Build with custom tags
TAG=latest-x86 docker compose -f docker/docker-compose.yml build datastream-deps
TAG=latest-x86 docker compose -f docker/docker-compose.yml build datastream

# Push with custom tags
TAG=latest-x86 docker compose -f docker/docker-compose.yml push datastream-deps
TAG=latest-x86 docker compose -f docker/docker-compose.yml push datastream
```

