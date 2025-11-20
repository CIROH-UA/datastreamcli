#!/bin/bash
set -euo pipefail

LOG_FILE="/home/ec2-user/datastreamcli/docker_login_log.txt"
mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null || LOG_FILE="/tmp/docker_login_log.txt"

log() { echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }

# Cleanup on exit
cleanup() {
    docker logout >> "$LOG_FILE" 2>&1 || true
}
trap cleanup EXIT

# Parse arguments
DEPS_TAG="${1:-latest-arm64}"
DS_TAG="${2:-latest-arm64}"
BUILD_ARGS=$(echo "${3:-}" | tr -d '"')

log "Called with DEPS_TAG=$DEPS_TAG, DS_TAG=$DS_TAG, BUILD_ARGS=$BUILD_ARGS"

# Build push queue based on flags
declare -A PUSH_QUEUE
[[ "$BUILD_ARGS" == *"-e"* ]] && PUSH_QUEUE["datastream-deps"]="$DEPS_TAG"
[[ "$BUILD_ARGS" == *"-f"* ]] && PUSH_QUEUE["datastream"]="$DS_TAG"

if [ ${#PUSH_QUEUE[@]} -eq 0 ]; then
    log "No build flags found - nothing to push"
    exit 0
fi

log "Will push ${#PUSH_QUEUE[@]} image(s): ${!PUSH_QUEUE[*]}"

# Docker login
log "Logging into Docker Hub..."
DOCKERHUB_TOKEN=$(aws secretsmanager get-secret-value \
    --secret-id docker_awiciroh_creds \
    --region us-east-1 \
    --query SecretString \
    --output text | jq -r .DOCKERHUB_TOKEN)

[[ -z "$DOCKERHUB_TOKEN" ]] && { log "✗ Failed to retrieve token"; exit 1; }

echo "$DOCKERHUB_TOKEN" | docker login -u awiciroh --password-stdin >> "$LOG_FILE" 2>&1 || {
    log "✗ Docker login failed"
    exit 1
}
log "✓ Docker login successful"

# Push function
push_image() {
    local image=$1 tag=$2
    log "Pushing $image:$tag and latest-arm64..."

    docker tag "awiciroh/$image:latest-arm64" "awiciroh/$image:$tag" >> "$LOG_FILE" 2>&1
    docker push "awiciroh/$image:$tag" >> "$LOG_FILE" 2>&1
    docker push "awiciroh/$image:latest-arm64" >> "$LOG_FILE" 2>&1

    log "✓ Successfully pushed $image:$tag"
}

# Push all queued images
for image in "${!PUSH_QUEUE[@]}"; do
    push_image "$image" "${PUSH_QUEUE[$image]}"
done

log "✓ All pushes completed successfully"
