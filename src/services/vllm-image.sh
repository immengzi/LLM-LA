#!/bin/bash
#
# Push images to the cluster private registry (reg.local:32000).
#
# Usage:
#   bash vllm-image.sh load   <image> <tag> <tar_path>           # tar → Docker → registry
#   bash vllm-image.sh direct <image> <tag> <tar_path>           # tar → containerd (local only)
#   bash vllm-image.sh pull   <image> <tag> <source_image:tag>   # upstream → Docker → registry
#
# Examples:
#   bash vllm-image.sh load   minimax27 selfcontained /mnt/nvme1/saeid/images/minimax27-selfcontained.tar.gz
#   bash vllm-image.sh direct minimax27 selfcontained /mnt/nvme1/saeid/images/minimax27-selfcontained.tar.gz
#   bash vllm-image.sh pull   ascend/vllm-ascend v0.11.0rc0 quay.io/ascend/vllm-ascend:v0.11.0rc0
#
set -e

REGISTRY="reg.local:32000"

usage() {
    echo "Usage: $0 <mode> <target_image> <target_tag> [tar_path|source_image:source_tag]"
    echo ""
    echo "Modes:"
    echo "  pull   <target_image> <target_tag> <source_image:source_tag>"
    echo "         Pull from upstream registry, retag, and push to cluster registry"
    echo ""
    echo "  load   <target_image> <target_tag> <tar_path>"
    echo "         Load from tar into Docker, retag, and push to cluster registry"
    echo ""
    echo "  direct <target_image> <target_tag> <tar_path>"
    echo "         Import tar directly into containerd on this node (no Docker needed)"
    echo ""
    echo "Examples:"
    echo "  $0 load   minimax27 selfcontained /mnt/nvme1/saeid/images/minimax27-selfcontained.tar.gz"
    echo "  $0 direct minimax27 selfcontained /mnt/nvme1/saeid/images/minimax27-selfcontained.tar.gz"
    echo "  $0 pull   ascend/vllm-ascend v0.11.0rc0 quay.io/ascend/vllm-ascend:v0.11.0rc0"
    exit 1
}

MODE="${1:-}"
TARGET_IMAGE="${2:-}"
TARGET_TAG="${3:-}"
SOURCE="${4:-}"

if [ -z "$MODE" ] || [ -z "$TARGET_IMAGE" ] || [ -z "$TARGET_TAG" ]; then
    usage
fi

case "$MODE" in
    pull)
        [ -z "$SOURCE" ] && { echo "Error: source_image:source_tag required for pull mode"; usage; }
        echo "=== Pulling from upstream: ${SOURCE} ==="
        docker pull "${SOURCE}"

        echo "=== Retagging for cluster registry ==="
        docker tag "${SOURCE}" ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}

        echo "=== Pushing to cluster registry (no proxy) ==="
        env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
          docker push ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}
        ;;

    load)
        [ -z "$SOURCE" ] && { echo "Error: tar_path required for load mode"; usage; }
        [ ! -f "$SOURCE" ] && { echo "Error: tar file not found: ${SOURCE}"; exit 1; }

        echo "=== Loading tar into Docker: ${SOURCE} ==="
        LOADED=$(docker load -i "${SOURCE}" | grep -oP '(?<=Loaded image: ).+')
        echo "Loaded: ${LOADED}"

        echo "=== Retagging for cluster registry ==="
        docker tag "${LOADED}" ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}

        echo "=== Pushing to cluster registry (no proxy) ==="
        env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
          docker push ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}
        ;;

    direct)
        [ -z "$SOURCE" ] && { echo "Error: tar_path required for direct mode"; usage; }
        [ ! -f "$SOURCE" ] && { echo "Error: tar file not found: ${SOURCE}"; exit 1; }

        echo "=== Importing tar directly into containerd: ${SOURCE} ==="
        ctr -n k8s.io image import "${SOURCE}"

        echo "=== Listing imported image ==="
        ctr -n k8s.io images ls | grep "${TARGET_IMAGE}"
        echo ""
        echo "NOTE: Image is only on this node's containerd. To make it"
        echo "available cluster-wide, use 'load' mode to push to the registry."
        ;;

    *)
        usage
        ;;
esac

echo "=== Done ==="
