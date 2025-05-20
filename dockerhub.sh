#!/usr/bin/env bash

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 DOCKERHUB_USERNAME IMAGE_NAME TAG"
  echo "Example: $0 christiandata duckdb-example latest"
  exit 1
fi

DOCKERHUB_USERNAME="$1"
IMAGE_NAME="$2"
TAG="$3"

FULL_IMAGE="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG}"

echo "🔨 Building Docker image ${FULL_IMAGE}…"
docker build -t "${FULL_IMAGE}" .

echo "📤 Pushing ${FULL_IMAGE} to Docker Hub…"
docker push "${FULL_IMAGE}"

echo "✅ Done!"
