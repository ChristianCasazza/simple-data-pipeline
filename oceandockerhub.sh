
if [ "$#" -ne 4 ]; then
  echo "Usage: $0 DOCKERHUB_USERNAME IMAGE_NAME TAG PLATFORM"
  echo "Example: $0 christiandata simplepipeline 503 linux/amd64"
  exit 1
fi

DOCKERHUB_USERNAME="$1"
IMAGE_NAME="$2"
TAG="$3"
PLATFORM="$4"

FULL_IMAGE="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${TAG}"

echo "🔨 Building Docker image ${FULL_IMAGE} for ${PLATFORM}…"
docker buildx build \
  --platform "${PLATFORM}" \
  --push \
  -t "${FULL_IMAGE}" \
  .

echo "✅ Build & push complete!"