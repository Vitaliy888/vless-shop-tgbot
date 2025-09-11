#!/usr/bin/env bash
set -euo pipefail
REPO_DIR="/opt/vless-shopbot"
if [ ! -d "$REPO_DIR" ]; then
  echo "Repo dir $REPO_DIR not found"
  exit 1
fi
cd "$REPO_DIR"
if [ -n "${1:-}" ]; then
  git fetch --all --tags
  git checkout "$1" || true
  git pull origin "$1" || git pull || true
else
  git pull || true
fi
sudo docker-compose down --remove-orphans
sudo docker-compose up -d --build
echo "Update complete"
