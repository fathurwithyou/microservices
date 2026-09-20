#!/bin/sh
set -eu

: "${K3S_URL:?K3S_URL must come from provisioning output}"
: "${K3S_TOKEN:?K3S_TOKEN must come from provisioning secret management}"
curl -sfL https://get.k3s.io | K3S_URL="$K3S_URL" K3S_TOKEN="$K3S_TOKEN" sh -s - agent
