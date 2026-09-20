#!/bin/sh
set -eu

: "${K3S_TOKEN:?K3S_TOKEN must come from provisioning secret management}"
curl -sfL https://get.k3s.io | K3S_TOKEN="$K3S_TOKEN" sh -s - server --write-kubeconfig-mode 644
install -d -m 700 /var/lib/rancher/k3s/server
printf '%s\n' "$(cat /var/lib/rancher/k3s/server/node-token)" > /var/lib/rancher/k3s/server/join-token
