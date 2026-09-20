/**
 * Infrastructure-only Alchemy entrypoint.
 *
 * Provider bindings are intentionally kept out of this repository. Supply
 * provider credentials and the VM/network resource implementations through
 * the Alchemy environment when provisioning the remote K3s cluster.
 */
import { createAlchemy } from "alchemy";

const app = await createAlchemy({ name: "bnpb-m1", stage: process.env.ALCHEMY_STAGE ?? "local" });

console.log(JSON.stringify({
  application: app.name,
  stage: app.stage,
  serverBootstrap: "cloud-init/k3s-server.sh",
  agentBootstrap: "cloud-init/k3s-agent.sh",
  note: "Attach provider VM, network, firewall, and volume resources here using environment-managed secrets.",
}, null, 2));

await app.finalize();
