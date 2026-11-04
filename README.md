<img src="assets/icon.svg" width="64" height="64" alt="">

# Catalyst Operator

Manage [Catalyst](https://github.com/catalystctl/catalyst) game servers with Kubernetes manifests. Define a game template, choose a Catalyst node, and declare whether the server should be running or stopped. The operator creates the server, installs it, and keeps its configuration in sync.

Game servers run on your existing Catalyst agents. Kubernetes runs the operator and stores your configuration.

| Resource | What you manage |
| --- | --- |
| `Nest` | A group of game templates in Catalyst. |
| `Template` | A native Catalyst template or an imported Pterodactyl/Pelican egg. |
| `Server` | A game server's node, resources, ports, environment, and power state. |

All three resources use `catalyst.catalystctl.io/v1alpha1` and belong to a namespace. References between resources, Secrets, and ConfigMaps stay within that namespace. Catalyst names default to `metadata.name`; use `spec.name` to override them. Existing Catalyst objects are not adopted automatically.

## Before you install

You need:

- A Kubernetes cluster, `kubectl`, Helm with OCI registry support, and permission to install CRDs and cluster RBAC.
- A reachable Catalyst installation with agents and capacity for your game servers.
- A Catalyst API credential that can manage nests, templates, and servers, including install/start/stop operations. It needs `server.update` access to `PATCH /servers/:id/restart-policy`. Allocation selection also needs `node.read`; setting a different `ownerId` needs `user.create`.
- Network access from the operator to the Kubernetes API, Catalyst API, and any HTTPS egg sources.

Install **one operator release per cluster**. It watches all namespaces and is intended for clusters with trusted administrators.

The API is currently `v1alpha1`. Check compatibility with your deployed Catalyst version before upgrading either component; automated tests use mock APIs rather than live game nodes.

## Install from GitHub Container Registry

Install the Helm chart and operator image from GHCR:

- Chart: `oci://ghcr.io/melvspace/charts/catalyst-operator`
- Image: `ghcr.io/melvspace/catalyst-operator`

No local image build or repository checkout is required. The commands below use chart version `0.1.0`; consult [releases](https://github.com/melvspace/catalyst-operator/releases) for available versions. Chart versions and operator image versions are released separately; the chart supplies its default image tag.

### 1. Add your API credential

Create the operator namespace and a Secret named `catalyst-api` with an `api-key` entry. You can use your existing Secret management tooling, or create it from a file containing only the credential:

```bash
kubectl create namespace catalyst-operator

kubectl create secret generic catalyst-api \
  --namespace catalyst-operator \
  --from-file=api-key=/path/to/catalyst-api-key
```

The Secret must be in the operator namespace. After rotating its credential, restart the operator Deployment so it reads the new value.

### 2. Configure API access and networking

Save the following as `operator-values.yaml`. Replace the Catalyst URL and example addresses with your actual destinations. The URL must include `/api`.

```yaml
catalyst:
  url: https://catalyst.example/api

networkPolicy:
  egress:
    # Kubernetes API endpoint and port as reached from the operator Pod.
    - to:
        - ipBlock:
            cidr: 192.0.2.10/32
      ports:
        - protocol: TCP
          port: 6443
    # Catalyst API endpoint. Add rules for HTTPS egg sources if you use them.
    - to:
        - ipBlock:
            cidr: 192.0.2.20/32
      ports:
        - protocol: TCP
          port: 443
```

The chart enables a NetworkPolicy that allows DNS to `kube-system` by default. **You must add egress rules for the API endpoints**, or a cluster enforcing NetworkPolicies will block the operator. Choose `ipBlock` or namespace/Pod selectors appropriate to your CNI and routing. If another policy already controls this traffic, you can set `networkPolicy.enabled: false`.

### 3. Install the chart

```bash
helm upgrade --install catalyst \
  oci://ghcr.io/melvspace/charts/catalyst-operator \
  --version 0.1.0 \
  --namespace catalyst-operator \
  --values operator-values.yaml

kubectl rollout status deployment \
  --namespace catalyst-operator \
  --selector app.kubernetes.io/instance=catalyst
```

The chart installs CRDs, RBAC, a single operator replica, health probes, and a leadership Lease. The operator container runs as non-root with a read-only filesystem.

### Common chart settings

Keep overrides in `operator-values.yaml` so you can reuse them during upgrades. See [all chart values](charts/catalyst-operator/values.yaml) for scheduling and resource settings.

| Value | Default | Purpose |
| --- | --- | --- |
| `catalyst.existingSecret` | `catalyst-api` | Secret containing the Catalyst credential. |
| `catalyst.secretKey` | `api-key` | Credential key within that Secret. |
| `image.repository` | `ghcr.io/melvspace/catalyst-operator` | Operator container registry and image. |
| `image.tag` | Supplied by the chart | Pin a different operator image version. |
| `pollSeconds` | `30` | Periodic reconciliation interval. |
| `maxRecoveryAttempts` | `5` | Automatic start attempt budget. |
| `operationTimeoutSeconds` | `900` | Time before an unresolved operation requires recovery. |

## Create your first game server

Save the [complete Minecraft example](examples/minecraft.yaml) as `minecraft.yaml`. It includes a Nest, a ConfigMap containing a Paper egg, a Template, and a Server.

Before applying it, replace:

- `REPLACE_WITH_CATALYST_NODE_ID` and `REPLACE_WITH_CATALYST_LOCATION_ID` with IDs from your Catalyst installation.
- `REPLACE_WITH_PINNED_PAPER_JAR_HTTPS_URL` with the exact Paper JAR download URL you want to install.

The example's installation script accepts the Minecraft EULA. Use it only if you accept that agreement. Ensure port `25565` is available on the selected Catalyst node.

```bash
kubectl create namespace games

kubectl apply --namespace games --filename minecraft.yaml

kubectl get nests,templates,servers --namespace games

kubectl wait server/survival \
  --namespace games \
  --for=condition=Ready \
  --timeout=15m
```

The operator reconciles dependencies in order. To inspect installation progress or a failure:

```bash
kubectl describe server survival --namespace games

kubectl logs deployment/catalyst-catalyst-operator \
  --namespace catalyst-operator \
  --tail=100
```

`Ready=True` means installation completed, the declared configuration is applied, and Catalyst reports the requested power state. Check game logs and connectivity separately; readiness does not test game-level health.

## Configure servers

A Server references a Template in the same namespace. This example assumes a Template named `paper` and a Secret named `game-credentials` already exist in `games`:

```yaml
apiVersion: catalyst.catalystctl.io/v1alpha1
kind: Server
metadata:
  name: survival
  namespace: games
spec:
  templateRef:
    name: paper
  nodeId: YOUR_CATALYST_NODE_ID
  locationId: YOUR_CATALYST_LOCATION_ID
  resources:
    cpu: 2
    memory: 2Gi
    disk: 20Gi
  primaryPort: 25565
  powerState: Running
  deletionPolicy: Retain
  environment:
    SERVER_JAR_URL:
      value: YOUR_PINNED_PAPER_JAR_HTTPS_URL
    GAME_PASSWORD:
      secretKeyRef:
        name: game-credentials
        key: password
```

CPU is a whole-core quota from 1 to 128. Memory accepts 512–131072 MiB and disk accepts 1024–1048576 MiB. You can use quantities such as `2Gi` or `20Gi`; the operator rounds them up to whole MiB.

Node, location, owner, and template reference are immutable after creation. Omit `ownerId` to use the API credential's owner.

Each environment entry must contain exactly one of `value` or `secretKeyRef`. Changes to referenced Secrets trigger reconciliation. Declare memory and ports through their spec fields: Catalyst manages injected runtime variables such as memory/port compatibility values, UUIDs, network IP, and runtime paths.

### Stop, start, and restart

Set `spec.powerState` to `Stopped` or `Running`. For example, to stop a server:

```bash
kubectl patch server survival \
  --namespace games \
  --type merge \
  --patch '{"spec":{"powerState":"Stopped"}}'
```

To start it again, set `powerState` to `Running`. Stopping a server directly in Catalyst while its spec says `Running` causes the operator to start it again.

Environment and startup changes update Catalyst configuration but require a restart to affect the running process. Resource and port changes wait until the server stops. Change `spec.restartToken` to a new string to authorize a stop/update/start cycle:

```bash
kubectl patch server survival \
  --namespace games \
  --type merge \
  --patch '{"spec":{"restartToken":"restart-1"}}'
```

Use a different token for each restart. You can also set `powerState: Stopped` to apply pending configuration without starting the server. If you use GitOps, make these changes in your source manifests so the next sync preserves them.

## Choose a template source

A Template must select exactly one source:

- **Native Catalyst configuration:** use `spec.native` for the image, startup/stop commands, installation settings, variables, ports, and defaults. See the [native template example](examples/native-template.yaml).
- **Egg from a URL:** use `spec.egg.url` with an HTTPS URL returning egg JSON. Change the URL or `spec.egg.revision` to import an update; the operator does not poll the URL for changes.
- **Egg from a ConfigMap:** use `spec.egg.configMapKeyRef` with a same-namespace name and key. Content changes trigger reimport. The Minecraft example uses this approach.

Use `spec.nestRef` to put the Template in a Nest. Egg import supports Pterodactyl and Pelican image formats, variables, installation scripts, startup placeholders, stop commands, configuration substitutions, and feature metadata. Downloads are limited to 700 KiB. Inspect `status.warnings` after import, especially for unknown egg format versions; malformed configuration blocks fail import.

**Template updates affect future servers.** Each existing Server keeps a private frozen runtime template and a Secret snapshot of inherited configuration. Updating the shared Template does not upgrade those servers.

The operator disables Catalyst's automatic agent restarts and manages recovery itself. If an existing running server needs that policy applied, it reports `RestartRequired`; change `spec.restartToken` to authorize the restart.

## Connect to your game server

The operator uses Catalyst bridge networking. Set `primaryPort` explicitly. Optional `portBindings` map container port strings to host ports; the primary port maps to the same host port unless you override it.

To select an address, set `allocationId` to an existing Catalyst allocation whose port matches `primaryPort`. Update the allocation and primary port together when switching allocations. The primary binding must publish the allocation's port. `primaryIp` is unsupported with bridge networking.

An assigned allocation cannot be removed through Catalyst's server-update API. Restore the reference, choose another allocation, or migrate to a separate Server. Promoting an already-owned secondary allocation with a different address or port is also unsupported; detach it in Catalyst first or choose a free allocation.

Inspect the addresses and TCP/UDP ports in Server status:

```bash
kubectl get server survival --namespace games --output yaml
```

Clients and proxies connect to the Catalyst node's published address and ports. Configure routing and node firewalls to allow that traffic. The operator does not create Kubernetes Services, tunnels, or proxy configuration.

## Troubleshoot and recover

Start with `kubectl describe server NAME --namespace NAMESPACE`. Current `Ready`, `Reconciling`, and `Stalled` conditions explain progress and blocked operations. `status.lastError` may retain an older failure after recovery, so use the current conditions to assess the server.

| Reason | What to do |
| --- | --- |
| `RestartRequired` | Change `spec.restartToken`, or stop the server through `spec.powerState` to apply pending configuration. |
| `RestartPolicyUnavailable` | Restore API access to the restart-policy endpoint, including `server.update` permission. No start attempt is spent while this is blocked. |
| `RecoveryRequired` | Inspect Catalyst and node logs. Fix the cause, then change `spec.retryToken`. For a timed-out power transition, first resolve or cancel it and wait for a stable `running` or `stopped` state. |
| `InstallationUnconfirmed` | Inspect Catalyst logs and server files. If installation completed, change `spec.installationConfirmationToken`. Resolve a failed installation manually in Catalyst. |
| `Missing` | Investigate the missing server and its data. Change `spec.replacementToken` only when you want a fresh server; replacement does not recover old data. |
| `NameOutcomeUnknown` | Check Catalyst/backend requests and recover the created server or complete its rename manually. `retryToken` does not release the reserved name. |
| `AllocationPortBindingMismatch` | Remove the primary binding override or set its host port to the allocation's port. |
| `AllocationPromotionUnsupported` | Restore the primary reference, choose a free allocation, or detach the secondary allocation in Catalyst first. |
| `DeletionBlocked` / `CleanupUnconfirmed` | Inspect remaining dependencies, operations, and Catalyst/node resources before intervening in deletion. |

Recovery tokens are acknowledgements: set the relevant field to a new string **after** completing its recovery steps. Tokens changed during an unresolved power transition do not queue another command.

Automatic starts use backoff and a default budget of five attempts. Five minutes of stable running resets the budget. The operator never automatically repeats an ambiguous installation or calls reinstall. A fast installation can require confirmation if its active state was never observed.

Ambiguous create and rename requests retain their operation records to avoid duplicates. Preserve the operator's UID markers in Catalyst descriptions. Do not clear operation records, name reservations, or finalizers until you have verified the external outcome and confirmed no request remains in flight.

## Delete resources and back up state

All resources default to `deletionPolicy: Retain`: deleting the Kubernetes resource leaves its Catalyst object in place. To have the operator stop and delete a game server, set `deletionPolicy: Delete` before deleting the Server resource. This also removes its private runtime template. With `Delete`, Templates and Nests cannot be deleted while dependants remain, including objects retained in Catalyst.

Deletion can wait for an unresolved create, install, rename, power, or cleanup operation, even with `Retain`. Follow the status guidance and resolve the external operation first. Switching to `Retain` does not clear an uncertain deletion. Removing the `catalyst.catalystctl.io/cleanup` finalizer manually can leave external resources behind.

Back up the custom resources **with their status**, template ConfigMap snapshots, Server Secret snapshots, and server-name identity ConfigMaps in the operator namespace. The identity ConfigMaps carry the label `catalyst.catalystctl.io/name-identity=server` and survive `Retain` deletion. Missing snapshots block reconciliation. Back up game data on your Catalyst nodes separately.

Server-name reservations coordinate operator-managed servers across namespaces. Coordinate other tools and administrators that create or rename servers directly in Catalyst, which permits duplicate server names.

## Upgrade or uninstall

Helm installs CRDs on the first installation but does not upgrade them. Set `CHART_VERSION` to your chosen published chart version, review its release notes, and apply its CRDs before upgrading:

```bash
CHART_VERSION=0.1.0

helm show crds oci://ghcr.io/melvspace/charts/catalyst-operator \
  --version "$CHART_VERSION" > catalyst-crds.yaml

kubectl apply --filename catalyst-crds.yaml

helm upgrade catalyst \
  oci://ghcr.io/melvspace/charts/catalyst-operator \
  --version "$CHART_VERSION" \
  --namespace catalyst-operator \
  --values operator-values.yaml
```

Before uninstalling, choose whether to retain or delete your Catalyst objects and let the operator finish processing resource deletions. Then remove the operator:

```bash
helm uninstall catalyst --namespace catalyst-operator
```

Helm leaves CRDs and custom resources in place. Retained game servers keep running in Catalyst; the operator no longer reconciles them.

## Contributing

From a repository checkout:

```bash
cargo fmt --check

cargo clippy --locked --all-targets -- -D warnings

cargo test --locked

cargo run --locked --bin crdgen > charts/catalyst-operator/crds/resources.yaml

helm lint charts/catalyst-operator \
  --set catalyst.url=https://catalyst.example/api
```

For local execution, set `KUBECONFIG`, `CATALYST_URL`, `CATALYST_API_KEY`, and `POD_NAMESPACE`. `HEALTH_BIND_ADDRESS` defaults to `0.0.0.0:8080`; `POLL_SECONDS`, `MAX_RECOVERY_ATTEMPTS`, and `OPERATION_TIMEOUT_SECONDS` default to `30`, `5`, and `900`.
