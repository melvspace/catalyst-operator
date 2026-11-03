<img src="assets/icon.svg" width="64" height="64" alt="">

# catalyst-operator

A Rust / kube-rs Kubernetes operator for declarative [Catalyst](https://github.com/catalystctl/catalyst) game servers. It manages an existing Catalyst installation; Catalyst's agents run the game servers on their own nodes.

The namespaced `Nest`, `Template`, and `Server` resources use `catalyst.catalystctl.io/v1alpha1`. References stay within a namespace, and dependencies reconcile in order. Catalyst names default to `metadata.name`; `spec.name` overrides them. Existing Catalyst objects are never adopted automatically.

## Build and install

Build and publish the image to a registry your cluster can access:

```bash
docker build -t YOUR_REGISTRY/catalyst-operator:0.1.0 .
docker push YOUR_REGISTRY/catalyst-operator:0.1.0
```

Create a `catalyst-api` Secret in the operator namespace containing an `api-key` entry. Supply your credential through your existing Secret management tooling. The API credential needs permission to read/manage templates and nests, create/update/delete servers, and install/start/stop servers. Selecting an allocation also requires `node.read` access to its node. Setting another owner additionally requires Catalyst's `user.create` permission.

Install one operator release per cluster. It watches all namespaces and assumes trusted administrators. Configure `catalyst.url` with the complete API base, including `/api`:

```bash
helm upgrade --install catalyst ./charts/catalyst-operator \
  --namespace catalyst-operator --create-namespace \
  --set image.repository=YOUR_REGISTRY/catalyst-operator \
  --set image.tag=0.1.0 \
  --set catalyst.url=https://catalyst.example/api \
  --values operator-values.yaml
```

The chart includes CRDs, RBAC, a single-replica Deployment, health probes, a Kubernetes Lease, and a non-root container with a read-only filesystem. Configure `networkPolicy.egress` in `operator-values.yaml` for your Kubernetes API, Catalyst endpoint, and HTTPS egg sources. The policy defaults to DNS-only egress until those destinations are provided. Example rules, **with addresses replaced for your infrastructure**:

```yaml
networkPolicy:
  egress:
    - to:
        - ipBlock:
            cidr: 192.0.2.10/32
      ports:
        - protocol: TCP
          port: 6443
    - to:
        - ipBlock:
            cidr: 192.0.2.20/32
      ports:
        - protocol: TCP
          port: 443
```

Helm installs CRDs on first installation but does not upgrade them. Apply the generated CRDs before upgrading the chart:

```bash
kubectl apply -f charts/catalyst-operator/crds/resources.yaml
```

## Declare resources

See [the complete example](examples/minecraft.yaml) for a Nest, ConfigMap egg, Template, and Server. Replace its node/location IDs and pinned server JAR URL before applying. The example install script accepts the Minecraft EULA; use it only if you accept that agreement. A [native template example](examples/native-template.yaml) shows the alternative source format.

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
    GAME_SETTING:
      value: example
    GAME_PASSWORD:
      secretKeyRef:
        name: game-credentials
        key: password
```

CPU is a whole-core quota. Memory and disk accept Kubernetes-style binary or decimal quantities; they are converted to integer Catalyst MiB quotas, rounded upward. Supported limits are 1–128 cores, 512–131072 MiB memory, and 1024–1048576 MiB disk. Node, location, owner, and template reference are immutable. Omit `ownerId` to use the API credential's owner.

Each environment entry has exactly one `value` or `secretKeyRef`. Secret references use the Server's namespace. Changes to referenced Secrets trigger reconciliation. Catalyst owns its injected runtime variables, including memory/port compatibility values, UUIDs, network IP, and runtime paths; declare quotas and networking through their corresponding spec fields.

## Templates and eggs

A Template selects exactly one source:

- `native`: Catalyst image, startup/stop commands, installation settings, variables, supported ports, resource defaults, and features.
- `egg.url`: an HTTPS URL returning egg JSON. Change `egg.url` or `egg.revision` to import a new version.
- `egg.configMapKeyRef`: a same-namespace ConfigMap name and key containing egg JSON. Content changes trigger reimport.

```yaml
apiVersion: catalyst.catalystctl.io/v1alpha1
kind: Template
metadata:
  name: paper
spec:
  nestRef:
    name: minecraft
  egg:
    url: https://example.com/pinned/egg-paper.json
    revision: "1"
```

Egg conversion supports Pterodactyl image maps and Pelican image arrays, environment variables and validation rules, startup placeholders, install scripts/images/entrypoints, stop commands/signals, configuration-file substitutions, startup/log detection, and feature metadata. Invalid important configuration blocks import instead of being silently dropped. Review `status.warnings`; unknown format versions require compatibility checks. Downloads are bounded to 700 KiB, and URL changes are not polled automatically.

Imported templates retain a canonical ConfigMap snapshot and source digest. Each Server gets a private frozen Catalyst runtime template plus a Secret snapshot of inherited configuration. Updating a shared Template therefore affects future servers, while existing servers keep their initial template. Back up these snapshots together with the custom resources; missing snapshots block reconciliation rather than guessing newer defaults. Catalyst's independent `restartOnExit` feature is disabled so it cannot bypass the operator's recovery budget.

## Networking

The initial implementation uses Catalyst bridge networking. `primaryPort` is explicit; optional `portBindings` maps container port strings to host ports. The primary port receives an identity mapping unless explicitly mapped. To bind a specific address, select an existing Catalyst `allocationId` whose port matches `primaryPort`; `primaryIp` is unsupported for bridge networking and is rejected. Change the allocation and primary port together to switch allocations. Removing an assigned allocation is unsupported by Catalyst's server-update API: restore the reference, select a different allocation, or migrate to a separate Server without an allocation.

Server status exposes TCP/UDP endpoints using the bound address or Catalyst node address and published host ports. An in-cluster proxy connects to those addresses. Your routing and node firewall must permit that path. The operator does not create Services, tunnels, proxy configuration, or game health checks.

## Reconciliation and recovery

Kubernetes owns declared fields. Conflicting Catalyst changes are reconciled back. Checks run every 30 seconds by default; custom-resource, referenced Secret, and ConfigMap changes trigger reconciliation immediately.

Creation, installation, and power requests are separate operations. Intent is persisted before non-idempotent calls, with resource-version checks and Lease leadership limiting concurrent writers. After an ambiguous create response, the operator searches for its Kubernetes UID marker. It never retries creation without a uniquely recoverable result. Preserve those description markers.

Environment and startup changes update Catalyst configuration but leave a running process untouched. Quota and port changes wait for the server to stop. Change `spec.restartToken` to authorize the stop/update/start cycle, or request `powerState: Stopped` to stop and apply the configuration without starting it. Pending changes report `RestartRequired`. Stopping a `Running` server through Catalyst alone is temporary.

Starts use bounded retries with backoff. Five minutes of stable running resets the failure budget. Once `RecoveryRequired` appears, resolve the failure and change `spec.retryToken`. The default budget is five attempts.

Installation completion requires an observed `installing` → `stopped`/`running` transition. Catalyst has no durable installed flag, so a fast installation or lost response can produce `InstallationUnconfirmed`. Inspect Catalyst logs and node files; if installation completed, change `spec.installationConfirmationToken`. Failed installation is resolved manually in Catalyst. The operator never calls reinstall or automatically repeats an ambiguous install.

If a managed server disappears, `Missing` blocks automatic recreation. After deciding how to recover its data, change `spec.replacementToken` to authorize creation of a fresh server. Replacement does not recover old data.

## Status and deletion

```bash
kubectl get servers,templates,nests -n games
kubectl describe server survival -n games
kubectl get server survival -n games -o yaml
```

Status includes `Ready`, `Reconciling`, and `Stalled` conditions, observed generation and time, external IDs/state, pending operation, endpoint addresses, configuration/source digests, warnings, last error/time, retry counts, and recovery guidance. Error messages expose HTTP status, Catalyst error code and operation, and guidance without reflecting API bodies that may contain credentials. `lastError` retains the previous failure even after recovery; inspect current conditions for the current state.

`Ready=True` means initial installation completed, declared configuration is applied, and the requested power state is observed. Pending restarts keep readiness false. It does not verify game-level health or connectivity from your proxy.

All three resources default to `deletionPolicy: Retain`. Their external objects survive CR deletion. `Delete` stops and deletes servers, then removes their private runtime templates. Nests and templates block deletion while dependants remain, including external objects retained in Catalyst. Cleanup warnings or uncertain deletion outcomes retain the finalizer for manual review. Inspect Catalyst/node resources before manually removing `catalyst.catalystctl.io/cleanup`.

## Development

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo run --locked --bin crdgen > charts/catalyst-operator/crds/resources.yaml
helm lint charts/catalyst-operator --set catalyst.url=https://catalyst.example/api
```

The operator targets the Catalyst REST contracts in upstream `main` inspected during implementation. API compatibility is not guaranteed across Catalyst upgrades; validate against your deployed version before rollout. Automated tests use mock Catalyst/Kubernetes endpoints and do not provision real game nodes.

For local execution, set `KUBECONFIG`, `CATALYST_URL`, `CATALYST_API_KEY`, and `POD_NAMESPACE`. `HEALTH_BIND_ADDRESS` defaults to `0.0.0.0:8080`. `POLL_SECONDS`, `MAX_RECOVERY_ATTEMPTS`, and `OPERATION_TIMEOUT_SECONDS` default to `30`, `5`, and `900`. Use the chart's matching values to configure these in-cluster. API credential rotation requires restarting the operator Pod.
