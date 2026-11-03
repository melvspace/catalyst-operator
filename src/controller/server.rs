use super::catalog::dependency;
use super::*;

const TEMPLATE_FIELDS: &[&str] = &[
    "image",
    "images",
    "defaultImage",
    "installImage",
    "installEntrypoint",
    "startup",
    "stopCommand",
    "sendSignalTo",
    "variables",
    "installScript",
    "supportedPorts",
    "allocatedMemoryMb",
    "allocatedCpuCores",
    "features",
    "author",
    "version",
];

fn template_fields(value: &Value) -> Value {
    let mut result = json!({});

    for key in TEMPLATE_FIELDS {
        if let Some(value) = value.get(*key) {
            result[*key] = value.clone();
        }
    }

    result["features"]["restartOnExit"] = json!(false);

    result
}

pub(super) fn immutable(spec: &ServerSpec) -> String {
    digest(
        &(
            &spec.template_ref.name,
            &spec.node_id,
            &spec.location_id,
            &spec.owner_id,
        ),
    )
}

async fn server_snapshot<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    spec: &ServerSpec,
) -> Result<Value, Error> {
    let api: Api<Secret> = Api::namespaced(
        context.client.clone(),
        &object.namespace().unwrap(),
    );
    let name = snapshot_name(object);

    if let Some(snapshot) = api.get_opt(&name).await? {
        let data = snapshot
            .data
            .as_ref()
            .and_then(|v| v.get("snapshot.json"))
            .ok_or_else(
                || {
                    blocked(
                        "SnapshotInvalid",
                        "Server snapshot Secret lacks snapshot.json",
                    )
                },
            )?;

        return serde_json::from_slice(&data.0).map_err(
            |_| {
                blocked(
                    "SnapshotInvalid",
                    "Server snapshot Secret has invalid JSON",
                )
            },
        );
    }

    if status.external_id.is_some() || status.runtime_template_id.is_some() {
        return Err(
            blocked(
                "SnapshotMissing",
                "Restore the server snapshot Secret. Template defaults cannot safely be reconstructed from a newer template.",
            ),
        );
    }

    let template = dependency::<Template>(
        context.client.clone(),
        &object.namespace().unwrap(),
        &spec.template_ref.name,
    )
    .await?;
    let id = template.status().external_id.unwrap();
    let source = context
        .catalyst
        .get(
            "templates",
            &id,
        )
        .await?
        .ok_or_else(
            || {
                blocked(
                    "DependencyNotReady",
                    "Referenced Catalyst template no longer exists",
                )
            },
        )?;
    let body = template_fields(&source);
    let mut defaults = json!({});

    if let Some(variables) = body["variables"].as_array() {
        for variable in variables {
            if let (Some(name), Some(value)) = (
                variable["name"].as_str(),
                variable["default"].as_str(),
            ) && !value.is_empty()
            {
                defaults[name] = json!(value);
            }
        }
    }

    let snapshot = json!({"template": body, "environment": defaults, "immutableDigest": immutable(spec)});
    let secret: Secret = serde_json::from_value(json!({"apiVersion": "v1", "kind": "Secret", "metadata": {
        "name": name, "namespace": object.namespace(), "ownerReferences": [object.controller_owner_ref(&()).unwrap()]
    }, "type": "Opaque", "stringData": {"snapshot.json": serde_json::to_string(&snapshot).unwrap()}})).unwrap();
    api.create(
        &PostParams::default(),
        &secret,
    )
    .await?;
    status.snapshot_ref = Some(name);
    status.immutable_digest = Some(immutable(spec));
    save(
        object, status, context,
    )
    .await?;

    Ok(snapshot)
}

async fn environment(
    context: &Context,
    namespace: &str,
    spec: &ServerSpec,
    defaults: &Value,
) -> Result<Value, Error> {
    let mut result = defaults.clone();

    for (name, entry) in &spec.environment {
        let value = match (
            &entry.value,
            &entry.secret_key_ref,
        ) {
            (Some(value), None) => value.clone(),
            (None, Some(reference)) => {
                let secret = Api::<Secret>::namespaced(
                    context.client.clone(),
                    namespace,
                )
                .get_opt(&reference.name)
                .await?
                .ok_or_else(
                    || {
                        blocked(
                            "DependencyNotReady",
                            format!(
                                "Secret {} is missing",
                                reference.name
                            ),
                        )
                    },
                )?;
                let data = secret
                    .data
                    .as_ref()
                    .and_then(|v| v.get(&reference.key))
                    .ok_or_else(
                        || {
                            blocked(
                                "DependencyNotReady",
                                format!(
                                    "Secret {} lacks key {}",
                                    reference.name, reference.key
                                ),
                            )
                        },
                    )?;
                String::from_utf8(data.0.clone()).map_err(
                    |_| {
                        blocked(
                            "InvalidEnvironment",
                            format!(
                                "Secret {} key {} must contain UTF-8",
                                reference.name, reference.key
                            ),
                        )
                    },
                )?
            }
            _ => {
                return Err(
                    blocked(
                        "InvalidEnvironment",
                        format!(
                            "Environment entry {name} needs exactly one value or secretKeyRef"
                        ),
                    ),
                );
            }
        };

        if value.is_empty() || value.len() > 4096 {
            return Err(
                blocked(
                    "InvalidEnvironment",
                    format!(
                        "Environment entry {name} must contain 1–4096 bytes"
                    ),
                ),
            );
        }

        result[name] = json!(value);
    }

    Ok(result)
}

pub(super) async fn update_server(
    context: &Context,
    id: &str,
    updates: &Value,
) -> Result<(), Error> {
    let _name_lock = if let Some(name) = updates.get("name") {
        let guard = context.server_names.lock().await;

        if context
            .catalyst
            .list("servers")
            .await?
            .iter()
            .any(|server| server["id"] != id && server["name"] == *name)
        {
            return Err(
                blocked(
                    "NameConflict",
                    "Another Catalyst server already uses the requested name; choose another spec.name",
                ),
            );
        }

        Some(guard)
    } else {
        None
    };

    ensure_leader(context)?;
    context
        .catalyst
        .request(
            Method::PUT,
            &format!("servers/{id}"),
            Some(updates),
        )
        .await?;

    Ok(())
}

async fn selected_allocation(
    context: &Context,
    spec: &ServerSpec,
) -> Result<Option<Value>, Error> {
    let Some(id) = &spec.allocation_id else {
        return Ok(None);
    };
    let allocation = context
        .catalyst
        .allocation(
            &spec.node_id,
            id,
        )
        .await?
        .ok_or_else(
            || {
                blocked(
                    "InvalidNetwork",
                    "The selected allocation does not exist on the declared node",
                )
            },
        )?;

    if allocation["port"] != spec.primary_port {
        return Err(
            blocked(
                "InvalidNetwork",
                "primaryPort must match the selected Catalyst allocation's port",
            ),
        );
    }

    if !allocation["serverId"].is_null() && !allocation["serverId"].is_string()
    {
        return Err(
            blocked(
                "InvalidResponse",
                "Catalyst allocation has an invalid serverId",
            ),
        );
    }

    Ok(Some(allocation))
}

pub(super) async fn server<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    spec: &ServerSpec,
) -> Result<Action, Error> {
    if spec.primary_ip.is_some() {
        return Err(
            blocked(
                "InvalidNetwork",
                "Bridge networking does not support primaryIp. Remove it and select an allocationId to bind an address.",
            ),
        );
    }

    let mut allocation = selected_allocation(
        context, spec,
    )
    .await?;
    let snapshot = server_snapshot(
        object, status, context, spec,
    )
    .await?;

    if snapshot["immutableDigest"] != immutable(spec) {
        return Err(
            blocked(
                "ImmutableFieldChanged",
                "nodeId, locationId, ownerId and templateRef cannot change after provisioning. Create a separate Server for migration.",
            ),
        );
    }

    if status.runtime_template_id.is_none() {
        let mut body = snapshot["template"].clone();
        body["name"] = json!(
            format!(
                "k8s-server-{}",
                object.uid().unwrap()
            )
        );
        body["description"] = json!(
            marked(
                "Frozen server template; managed by catalyst-operator",
                object,
                ":runtime"
            )
        );
        body["nestId"] = Value::Null;
        status.runtime_template_id = Some(
            create(
                object,
                status,
                context,
                "templates",
                &body,
                true,
            )
            .await?,
        );
        save(
            object, status, context,
        )
        .await?;
    }

    let runtime_id = status
        .runtime_template_id
        .clone()
        .unwrap();
    let runtime = context.catalyst.get("templates", &runtime_id).await?
        .ok_or_else(|| blocked("SnapshotMissing", "Frozen Catalyst runtime template is missing. Restore it before reconciling this server."))?;
    let frozen = snapshot["template"].clone();

    if differs(
        &runtime, &frozen,
    ) {
        ensure_leader(context)?;
        context
            .catalyst
            .request(
                Method::PUT,
                &format!("templates/{runtime_id}"),
                Some(&frozen),
            )
            .await?;
    }

    let memory = quantity_mb(&spec.resources.memory).map_err(
        |v| {
            blocked(
                "InvalidResources",
                v,
            )
        },
    )?;
    let disk = quantity_mb(&spec.resources.disk).map_err(
        |v| {
            blocked(
                "InvalidResources",
                v,
            )
        },
    )?;

    if !(512..=131072).contains(&memory)
        || !(1024..=1048576).contains(&disk)
        || !(1..=128).contains(&spec.resources.cpu)
    {
        return Err(
            blocked(
                "InvalidResources",
                "Catalyst requires 1–128 CPU cores, 512–131072 MiB memory and 1024–1048576 MiB disk",
            ),
        );
    }

    let mut env = environment(
        context,
        &object.namespace().unwrap(),
        spec,
        &snapshot["environment"],
    )
    .await?;

    for (key, value) in [
        (
            "SERVER_MEMORY",
            memory.to_string(),
        ),
        (
            "SERVER_TOTAL_MEMORY",
            memory.to_string(),
        ),
        (
            "SERVER_TOTAL_DISK",
            disk.to_string(),
        ),
        (
            "SERVER_PORT",
            spec.primary_port.to_string(),
        ),
        (
            "SERVER_PRIMARY_PORT",
            spec.primary_port.to_string(),
        ),
        (
            "PORT",
            spec.primary_port.to_string(),
        ),
        (
            "GAME_PORT",
            spec.primary_port.to_string(),
        ),
    ] {
        if env.get(key).is_some() {
            env[key] = json!(value);
        }
    }

    let mut bindings = spec.port_bindings.clone();
    bindings
        .entry(spec.primary_port.to_string())
        .or_insert(spec.primary_port);

    if bindings.keys().any(
        |key| {
            key.parse::<u16>()
                .ok()
                .is_none_or(|port| port == 0)
        },
    ) || bindings.values().any(|port| *port == 0)
    {
        return Err(
            blocked(
                "InvalidNetwork",
                "Port binding keys and values must be ports between 1 and 65535",
            ),
        );
    }

    let name = object
        .model()
        .name(&object.name_any())
        .to_owned();
    let startup = spec
        .startup_command
        .clone()
        .unwrap_or_else(
            || {
                snapshot["template"]["startup"]
                    .as_str()
                    .unwrap_or("")
                    .into()
            },
        );
    let mut body = json!({"name": name, "description": marked(&spec.description, object, ""),
        "allocatedMemoryMb": memory, "allocatedDiskMb": disk, "allocatedCpuCores": spec.resources.cpu,
        "primaryPort": spec.primary_port, "portBindings": bindings, "environment": env,
        "startupCommand": startup,
    });

    if let Some(allocation) = &spec.allocation_id {
        body["allocationId"] = json!(allocation);
    }

    if status.external_id.is_none() {
        let mut create_body = body.clone();
        create_body
            .as_object_mut()
            .unwrap()
            .remove("startupCommand");
        create_body["templateId"] = json!(runtime_id);
        create_body["nodeId"] = json!(spec.node_id);
        create_body["locationId"] = json!(spec.location_id);
        create_body["networkMode"] = json!("bridge");

        if let Some(owner) = &spec.owner_id {
            create_body["ownerId"] = json!(owner);
        }

        status.external_id = Some(
            create(
                object,
                status,
                context,
                "servers",
                &create_body,
                false,
            )
            .await?,
        );
        status.immutable_digest = Some(immutable(spec));
        status.allocation_id = spec.allocation_id.clone();
        status.replacement_token = spec.replacement_token.clone();
        status.installation_confirmation_token = spec
            .installation_confirmation_token
            .clone();
        save(
            object, status, context,
        )
        .await?;
        allocation = selected_allocation(
            context, spec,
        )
        .await?;
    }

    let id = status.external_id.clone().unwrap();
    let Some(actual) = context
        .catalyst
        .get(
            "servers", &id,
        )
        .await?
    else {
        if status.replacement_token != spec.replacement_token {
            status.external_id = None;
            status.installed = false;
            status.operation = None;
            status.running_digest = None;
            status.running_since = None;
            status.recovery_attempts = 0;
            save(
                object, status, context,
            )
            .await?;

            return Ok(Action::requeue(Duration::from_secs(1)));
        }

        return Err(
            blocked(
                "Missing",
                "Catalyst server is missing. Confirm data loss/recovery requirements, then change spec.replacementToken to authorize a fresh server. This does not restore its data.",
            ),
        );
    };
    observed(
        status,
        context.poll,
    );
    let state = actual["status"]
        .as_str()
        .unwrap_or("unknown");
    status.external_state = Some(state.into());
    status.endpoints = endpoints(&actual);
    observe_running(
        status, state,
    );

    if actual["templateId"] != runtime_id
        || actual["nodeId"] != spec.node_id
        || actual["locationId"] != spec.location_id
    {
        return Err(
            blocked(
                "PlacementDrift",
                "Catalyst placement or template differs from the provisioned identity; restore it manually",
            ),
        );
    }

    if spec.allocation_id.is_none() {
        // Recover ownership even if the assignment's status write was lost.
        if let Some(ip) = actual["primaryIp"].as_str() {
            let port = actual["primaryPort"]
                .as_u64()
                .and_then(|port| u16::try_from(port).ok())
                .ok_or_else(
                    || {
                        blocked(
                            "InvalidResponse",
                            "Catalyst server has an invalid primary port",
                        )
                    },
                )?;

            if let Some(assigned) = context
                .catalyst
                .assigned_allocation(
                    &spec.node_id,
                    &id,
                    ip,
                    port,
                )
                .await?
            {
                status.allocation_id = Some(
                    assigned["id"]
                        .as_str()
                        .ok_or_else(
                            || {
                                blocked(
                                    "InvalidResponse",
                                    "Catalyst allocation has no ID",
                                )
                            },
                        )?
                        .to_owned(),
                );
            }
        }

        if status.allocation_id.is_some() {
            return Err(
                blocked(
                    "AllocationRemovalUnsupported",
                    "Catalyst cannot release an allocation through server updates. Restore spec.allocationId, select another allocation, or migrate to a separate Server without an allocation.",
                ),
            );
        }
    }

    let allocation_changed = if let Some(allocation) = &allocation {
        if allocation["serverId"]
            .as_str()
            .is_some_and(|owner| owner != id)
        {
            return Err(
                blocked(
                    "AllocationConflict",
                    "The selected Catalyst allocation belongs to another server",
                ),
            );
        }

        allocation["serverId"] != id
    } else {
        false
    };

    if !allocation_changed {
        body.as_object_mut()
            .unwrap()
            .remove("allocationId");
    }

    // Compare only managed fields; Catalyst injects runtime variables into environment.
    let mut comparable = body.clone();
    comparable
        .as_object_mut()
        .unwrap()
        .remove("allocationId");
    let mut actual_comparable = actual.clone();
    actual_comparable["environment"] = json!({});

    for key in env.as_object().unwrap().keys() {
        actual_comparable["environment"][key] =
            actual["environment"][key].clone();
    }

    // Removal of an explicitly managed variable must also converge.
    for key in &status.managed_environment_keys {
        if env.get(key).is_none() && actual["environment"].get(key).is_some() {
            actual_comparable["environment"][key] =
                actual["environment"][key].clone();
        }
    }

    let mut updates = comparable.clone();
    updates.as_object_mut().unwrap().retain(
        |key, value| {
            field_differs(
                &actual_comparable,
                key,
                value,
            )
        },
    );

    if allocation_changed {
        updates["allocationId"] = json!(spec.allocation_id);
    }

    if status.installed
        && let Some(action) = power_transition(
            object, status, context, state,
        )?
    {
        return Ok(action);
    }

    let requires_stopped = [
        "allocatedMemoryMb",
        "allocatedCpuCores",
        "allocatedDiskMb",
        "primaryPort",
        "portBindings",
        "allocationId",
    ]
    .iter()
    .any(|field| updates.get(*field).is_some());

    if requires_stopped && state != "stopped" {
        if !status.installed {
            return install(
                object, status, context, spec, &id, state,
            )
            .await;
        }

        if state == "running"
            && spec.power_state == PowerState::Running
            && status.restart_token == spec.restart_token
        {
            condition(
                object,
                status,
                false,
                "RestartRequired",
                "Resource or network changes require a stopped server. Change spec.restartToken or request powerState: Stopped to apply them.",
                true,
            );

            return Ok(Action::requeue(context.poll));
        }

        let mut stopping_spec = spec.clone();
        stopping_spec.power_state = PowerState::Stopped;
        let applied_digest = status
            .configuration_digest
            .clone()
            .unwrap_or_default();

        return power(
            object,
            status,
            context,
            &stopping_spec,
            &id,
            state,
            &applied_digest,
        )
        .await;
    }

    if updates.get("environment").is_some() {
        let mut updated_environment = actual["environment"]
            .as_object()
            .cloned()
            .unwrap_or_default();

        for key in &status.managed_environment_keys {
            updated_environment.remove(key);
        }

        updated_environment.extend(env.as_object().unwrap().clone());
        updates["environment"] = Value::Object(updated_environment);
    }

    if !updates.as_object().unwrap().is_empty() {
        update_server(
            context, &id, &updates,
        )
        .await?;
    }

    status.allocation_id = spec.allocation_id.clone();
    status.managed_environment_keys = spec
        .environment
        .keys()
        .cloned()
        .collect();
    let mut runtime_config = body.clone();
    runtime_config["allocationId"] = json!(spec.allocation_id);
    runtime_config
        .as_object_mut()
        .unwrap()
        .remove("name");
    runtime_config
        .as_object_mut()
        .unwrap()
        .remove("description");
    let desired_digest = digest(&runtime_config);
    status.configuration_digest = Some(desired_digest.clone());

    if !status.installed {
        return install(
            object, status, context, spec, &id, state,
        )
        .await;
    }

    power(
        object,
        status,
        context,
        spec,
        &id,
        state,
        &desired_digest,
    )
    .await
}

pub fn endpoints(actual: &Value) -> Vec<Endpoint> {
    let ip = actual["primaryIp"]
        .as_str()
        .filter(|ip| !["0.0.0.0", "::", ""].contains(ip));
    let host = ip
        .or_else(|| actual["node"]["publicAddress"].as_str())
        .unwrap_or("");

    if host.is_empty() {
        return Vec::new();
    }

    let mut ports: Vec<u16> = actual["portBindings"]
        .as_object()
        .into_iter()
        .flat_map(|v| v.values())
        .filter_map(
            |v| {
                v.as_u64()
                    .and_then(|p| u16::try_from(p).ok())
            },
        )
        .collect();

    if let Some(port) = actual["primaryPort"]
        .as_u64()
        .and_then(|p| u16::try_from(p).ok())
    {
        let host_port = actual["portBindings"][port.to_string()]
            .as_u64()
            .and_then(|p| u16::try_from(p).ok())
            .unwrap_or(port);
        ports.push(host_port);
    }

    ports.sort_unstable();
    ports.dedup();

    ports
        .into_iter()
        .flat_map(
            |port| {
                ["TCP", "UDP"].map(
                    |protocol| Endpoint {
                        host: host.into(),
                        port,
                        protocol: protocol.into(),
                    },
                )
            },
        )
        .collect()
}

pub(super) async fn install<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    spec: &ServerSpec,
    id: &str,
    state: &str,
) -> Result<Action, Error> {
    if status.installation_confirmation_token
        != spec.installation_confirmation_token
    {
        if !["stopped", "running"].contains(&state) {
            return Err(
                blocked(
                    "InstallationUnconfirmed",
                    "Only confirm installation after inspecting a stopped or running server",
                ),
            );
        }

        status.installation_confirmation_token = spec
            .installation_confirmation_token
            .clone();
        status.installed = true;
        status.operation = None;
        save(
            object, status, context,
        )
        .await?;

        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    if let Some(op) = status.operation.as_mut() {
        if op.kind != "Install" {
            return Err(
                blocked(
                    "RecoveryRequired",
                    "An unresolved operation blocks installation; inspect status.operation",
                ),
            );
        }

        if state == "installing" {
            op.observed_active = true;
        } else if op.observed_active && ["stopped", "running"].contains(&state)
        {
            status.installed = true;
            status.operation = None;
            save(
                object, status, context,
            )
            .await?;

            return Ok(Action::requeue(Duration::from_secs(1)));
        } else {
            return Err(
                blocked(
                    "InstallationUnconfirmed",
                    "Initial installation outcome is ambiguous or failed. Inspect Catalyst logs and node files. If installation completed, change spec.installationConfirmationToken. The operator never retries install or calls reinstall automatically.",
                ),
            );
        }

        if expired(
            op,
            context.operation_timeout,
        ) {
            return Err(
                blocked(
                    "InstallationTimeout",
                    "Installation exceeded the operation timeout. Inspect Catalyst/node logs; resolve or cancel it manually. Never confirm an installation still in progress.",
                ),
            );
        }

        condition(
            object,
            status,
            false,
            "Installing",
            "Initial installation is in progress; waiting for an observed completion transition",
            false,
        );

        return Ok(Action::requeue(Duration::from_secs(2)));
    }

    if state != "stopped" {
        return Err(
            blocked(
                "InstallationUnconfirmed",
                "New server was not stopped before initial installation; inspect its external state",
            ),
        );
    }

    status.operation = Some(operation("Install"));
    condition(
        object,
        status,
        false,
        "Installing",
        "Initial installation intent persisted",
        false,
    );
    save(
        object, status, context,
    )
    .await?;
    ensure_leader(context)?;
    context
        .catalyst
        .request(
            Method::POST,
            &format!("servers/{id}/install"),
            Some(&json!({})),
        )
        .await?;
    status
        .operation
        .as_mut()
        .unwrap()
        .accepted = true;

    Ok(Action::requeue(Duration::from_secs(1)))
}

fn observe_running(status: &mut ResourceStatus, state: &str) {
    if state != "running" {
        status.running_since = None;

        return;
    }

    let since = status
        .running_since
        .get_or_insert_with(|| Utc::now().to_rfc3339());

    if chrono::DateTime::parse_from_rfc3339(since)
        .ok()
        .is_some_and(
            |since| {
                Utc::now()
                    .signed_duration_since(since)
                    .num_seconds()
                    >= 300
            },
        )
    {
        status.recovery_attempts = 0;
    }
}

fn power_transition<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    state: &str,
) -> Result<Option<Action>, Error> {
    if let Some(op) = status.operation.clone() {
        let reached = match op.kind.as_str() {
            "Start" => state == "running",
            "Stop" | "RestartStop" => state == "stopped",
            _ => {
                return Err(
                    blocked(
                        "RecoveryRequired",
                        "Unknown pending power operation; inspect status.operation",
                    ),
                );
            }
        };

        if reached {
            status.operation = None;

            if op.kind == "Start" {
                status.running_digest = status
                    .operation_configuration_digest
                    .take();
                status.restart_token = status
                    .operation_restart_token
                    .take()
                    .unwrap_or_default();
            }
        } else if expired(
            &op,
            context.operation_timeout,
        ) || ["error", "crashed"].contains(&state)
        {
            status.operation = None;
        } else {
            condition(
                object,
                status,
                false,
                "PowerTransition",
                "Waiting for Catalyst to reach the requested power state",
                false,
            );

            return Ok(Some(Action::requeue(Duration::from_secs(2))));
        }
    }

    Ok(None)
}

pub(super) async fn power<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    spec: &ServerSpec,
    id: &str,
    state: &str,
    desired_digest: &str,
) -> Result<Action, Error> {
    observe_running(
        status, state,
    );

    if let Some(action) = power_transition(
        object, status, context, state,
    )? {
        return Ok(action);
    }

    let wants_running = spec.power_state == PowerState::Running;
    let restart_requested = status.restart_token != spec.restart_token;

    if ["running", "starting", "crashed", "error"].contains(&state)
        && (!wants_running || restart_requested)
    {
        return power_request(
            object,
            status,
            context,
            spec,
            id,
            if restart_requested {
                "RestartStop"
            } else {
                "Stop"
            },
            desired_digest,
        )
        .await;
    }

    if !wants_running && state == "stopped" {
        status.restart_token = spec.restart_token.clone();
        condition(
            object,
            status,
            true,
            "Stopped",
            "Installed server is stopped and declared configuration is applied",
            false,
        );

        return Ok(Action::requeue(context.poll));
    }

    if wants_running && ["stopped", "crashed", "error"].contains(&state) {
        if status.recovery_attempts >= context.max_recovery {
            return Err(
                blocked(
                    "RecoveryRequired",
                    "Automatic power recovery budget exhausted. Inspect Catalyst/node logs, resolve the failure and change spec.retryToken.",
                ),
            );
        }

        return power_request(
            object,
            status,
            context,
            spec,
            id,
            "Start",
            desired_digest,
        )
        .await;
    }

    if wants_running && state == "running" {
        if status.running_digest.as_deref() != Some(desired_digest) {
            condition(
                object,
                status,
                false,
                "RestartRequired",
                "Configuration is applied in Catalyst; change spec.restartToken to apply it to the running process",
                true,
            );
        } else {
            condition(
                object,
                status,
                true,
                "Running",
                "Installed server is running with the declared configuration; network and game health are not probed",
                false,
            );
        }

        return Ok(Action::requeue(context.poll));
    }

    Err(
        blocked(
            "PowerTransition",
            format!(
                "Waiting for Catalyst state {state}; inspect Catalyst if it remains stuck"
            ),
        ),
    )
}

async fn power_request<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    spec: &ServerSpec,
    id: &str,
    kind: &str,
    desired_digest: &str,
) -> Result<Action, Error> {
    let endpoint = if kind == "Start" { "start" } else { "stop" };

    if let Some(last) = status
        .last_power_attempt_at
        .as_deref()
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
    {
        let backoff =
            (5_i64 * 2_i64.pow(status.recovery_attempts.min(6))).min(300);
        let elapsed = Utc::now()
            .signed_duration_since(last)
            .num_seconds();

        if elapsed < backoff {
            condition(
                object,
                status,
                false,
                "RecoveryBackoff",
                "Waiting before the next bounded power recovery attempt",
                false,
            );

            return Ok(
                Action::requeue(
                    Duration::from_secs((backoff - elapsed) as u64),
                ),
            );
        }
    }

    status.operation = Some(operation(kind));
    status.operation_configuration_digest = Some(desired_digest.into());
    status.operation_restart_token = Some(spec.restart_token.clone());
    status.last_power_attempt_at = Some(Utc::now().to_rfc3339());
    status.running_since = None;

    if endpoint == "start" {
        status.recovery_attempts += 1;
    }

    condition(
        object,
        status,
        false,
        "PowerTransition",
        "Power operation intent persisted; waiting for Catalyst",
        false,
    );
    save(
        object, status, context,
    )
    .await?;
    ensure_leader(context)?;
    context
        .catalyst
        .request(
            Method::POST,
            &format!("servers/{id}/{endpoint}"),
            Some(&json!({})),
        )
        .await?;
    status
        .operation
        .as_mut()
        .unwrap()
        .accepted = true;

    Ok(Action::requeue(Duration::from_secs(2)))
}
