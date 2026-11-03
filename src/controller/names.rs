use super::*;

const IDENTITY_LABEL: &str = "catalyst.catalystctl.io/name-identity";

fn record_name(name: &str) -> String {
    format!(
        "catalyst-server-name-{}",
        digest(&name)
    )
}

async fn record(
    context: &Context,
    name: &str,
) -> Result<Option<ConfigMap>, Error> {
    let record = Api::<ConfigMap>::namespaced(
        context.client.clone(),
        &context.operator_namespace,
    )
    .get_opt(&record_name(name))
    .await?;

    if let Some(record) = &record {
        let data = record.data.as_ref();

        if record
            .labels()
            .get(IDENTITY_LABEL)
            .map(String::as_str)
            != Some("server")
            || data
                .and_then(|data| data.get("name"))
                .map(String::as_str)
                != Some(name)
            || data
                .and_then(|data| data.get("externalId"))
                .is_none_or(String::is_empty)
        {
            return Err(
                blocked(
                    "NameIdentityInvalid",
                    format!(
                        "Restore the name identity ConfigMap {}/{} before claiming this server name",
                        context.operator_namespace,
                        record.name_any(),
                    ),
                ),
            );
        }
    }

    Ok(record)
}

async fn check_identity(
    context: &Context,
    id: &str,
    name: &str,
) -> Result<(), Error> {
    if context
        .catalyst
        .get(
            "servers", id,
        )
        .await?
        .is_some_and(|server| server["name"] == name)
    {
        return Err(
            blocked(
                "NameConflict",
                format!(
                    "Catalyst server {id} already owns the requested name; choose another spec.name",
                ),
            ),
        );
    }

    Ok(())
}

pub(super) async fn confirm<K: Managed>(
    object: &K,
    context: &Context,
    id: &str,
    name: &str,
) -> Result<(), Error> {
    let api = Api::<ConfigMap>::namespaced(
        context.client.clone(),
        &context.operator_namespace,
    );
    let current = record(
        context, name,
    )
    .await?;
    let data = json!({"name": name, "externalId": id, "resourceNamespace": object.namespace(),
        "resourceName": object.name_any(), "resourceUid": object.uid()});

    if let Some(current) = current {
        let existing = &current.data.as_ref().unwrap()["externalId"];

        if existing != id {
            check_identity(
                context, existing, name,
            )
            .await?;
        }

        if serde_json::to_value(&current.data).unwrap() != data {
            api.patch(&current.name_any(), &PatchParams::default(), &Patch::Merge(json!({
                "metadata": {"resourceVersion": current.resource_version()}, "data": data,
            }))).await?;
        }
    } else {
        let record: ConfigMap = serde_json::from_value(json!({"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": record_name(name), "namespace": context.operator_namespace,
                "labels": {(IDENTITY_LABEL): "server"}}, "data": data,
        })).unwrap();
        api.create(
            &PostParams::default(),
            &record,
        )
        .await?;
    }

    Ok(())
}

pub(super) async fn retain<K: Managed>(
    object: &K,
    status: &ResourceStatus,
    context: &Context,
) -> Result<(), Error> {
    let Some(id) = &status.external_id else {
        return Ok(());
    };
    let name = if let Some(name) = &status.applied_name {
        name.clone()
    } else if let Some(actual) = context
        .catalyst
        .get(
            "servers", id,
        )
        .await?
    {
        actual["name"]
            .as_str()
            .ok_or_else(
                || {
                    blocked(
                        "InvalidResponse",
                        "Catalyst server has no name",
                    )
                },
            )?
            .into()
    } else {
        return Ok(());
    };

    confirm(
        object, context, id, &name,
    )
    .await
}

pub(super) fn operation(name: &str, id: Option<&str>) -> NameOperation {
    NameOperation {
        name: name.into(),
        external_id: id.map(str::to_owned),
        started_at: Utc::now().to_rfc3339(),
    }
}

pub(super) fn persisted<K: Managed>(
    object: &K,
    status: &ResourceStatus,
) -> Result<(), Error> {
    if object.status().name_operation != status.name_operation {
        return Err(
            blocked(
                "SchemaUpgradeRequired",
                "Kubernetes did not preserve status.nameOperation. Apply the current operator CRDs before provisioning or renaming servers; no Catalyst name mutation was sent.",
            ),
        );
    }

    Ok(())
}

pub(super) async fn check<K: Managed>(
    object: &K,
    status: &ResourceStatus,
    context: &Context,
    name: &str,
) -> Result<(), Error> {
    if status.name_operation.is_some() {
        return Err(unknown());
    }

    let servers = Api::<Server>::all(context.client.clone())
        .list(&ListParams::default())
        .await?;

    for server in servers.items {
        if server.uid() == object.uid() {
            continue;
        }

        let status = server.status();
        let legacy_creation = status.name_operation.is_none()
            && status
                .operation
                .as_ref()
                .is_some_and(|op| op.kind == "Create");

        if legacy_creation
            || status
                .name_operation
                .as_ref()
                .is_some_and(|op| op.name == name)
        {
            return Err(
                blocked(
                    "NameConflict",
                    format!(
                        "Server {}/{} has an unresolved name claim. Wait for its recovery, or inspect Catalyst and its status.nameOperation before making another claim. An older unresolved creation can block all names because its requested name was not recorded.",
                        server.namespace().unwrap(),
                        server.name_any(),
                    ),
                ),
            );
        }

        if status.external_id.is_some()
            && status.applied_name.as_deref() == Some(name)
        {
            return Err(
                blocked(
                    "NameConflict",
                    format!(
                        "Server {}/{} has the requested applied Catalyst name; resolve that resource before reusing the name",
                        server.namespace().unwrap(),
                        server.name_any(),
                    ),
                ),
            );
        }

        if status.applied_name.is_none()
            && let Some(id) = &status.external_id
        {
            check_identity(
                context, id, name,
            )
            .await?;
        }
    }

    if let Some(record) = record(
        context, name,
    )
    .await?
    {
        let id = &record.data.as_ref().unwrap()["externalId"];

        if status.external_id.as_deref() != Some(id) {
            check_identity(
                context, id, name,
            )
            .await?;
        }
    }

    Ok(())
}

pub(super) fn unknown() -> Error {
    blocked(
        "NameOutcomeUnknown",
        "A server name operation may still commit in Catalyst. Its status.nameOperation reserves the name across restarts. Wait for the requested name to become visible, or inspect Catalyst/backend requests and recover the created server or complete its rename manually. Clear status.nameOperation only after confirming no request remains in flight and the name is safe to release. Changing retryToken does not release the claim.",
    )
}

pub(super) async fn recover_rename<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
) -> Result<(), Error> {
    let Some(pending) = &status.name_operation else {
        return Ok(());
    };
    let Some(id) = &pending.external_id else {
        // Creation is recovered only through its unique UID marker.
        return Ok(());
    };
    let actual = context
        .catalyst
        .get(
            "servers", id,
        )
        .await?;

    if actual
        .as_ref()
        .is_none_or(|server| server["name"] != pending.name)
    {
        return Err(unknown());
    }

    confirm(
        object,
        context,
        id,
        &pending.name,
    )
    .await?;
    status.applied_name = Some(pending.name.clone());
    status.name_operation = None;
    save(
        object, status, context,
    )
    .await
}
