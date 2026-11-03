use super::*;

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

    status.name_operation = None;
    save(
        object, status, context,
    )
    .await
}
