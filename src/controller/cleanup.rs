use super::*;

pub(super) async fn cleanup<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
) -> Result<Action, Error> {
    if !object
        .finalizers()
        .iter()
        .any(|v| v == FINALIZER)
    {
        return Ok(Action::await_change());
    }

    if K::COLLECTION == "servers" {
        names::recover_rename(
            object, status, context,
        )
        .await?;

        if status
            .operation
            .as_ref()
            .is_some_and(|op| op.kind == "Create")
        {
            let name = status
                .name_operation
                .as_ref()
                .map(|pending| pending.name.clone())
                .unwrap_or_else(
                    || {
                        object
                            .model()
                            .name(&object.name_any())
                            .to_owned()
                    },
                );
            status.external_id = Some(
                create(
                    object,
                    status,
                    context,
                    "servers",
                    &json!({"name": name}),
                    false,
                )
                .await?,
            );
            save(
                object, status, context,
            )
            .await?;
        } else if status.name_operation.is_some() {
            return Err(names::unknown());
        }
    }

    if object.model().deletion_policy() == &DeletionPolicy::Retain {
        if K::COLLECTION == "servers" {
            names::retain(
                object, status, context,
            )
            .await?;
        }

        return remove_finalizer(
            object, context,
        )
        .await;
    }

    let namespace = object.namespace().unwrap();

    match object.model() {
        Model::Nest(_) => {
            let children = Api::<Template>::namespaced(
                context.client.clone(),
                &namespace,
            )
            .list(&ListParams::default())
            .await?;
            let names: Vec<_> = children
                .items
                .iter()
                .filter(
                    |v| {
                        v.spec
                            .nest_ref
                            .as_ref()
                            .is_some_and(|r| r.name == object.name_any())
                    },
                )
                .map(ResourceExt::name_any)
                .collect();

            if !names.is_empty() {
                return Err(
                    blocked(
                        "DeletionBlocked",
                        format!(
                            "Delete or detach Template dependants first: {}",
                            names.join(", ")
                        ),
                    ),
                );
            }

            if let Some(id) = &status.external_id {
                let templates = context
                    .catalyst
                    .list("templates")
                    .await?;

                if templates
                    .iter()
                    .any(|v| v["nestId"] == *id)
                {
                    return Err(
                        blocked(
                            "DeletionBlocked",
                            "Catalyst templates still reference this nest, including retained or unmanaged templates",
                        ),
                    );
                }
            }
        }
        Model::Template(_) => {
            let children = Api::<Server>::namespaced(
                context.client.clone(),
                &namespace,
            )
            .list(&ListParams::default())
            .await?;
            let names: Vec<_> = children
                .items
                .iter()
                .filter(|v| v.spec.template_ref.name == object.name_any())
                .map(ResourceExt::name_any)
                .collect();

            if !names.is_empty() {
                return Err(
                    blocked(
                        "DeletionBlocked",
                        format!(
                            "Delete Server dependants first: {}",
                            names.join(", ")
                        ),
                    ),
                );
            }
        }
        Model::Server(_) => {}
    }

    if let Some(op) = &status.operation
        && ["Create", "CreateRuntimeTemplate"].contains(&op.kind.as_str())
    {
        return Err(
            blocked(
                "DeletionBlocked",
                "Creation outcome is unresolved. Recover the external identity before deleting this CR.",
            ),
        );
    }

    if let Some(id) = status.external_id.clone()
        && let Some(actual) = context
            .catalyst
            .get(
                K::COLLECTION,
                &id,
            )
            .await?
    {
        if K::COLLECTION == "servers"
            && !["stopped", "crashed", "error"]
                .contains(&actual["status"].as_str().unwrap_or(""))
        {
            if actual["status"] == "installing" {
                return Err(
                    blocked(
                        "DeletionBlocked",
                        "Installation is in progress. Resolve or cancel it in Catalyst before deletion.",
                    ),
                );
            }

            if status.operation.as_ref().is_none_or(
                |op| {
                    op.kind != "DeleteStop"
                        || expired(
                            op,
                            context.operation_timeout,
                        )
                },
            ) {
                status.operation = Some(operation("DeleteStop"));
                save(
                    object, status, context,
                )
                .await?;
                ensure_leader(context)?;
                context
                    .catalyst
                    .request(
                        Method::POST,
                        &format!("servers/{id}/stop"),
                        Some(&json!({})),
                    )
                    .await?;
            }

            return Err(
                blocked(
                    "Deleting",
                    "Waiting for the server to stop before deletion",
                ),
            );
        }

        status.operation = Some(operation("Delete"));
        save(
            object, status, context,
        )
        .await?;
        ensure_leader(context)?;
        let response = context
            .catalyst
            .request(
                Method::DELETE,
                &format!(
                    "{}/{id}",
                    K::COLLECTION
                ),
                None,
            )
            .await?;

        if response.get("warning").is_some()
            || response
                .get("databaseWarnings")
                .is_some()
            || response.get("agentCleanup") == Some(&json!(false))
        {
            status.warnings.push("Catalyst deleted its record but did not confirm complete agent/database cleanup. Inspect Catalyst/node logs; perform manual cleanup and remove the finalizer only after review.".into());
            status.operation = Some(operation("CleanupUnconfirmed"));
            save(
                object, status, context,
            )
            .await?;

            return Err(
                blocked(
                    "CleanupUnconfirmed",
                    "External record deleted with cleanup warnings; finalizer retained for manual review",
                ),
            );
        }

        status.operation = None;
        save(
            object, status, context,
        )
        .await?;
    }

    if let Some(op) = &status.operation
        && (op.kind == "CleanupUnconfirmed" || op.kind == "Delete")
    {
        return Err(
            blocked(
                "CleanupUnconfirmed",
                "Delete outcome or node cleanup is unconfirmed. Inspect Catalyst/node resources before manually removing the finalizer.",
            ),
        );
    }

    if K::COLLECTION == "servers"
        && let Some(id) = &status.runtime_template_id
        && context
            .catalyst
            .get(
                "templates",
                id,
            )
            .await?
            .is_some()
    {
        ensure_leader(context)?;
        context
            .catalyst
            .request(
                Method::DELETE,
                &format!("templates/{id}"),
                None,
            )
            .await?;
    }

    remove_finalizer(
        object, context,
    )
    .await
}

async fn remove_finalizer<K: Managed>(
    object: &mut K,
    context: &Context,
) -> Result<Action, Error> {
    let finalizers: Vec<_> = object
        .finalizers()
        .iter()
        .filter(|v| v.as_str() != FINALIZER)
        .cloned()
        .collect();
    let api: Api<K> = Api::namespaced(
        context.client.clone(),
        &object.namespace().unwrap(),
    );
    api.patch(&object.name_any(), &PatchParams::default(), &Patch::Merge(json!({"metadata": {"resourceVersion": object.resource_version(), "finalizers": finalizers}}))).await?;

    Ok(Action::await_change())
}
