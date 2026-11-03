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
    }

    if let Some(op) = &status.operation
        && ["Create", "CreateRuntimeTemplate"].contains(&op.kind.as_str())
    {
        // A matching journal makes create recover-only, including under Retain.
        let runtime = op.kind == "CreateRuntimeTemplate";
        let collection = if runtime { "templates" } else { K::COLLECTION };
        let id = create(
            object,
            status,
            context,
            collection,
            &json!({}),
            runtime,
        )
        .await?;

        if runtime {
            status.runtime_template_id = Some(id);
        } else {
            status.external_id = Some(id);
        }

        save(
            object, status, context,
        )
        .await?;
    }

    if K::COLLECTION == "servers" && status.name_operation.is_some() {
        return Err(names::unknown());
    }

    if let Some(op) = &status.operation
        && ["CleanupUnconfirmed", "Delete"].contains(&op.kind.as_str())
    {
        return Err(
            blocked(
                "CleanupUnconfirmed",
                "Delete outcome or node cleanup is unconfirmed. Inspect Catalyst/node resources before manually removing the finalizer.",
            ),
        );
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

    if let Some(id) = status.external_id.clone()
        && let Some(actual) = context
            .catalyst
            .get(
                K::COLLECTION,
                &id,
            )
            .await?
    {
        if let Model::Server(spec) = object.model() {
            let state = actual["status"]
                .as_str()
                .unwrap_or("unknown");
            status.external_state = Some(state.into());
            status.endpoints = server::endpoints(&actual);
            observed(
                status,
                context.poll,
            );

            if status
                .operation
                .as_ref()
                .is_some_and(|op| op.kind == "Install")
            {
                return server::install(
                    object, status, context, &spec, &id, state,
                )
                .await;
            }

            if status.operation.as_ref().is_some_and(
                |op| {
                    ["Start", "Stop", "RestartStop", "DeleteStop"]
                        .contains(&op.kind.as_str())
                },
            ) && let Some(action) = server::power_transition(
                object, status, context, &spec, state,
            )
            .await?
            {
                return Ok(action);
            }

            if state == "installing" {
                return Err(
                    blocked(
                        "DeletionBlocked",
                        "Installation is in progress. Resolve or cancel it in Catalyst before deletion.",
                    ),
                );
            }

            if ["running", "starting"].contains(&state) {
                let digest = status
                    .configuration_digest
                    .clone()
                    .unwrap_or_default();

                return server::power_request(
                    object,
                    status,
                    context,
                    &spec,
                    &id,
                    "DeleteStop",
                    &digest,
                )
                .await;
            }

            if !["stopped", "crashed", "error"].contains(&state) {
                return Err(
                    blocked(
                        "DeletionBlocked",
                        format!(
                            "Waiting for Catalyst state {state} to resolve before deletion. Inspect Catalyst/node logs; no additional Stop is dispatched during an external transition."
                        ),
                    ),
                );
            }
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
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                if !error.ambiguous() {
                    status.operation = None;
                }

                return Err(error.into());
            }
        };

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
