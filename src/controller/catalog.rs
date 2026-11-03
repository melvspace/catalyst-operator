use super::*;

pub(super) async fn dependency<K: Managed>(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<K, Error> {
    let value = Api::<K>::namespaced(
        client, namespace,
    )
    .get_opt(name)
    .await?
    .ok_or_else(
        || {
            blocked(
                "DependencyNotReady",
                format!(
                    "{} {name} does not exist in namespace {namespace}",
                    K::COLLECTION
                ),
            )
        },
    )?;
    let status = value.status();

    if value
        .meta()
        .deletion_timestamp
        .is_some()
        || !status.ready()
        || status.observed_generation != value.meta().generation
    {
        return Err(
            blocked(
                "DependencyNotReady",
                format!(
                    "{} {name} is deleting or has not reconciled its current generation",
                    K::COLLECTION
                ),
            ),
        );
    }

    Ok(value)
}

pub(super) async fn template_body<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    spec: &TemplateSpec,
) -> Result<Value, Error> {
    let namespace = object.namespace().unwrap();
    let nest = match &spec.nest_ref {
        Some(reference) => {
            dependency::<Nest>(
                context.client.clone(),
                &namespace,
                &reference.name,
            )
            .await?
            .status()
            .external_id
        }
        None => None,
    };
    let mut body = match (
        &spec.native,
        &spec.egg,
    ) {
        (Some(native), None) => {
            let mut body = serde_json::to_value(native).unwrap();
            body["installEntrypoint"] = json!(
                native
                    .install_entrypoint
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .unwrap_or("bash")
            );

            body
        }
        (None, Some(source)) => match (
            &source.url,
            &source.config_map_key_ref,
        ) {
            (Some(url), None) => {
                let revision = digest(
                    &(
                        url,
                        &source.revision,
                    ),
                );

                if status.source_revision.as_ref() == Some(&revision) {
                    read_template_snapshot(
                        object, context,
                    )
                    .await?
                } else {
                    let bytes = fetch_egg(url).await?;
                    import_snapshot(
                        object, status, context, &bytes, &revision,
                    )
                    .await?
                }
            }
            (None, Some(reference)) => {
                let cm = Api::<ConfigMap>::namespaced(
                    context.client.clone(),
                    &namespace,
                )
                .get_opt(&reference.name)
                .await?
                .ok_or_else(
                    || {
                        blocked(
                            "DependencyNotReady",
                            format!(
                                "ConfigMap {} is missing",
                                reference.name
                            ),
                        )
                    },
                )?;
                let data = cm
                    .data
                    .as_ref()
                    .and_then(|v| v.get(&reference.key))
                    .ok_or_else(
                        || {
                            blocked(
                                "InvalidSource",
                                format!(
                                    "ConfigMap {} is missing key {}",
                                    reference.name, reference.key
                                ),
                            )
                        },
                    )?;
                let revision = digest(
                    &(
                        reference.name.clone(),
                        reference.key.clone(),
                        data,
                        &source.revision,
                    ),
                );

                if status.source_revision.as_ref() == Some(&revision) {
                    read_template_snapshot(
                        object, context,
                    )
                    .await?
                } else {
                    import_snapshot(
                        object,
                        status,
                        context,
                        data.as_bytes(),
                        &revision,
                    )
                    .await?
                }
            }
            _ => {
                return Err(
                    blocked(
                        "InvalidSource",
                        "Specify exactly one egg.url or egg.configMapKeyRef",
                    ),
                );
            }
        },
        _ => {
            return Err(
                blocked(
                    "InvalidSource",
                    "Specify exactly one native or egg template source",
                ),
            );
        }
    };
    body["name"] = json!(
        spec.name
            .clone()
            .unwrap_or_else(|| object.name_any())
    );
    body["nestId"] = json!(nest);
    let description = body["description"]
        .as_str()
        .unwrap_or("")
        .to_owned();
    body["description"] = json!(
        marked(
            &description,
            object,
            ""
        )
    );
    // Catalyst-side restart loops must not bypass the operator's recovery budget.
    body["features"]["restartOnExit"] = json!(false);

    Ok(body)
}

pub(super) fn egg_http_client() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .https_only(true)
        .redirect(reqwest::redirect::Policy::limited(3))
}

async fn fetch_egg(url: &str) -> Result<Vec<u8>, Error> {
    let parsed = reqwest::Url::parse(url).map_err(
        |_| {
            blocked(
                "InvalidSource",
                "Egg URL is invalid",
            )
        },
    )?;

    if parsed.scheme() != "https"
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(
            blocked(
                "InvalidSource",
                "Egg URL must use HTTPS without embedded credentials",
            ),
        );
    }

    let client = egg_http_client().build().map_err(
        |_| {
            blocked(
                "SourceUnavailable",
                "Could not initialize egg HTTP client",
            )
        },
    )?;
    let response = client
        .get(parsed)
        .send()
        .await
        .map_err(
            |_| {
                blocked(
                    "SourceUnavailable",
                    "Egg URL fetch failed; check connectivity and URL",
                )
            },
        )?;

    if !response.status().is_success() {
        return Err(
            blocked(
                "SourceUnavailable",
                format!(
                    "Egg URL returned HTTP {}",
                    response.status().as_u16()
                ),
            ),
        );
    }

    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(
            |_| {
                blocked(
                    "SourceUnavailable",
                    "Egg download was interrupted",
                )
            },
        )?;

        if bytes.len() + chunk.len() > 700 * 1024 {
            return Err(
                blocked(
                    "InvalidSource",
                    "Egg exceeds 700 KiB snapshot limit",
                ),
            );
        }

        bytes.extend_from_slice(&chunk);
    }

    Ok(bytes)
}

async fn import_snapshot<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    bytes: &[u8],
    revision: &str,
) -> Result<Value, Error> {
    let egg: Value = serde_json::from_slice(bytes).map_err(
        |_| {
            blocked(
                "InvalidSource",
                "Egg is not valid JSON",
            )
        },
    )?;
    let (body, warnings) = egg::convert(&egg).map_err(
        |e| {
            blocked(
                "ImportFailed",
                e.to_string(),
            )
        },
    )?;
    let name = snapshot_name(object);
    let snapshot = json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {
        "name": name, "namespace": object.namespace(), "ownerReferences": [object.controller_owner_ref(&()).unwrap()]
    }, "data": {"template.json": serde_json::to_string(&body).unwrap(), "revision": revision, "digest": digest(&egg)}});
    let api: Api<ConfigMap> = Api::namespaced(
        context.client.clone(),
        &object.namespace().unwrap(),
    );
    api.patch(
        &name,
        &PatchParams::apply("catalyst-operator"),
        &Patch::Apply(snapshot),
    )
    .await?;
    status.snapshot_ref = Some(name);
    status.source_digest = Some(digest(&egg));
    status.source_revision = Some(revision.into());
    status.warnings = warnings;
    save(
        object, status, context,
    )
    .await?;

    Ok(body)
}

async fn read_template_snapshot<K: Managed>(
    object: &K,
    context: &Context,
) -> Result<Value, Error> {
    let cm = Api::<ConfigMap>::namespaced(context.client.clone(), &object.namespace().unwrap()).get_opt(&snapshot_name(object)).await?
        .ok_or_else(|| blocked("SnapshotMissing", "Imported template snapshot is missing. Restore its ConfigMap or change the source revision to explicitly reimport."))?;
    let data = cm
        .data
        .as_ref()
        .and_then(|v| v.get("template.json"))
        .ok_or_else(
            || {
                blocked(
                    "SnapshotInvalid",
                    "Imported snapshot lacks template.json",
                )
            },
        )?;

    serde_json::from_str(data).map_err(
        |_| {
            blocked(
                "SnapshotInvalid",
                "Imported template snapshot is invalid JSON",
            )
        },
    )
}
