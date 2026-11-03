mod catalog;
mod cleanup;
mod names;
mod server;

#[cfg(test)]
mod tests;

use catalog::template_body;
use cleanup::cleanup;
use server::server;

pub use server::endpoints;

use std::{fmt::Debug, sync::Arc, time::Duration};

use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::{
    api::core::v1::{ConfigMap, Secret},
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ListParams, Patch, PatchParams, PostParams},
    core::NamespaceResourceScope,
    runtime::{Controller, controller::Action, reflector::ObjectRef, watcher},
};
use reqwest::Method;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    api::*,
    catalyst::{ApiError, Catalyst},
    egg,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Api(#[from] ApiError),
    #[error(
        "Kubernetes request failed; check operator RBAC and API connectivity"
    )]
    Kube(#[from] kube::Error),
    #[error("{reason}: {message}")]
    Blocked {
        reason: &'static str,
        message: String,
    },
}

fn blocked(reason: &'static str, message: impl Into<String>) -> Error {
    Error::Blocked {
        reason,
        message: message.into(),
    }
}

pub struct Context {
    pub client: Client,
    pub catalyst: Catalyst,
    pub poll: Duration,
    pub leader: Arc<crate::runtime::Leadership>,
    pub max_recovery: u32,
    pub operation_timeout: i64,
    pub server_names: tokio::sync::Mutex<()>,
}

#[derive(Clone)]
pub enum Model {
    Server(ServerSpec),
    Template(TemplateSpec),
    Nest(NestSpec),
}

pub trait Managed:
    Clone
    + Debug
    + DeserializeOwned
    + Serialize
    + Resource<DynamicType = (), Scope = NamespaceResourceScope>
    + Send
    + Sync
    + 'static
{
    const COLLECTION: &'static str;
    fn model(&self) -> Model;
    fn status(&self) -> ResourceStatus;
    fn set_status(&mut self, status: ResourceStatus);
}

macro_rules! managed {
    ($kind:ident, $collection:literal) => {
        impl Managed for $kind {
            const COLLECTION: &'static str = $collection;

            fn model(&self) -> Model {
                Model::$kind(self.spec.clone())
            }

            fn status(&self) -> ResourceStatus {
                self.status.clone().unwrap_or_default()
            }

            fn set_status(&mut self, status: ResourceStatus) {
                self.status = Some(status);
            }
        }
    };
}

managed!(
    Server, "servers"
);
managed!(
    Template,
    "templates"
);
managed!(
    Nest, "nests"
);

impl Model {
    fn deletion_policy(&self) -> &DeletionPolicy {
        match self {
            Self::Server(v) => &v.deletion_policy,
            Self::Template(v) => &v.deletion_policy,
            Self::Nest(v) => &v.deletion_policy,
        }
    }

    fn retry_token(&self) -> &str {
        match self {
            Self::Server(v) => &v.retry_token,
            Self::Template(v) => &v.retry_token,
            Self::Nest(v) => &v.retry_token,
        }
    }

    fn name<'a>(&'a self, fallback: &'a str) -> &'a str {
        match self {
            Self::Server(v) => v.name.as_deref().unwrap_or(fallback),
            Self::Template(v) => v.name.as_deref().unwrap_or(fallback),
            Self::Nest(v) => v.name.as_deref().unwrap_or(fallback),
        }
    }
}

pub async fn run<K: Managed>(context: Arc<Context>) {
    use kube::runtime::{WatchStreamExt, reflector};
    use std::hash::{Hash, Hasher};

    let (store, writer) = reflector::store();
    let stream = watcher(
        Api::<K>::all(context.client.clone()),
        watcher::Config::default(),
    )
    .default_backoff()
    .reflect(writer)
    .applied_objects()
    .predicate_filter(
        |object: &K| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            (
                object.meta().generation,
                object
                    .meta()
                    .deletion_timestamp
                    .is_some(),
                object.finalizers(),
            )
                .hash(&mut hasher);

            Some(hasher.finish())
        },
        Default::default(),
    );
    let controller = Controller::for_stream(
        stream,
        store.clone(),
    );
    let secret_store = store.clone();
    let cm_store = store.clone();
    let template_store = store.clone();
    let nest_store = store;
    let mut stream = controller
        .watches(Api::<Secret>::all(context.client.clone()), watcher::Config::default(), move |secret: Secret| {
            secret_store.state().iter().filter(|object| {
                object.namespace() == secret.namespace() && (snapshot_name(object.as_ref()) == secret.name_any() || match object.model() {
                    Model::Server(spec) => spec.environment.values().any(|v| v.secret_key_ref.as_ref().is_some_and(|r| r.name == secret.name_any())),
                    _ => false,
                })
            }).map(|v| ObjectRef::from_obj(v.as_ref())).collect::<Vec<_>>()
        })
        .watches(Api::<ConfigMap>::all(context.client.clone()), watcher::Config::default(), move |cm: ConfigMap| {
            cm_store.state().iter().filter(|object| {
                object.namespace() == cm.namespace() && match object.model() {
                    Model::Template(spec) => snapshot_name(object.as_ref()) == cm.name_any() || spec.egg.as_ref().and_then(|v| v.config_map_key_ref.as_ref()).is_some_and(|r| r.name == cm.name_any()),
                    _ => false,
                }
            }).map(|v| ObjectRef::from_obj(v.as_ref())).collect::<Vec<_>>()
        })
        .watches(Api::<Template>::all(context.client.clone()), watcher::Config::default(), move |template: Template| {
            template_store.state().iter().filter(|object| {
                object.namespace() == template.namespace() && matches!(object.model(), Model::Server(spec) if spec.template_ref.name == template.name_any())
            }).map(|v| ObjectRef::from_obj(v.as_ref())).collect::<Vec<_>>()
        })
        .watches(Api::<Nest>::all(context.client.clone()), watcher::Config::default(), move |nest: Nest| {
            nest_store.state().iter().filter(|object| {
                object.namespace() == nest.namespace() && matches!(object.model(), Model::Template(spec) if spec.nest_ref.as_ref().is_some_and(|r| r.name == nest.name_any()))
            }).map(|v| ObjectRef::from_obj(v.as_ref())).collect::<Vec<_>>()
        })
        .shutdown_on_signal()
        .run(reconcile::<K>, error_policy::<K>, context)
        .boxed();

    while let Some(result) = stream.next().await {
        if result.is_err() {
            tracing::warn!(
                resource = K::COLLECTION,
                "Controller reconciliation or watch failed; retry scheduled"
            );
        }
    }
}

fn error_policy<K: Managed>(
    _: Arc<K>,
    _: &Error,
    context: Arc<Context>,
) -> Action {
    Action::requeue(context.poll)
}

async fn save<K: Managed>(
    object: &mut K,
    status: &ResourceStatus,
    context: &Context,
) -> Result<(), Error> {
    if serde_json::to_value(object.status()).unwrap()
        == serde_json::to_value(status).unwrap()
    {
        return Ok(());
    }

    let api: Api<K> = Api::namespaced(
        context.client.clone(),
        &object.namespace().unwrap(),
    );
    let patch = json!({"metadata": {"resourceVersion": object.resource_version()}, "status": status});
    *object = api
        .patch_status(
            &object.name_any(),
            &PatchParams::default(),
            &Patch::Merge(patch),
        )
        .await?;

    Ok(())
}

fn condition<K: Managed>(
    object: &K,
    status: &mut ResourceStatus,
    ready: bool,
    reason: &str,
    message: &str,
    stalled: bool,
) {
    for (type_, state) in [
        (
            "Ready", ready,
        ),
        (
            "Reconciling",
            !ready && !stalled,
        ),
        (
            "Stalled", stalled,
        ),
    ] {
        let state = if state { "True" } else { "False" };
        let previous = status
            .conditions
            .iter()
            .find(|c| c.type_ == type_);
        let transition = previous
            .filter(|c| c.status == state)
            .map(|c| c.last_transition_time.clone())
            .unwrap_or_else(|| Time(k8s_openapi::jiff::Timestamp::now()));
        let value = Condition {
            type_: type_.into(),
            status: state.into(),
            reason: reason.into(),
            message: message.into(),
            observed_generation: object.meta().generation,
            last_transition_time: transition,
        };

        if let Some(index) = status
            .conditions
            .iter()
            .position(|c| c.type_ == type_)
        {
            status.conditions[index] = value;
        } else {
            status.conditions.push(value);
        }
    }

    status.observed_generation = object.meta().generation;
    status.recovery_action = stalled.then(|| message.into());
}

fn observed(status: &mut ResourceStatus, poll: Duration) {
    let old = status
        .last_observed_at
        .as_deref()
        .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok());

    if old.is_none_or(
        |v| {
            Utc::now()
                .signed_duration_since(v)
                .num_seconds()
                >= poll.as_secs() as i64
        },
    ) {
        status.last_observed_at = Some(Utc::now().to_rfc3339());
    }
}

fn operation(kind: &str) -> Operation {
    Operation {
        kind: kind.into(),
        started_at: Utc::now().to_rfc3339(),
        accepted: false,
        observed_active: false,
    }
}

fn expired(op: &Operation, seconds: i64) -> bool {
    chrono::DateTime::parse_from_rfc3339(&op.started_at)
        .map(
            |v| {
                Utc::now()
                    .signed_duration_since(v)
                    .num_seconds()
                    > seconds
            },
        )
        .unwrap_or(true)
}

fn ensure_leader(context: &Context) -> Result<(), Error> {
    if !context.leader.is_leader() {
        return Err(
            blocked(
                "LeadershipLost",
                "Waiting for the operator's Kubernetes Lease",
            ),
        );
    }

    Ok(())
}

pub async fn reconcile<K: Managed>(
    object: Arc<K>,
    context: Arc<Context>,
) -> Result<Action, Error> {
    if !context.leader.is_leader() {
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    let mut object = object.as_ref().clone();
    let mut status = object.status();
    let model = object.model();

    if status.retry_token != model.retry_token() {
        status.retry_token = model.retry_token().into();
        status.recovery_attempts = 0;
        status.api_failures = 0;
    }

    let result = reconcile_inner(
        &mut object,
        &mut status,
        &context,
    )
    .await;

    match result {
        Ok(action) => {
            status.api_failures = 0;
            save(
                &mut object,
                &status,
                &context,
            )
            .await?;

            Ok(action)
        }
        Err(error) => {
            let (reason, stalled) = match &error {
                Error::Blocked { reason, .. } => (
                    *reason,
                    ![
                        "DependencyNotReady",
                        "Deleting",
                        "Installing",
                        "PowerTransition",
                    ]
                    .contains(reason),
                ),
                Error::Api(_) => (
                    "CatalystApiError",
                    true,
                ),
                Error::Kube(_) => (
                    "KubernetesApiError",
                    true,
                ),
            };

            if status.last_error.as_deref() != Some(&error.to_string()) {
                status.last_error = Some(error.to_string());
                status.last_error_at = Some(Utc::now().to_rfc3339());
            }
            condition(
                &object,
                &mut status,
                false,
                reason,
                &error.to_string(),
                stalled,
            );
            let delay = if matches!(
                error,
                Error::Api(_) | Error::Kube(_)
            ) {
                status.api_failures = status.api_failures.saturating_add(1);
                Duration::from_secs(
                    (5_u64.saturating_mul(
                        2_u64.saturating_pow(status.api_failures.min(6)),
                    ))
                    .min(300),
                )
            } else {
                context.poll
            };
            save(
                &mut object,
                &status,
                &context,
            )
            .await?;

            Ok(Action::requeue(delay))
        }
    }
}

async fn reconcile_inner<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
) -> Result<Action, Error> {
    let model = object.model();

    if object
        .meta()
        .deletion_timestamp
        .is_some()
    {
        return cleanup(
            object, status, context,
        )
        .await;
    }

    if !object
        .finalizers()
        .iter()
        .any(|v| v == FINALIZER)
    {
        let mut finalizers = object.finalizers().to_vec();
        finalizers.push(FINALIZER.into());
        let api: Api<K> = Api::namespaced(
            context.client.clone(),
            &object.namespace().unwrap(),
        );
        *object = api.patch(&object.name_any(), &PatchParams::default(), &Patch::Merge(json!({"metadata": {"resourceVersion": object.resource_version(), "finalizers": finalizers}}))).await?;
    }

    match model {
        Model::Server(spec) => {
            server(
                object, status, context, &spec,
            )
            .await
        }
        Model::Nest(spec) => {
            let mut body = json!({"name": spec.name.unwrap_or_else(|| object.name_any()), "description": marked(&spec.description, object, ""), "icon": spec.icon.unwrap_or_default(), "author": spec.author.unwrap_or_default()});
            sync_resource(
                object, status, context, &mut body,
            )
            .await
        }
        Model::Template(spec) => {
            let body = template_body(
                object, status, context, &spec,
            )
            .await?;
            sync_resource(
                object,
                status,
                context,
                &mut body.clone(),
            )
            .await
        }
    }
}

fn marker<K: Managed>(object: &K, suffix: &str) -> String {
    format!(
        "[catalyst-operator:{}{suffix}]",
        object.uid().expect("Kubernetes UID")
    )
}

fn marked<K: Managed>(description: &str, object: &K, suffix: &str) -> String {
    format!(
        "{}\n{}",
        description.trim(),
        marker(
            object, suffix
        )
    )
    .trim()
    .into()
}

pub fn differs(actual: &Value, desired: &Value) -> bool {
    desired.as_object().unwrap().iter().any(
        |(key, value)| {
            field_differs(
                actual, key, value,
            )
        },
    )
}

fn field_differs(actual: &Value, key: &str, desired: &Value) -> bool {
    let actual = actual.get(key).unwrap_or(&Value::Null);

    !(actual == desired || (actual.is_null() && desired == ""))
}

async fn create<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    collection: &str,
    body: &Value,
    runtime: bool,
) -> Result<String, Error> {
    let _name_lock = if collection == "servers" {
        Some(context.server_names.lock().await)
    } else {
        None
    };

    ensure_leader(context)?;
    let kind = if runtime {
        "CreateRuntimeTemplate"
    } else {
        "Create"
    };
    let expected_marker = marker(
        object,
        if runtime { ":runtime" } else { "" },
    );
    let rows = context
        .catalyst
        .list(collection)
        .await?;
    let own: Vec<_> = rows
        .iter()
        .filter(
            |v| {
                v["description"]
                    .as_str()
                    .unwrap_or("")
                    .lines()
                    .any(|line| line == expected_marker)
            },
        )
        .collect();
    let journaled = status
        .operation
        .as_ref()
        .is_some_and(|v| v.kind == kind);

    if journaled {
        if own.len() == 1 {
            let id = own[0]["id"]
                .as_str()
                .ok_or_else(
                    || {
                        blocked(
                            "InvalidResponse",
                            "Recovered object has no ID",
                        )
                    },
                )?
                .to_owned();
            status.operation = None;
            status.name_operation = None;

            return Ok(id);
        }

        return Err(
            blocked(
                "CreateOutcomeUnknown",
                "Creation was previously attempted. No unique UID-marked result is visible. Inspect Catalyst and restore the correct externalId/runtimeTemplateId in status, or recreate the CR only after confirming no object exists. Changing retryToken never repeats ambiguous creation.",
            ),
        );
    }

    if rows
        .iter()
        .any(|v| v["name"] == body["name"])
        || !own.is_empty()
    {
        return Err(
            blocked(
                "NameConflict",
                "A Catalyst object already uses this name or UID marker; choose another spec.name. Automatic adoption is disabled.",
            ),
        );
    }

    if collection == "servers" {
        let name = body["name"]
            .as_str()
            .expect("server name");
        names::check(
            object, status, context, name,
        )
        .await?;
        status.name_operation = Some(
            names::operation(
                name, None,
            ),
        );
    }

    status.operation = Some(operation(kind));
    condition(
        object,
        status,
        false,
        "Creating",
        "Creation intent recorded before contacting Catalyst",
        false,
    );
    save(
        object, status, context,
    )
    .await?;
    names::persisted(
        object, status,
    )?;
    ensure_leader(context)?;
    let response = context
        .catalyst
        .request(
            Method::POST,
            collection,
            Some(body),
        )
        .await;

    match response {
        Ok(value) => {
            let id = value["data"]["id"].as_str().ok_or_else(|| {
                blocked(
                    "CreateOutcomeUnknown",
                    "Create succeeded without a usable ID; inspect Catalyst before retrying",
                )
            })?;
            status.operation = None;
            status.name_operation = None;

            Ok(id.into())
        }
        Err(error) => {
            if !error.ambiguous() {
                status.operation = None;
                status.name_operation = None;
            }

            Err(error.into())
        }
    }
}

async fn sync_resource<K: Managed>(
    object: &mut K,
    status: &mut ResourceStatus,
    context: &Context,
    body: &mut Value,
) -> Result<Action, Error> {
    if status.external_id.is_none() {
        status.external_id = Some(
            create(
                object,
                status,
                context,
                K::COLLECTION,
                body,
                false,
            )
            .await?,
        );
        save(
            object, status, context,
        )
        .await?;
    }

    let id = status.external_id.clone().unwrap();
    let actual = context.catalyst.get(K::COLLECTION, &id).await?
        .ok_or_else(|| blocked("Missing", "Managed Catalyst object is missing. Restore it or explicitly recreate this CR; automatic replacement is disabled."))?;
    observed(
        status,
        context.poll,
    );

    if differs(
        &actual, body,
    ) {
        ensure_leader(context)?;
        context
            .catalyst
            .request(
                Method::PUT,
                &format!(
                    "{}/{id}",
                    K::COLLECTION
                ),
                Some(body),
            )
            .await?;
    }

    condition(
        object,
        status,
        true,
        "Reconciled",
        "Declared Catalyst configuration is applied",
        false,
    );

    Ok(Action::requeue(context.poll))
}

fn snapshot_name<K: Managed>(object: &K) -> String {
    format!(
        "catalyst-{}",
        object.uid().unwrap()
    )
}
