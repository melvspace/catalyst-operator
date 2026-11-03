use super::server::{install, power};
use super::*;
use std::sync::Mutex;
use wiremock::{
    Mock, MockServer, Request, Respond, ResponseTemplate,
    matchers::{method, path},
};

#[derive(Clone)]
struct StatusApi(Arc<Mutex<Value>>);

impl Respond for StatusApi {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        let mut object = self.0.lock().unwrap();
        object["status"] = body["status"].clone();
        let version = object["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            + 1;
        object["metadata"]["resourceVersion"] = json!(version.to_string());

        ResponseTemplate::new(200).set_body_json(object.clone())
    }
}

fn server_object() -> Server {
    serde_json::from_value(json!({"apiVersion": "catalyst.catalystctl.io/v1alpha1", "kind": "Server", "metadata": {
        "name": "game", "namespace": "games", "uid": "uid-123", "resourceVersion": "1", "generation": 1,
        "finalizers": [FINALIZER]
    }, "spec": {"templateRef": {"name": "paper"}, "nodeId": "node", "locationId": "location",
        "resources": {"cpu": 2, "memory": "2Gi", "disk": "20Gi"}, "primaryPort": 25565
    }})).unwrap()
}

async fn fixture() -> (
    MockServer,
    MockServer,
    Context,
    Server,
) {
    let (kube, catalyst, context, object, _) =
        fixture_for(server_object()).await;

    (
        kube, catalyst, context, object,
    )
}

async fn fixture_for<K: Managed>(
    object: K,
) -> (
    MockServer,
    MockServer,
    Context,
    K,
    Arc<Mutex<Value>>,
) {
    let kube_server = MockServer::start().await;
    let catalyst_server = MockServer::start().await;
    let kube_object =
        Arc::new(Mutex::new(serde_json::to_value(&object).unwrap()));
    Mock::given(method("PATCH"))
        .and(path(format!(
            "/apis/catalyst.catalystctl.io/v1alpha1/namespaces/games/{}/game/status",
            K::COLLECTION,
        )))
        .respond_with(StatusApi(kube_object.clone()))
        .mount(&kube_server)
        .await;
    let config = kube::Config::new(kube_server.uri().parse().unwrap());
    let leader = Arc::new(crate::runtime::Leadership::default());
    leader.renew();
    let context = Context {
        client: Client::try_from(config).unwrap(),
        catalyst: Catalyst::new(
            &catalyst_server.uri(),
            "test".into(),
        )
        .unwrap(),
        leader,
        poll: Duration::from_secs(30),
        max_recovery: 5,
        operation_timeout: 900,
    };

    (
        kube_server,
        catalyst_server,
        context,
        object,
        kube_object,
    )
}

#[derive(Clone)]
struct ServerApi {
    actual: Arc<Mutex<Value>>,
    allocations: Arc<Mutex<Vec<Value>>>,
}

impl Respond for ServerApi {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let mut actual = self.actual.lock().unwrap();

        if request.method == "PUT" {
            let body: Value = serde_json::from_slice(&request.body).unwrap();

            if actual["status"] != "stopped"
                && [
                    "allocatedMemoryMb",
                    "allocatedCpuCores",
                    "allocatedDiskMb",
                    "primaryPort",
                    "portBindings",
                    "primaryIp",
                    "allocationId",
                ]
                .iter()
                .any(|field| body.get(*field).is_some())
            {
                return ResponseTemplate::new(409)
                    .set_body_json(json!({"code": "SERVER_NOT_STOPPED"}));
            }

            if body.get("primaryIp").is_some() {
                return ResponseTemplate::new(400).set_body_json(
                    json!({"code": "SERVER_NETWORK_MODE_INVALID"}),
                );
            }

            actual
                .as_object_mut()
                .unwrap()
                .extend(body.as_object().unwrap().clone());

            if let Some(id) = body["allocationId"].as_str() {
                let mut allocations = self.allocations.lock().unwrap();

                if allocations
                    .iter()
                    .any(|row| row["id"] == id && !row["serverId"].is_null())
                {
                    return ResponseTemplate::new(409).set_body_json(
                        json!({"code": "ALLOCATION_ALREADY_ASSIGNED"}),
                    );
                }

                for row in allocations.iter_mut() {
                    if row["serverId"] == "id" {
                        row["serverId"] = Value::Null;
                    }

                    if row["id"] == id {
                        row["serverId"] = json!("id");
                        actual["primaryIp"] = row["ip"].clone();
                        actual["primaryPort"] = row["port"].clone();
                        actual["environment"]["CATALYST_NETWORK_IP"] =
                            row["ip"].clone();
                    }
                }
            }
        } else if request.method == "POST" {
            actual["status"] = json!(
                if request.url.path().ends_with("/stop") {
                    "stopped"
                } else {
                    "running"
                }
            );
        }

        ResponseTemplate::new(200)
            .set_body_json(json!({"data": actual.clone()}))
    }
}

struct ServerFixture {
    _kube: MockServer,
    catalyst: MockServer,
    context: Context,
    object: Server,
    status: ResourceStatus,
    kube_object: Arc<Mutex<Value>>,
    backend: ServerApi,
}

impl ServerFixture {
    async fn reconcile(&mut self) -> Result<Action, Error> {
        let spec = self.object.spec.clone();
        self.kube_object.lock().unwrap()["spec"] =
            serde_json::to_value(&spec).unwrap();

        server(
            &mut self.object,
            &mut self.status,
            &self.context,
            &spec,
        )
        .await
    }
}

async fn server_fixture(state: &str) -> ServerFixture {
    use std::collections::BTreeMap;

    let mut object = server_object();

    if state == "stopped" {
        object.spec.power_state = PowerState::Stopped;
    }

    object.spec.environment.insert(
        "SETTING".into(),
        EnvValue {
            value: Some("old".into()),
            secret_key_ref: None,
        },
    );
    let status = ResourceStatus {
        external_id: Some("id".into()),
        runtime_template_id: Some("runtime".into()),
        installed: true,
        managed_environment_keys: vec!["SETTING".into()],
        ..Default::default()
    };
    object.status = Some(status.clone());
    let (kube, catalyst, context, object, kube_object) =
        fixture_for(object).await;
    let template = json!({"image": "java:21", "startup": "java server.jar", "features": {"restartOnExit": false}});
    let snapshot = json!({"template": template, "environment": {}, "immutableDigest": super::server::immutable(&object.spec)});
    let secret = Secret {
        data: Some(
            BTreeMap::from(
                [(
                    "snapshot.json".into(),
                    k8s_openapi::ByteString(
                        serde_json::to_vec(&snapshot).unwrap(),
                    ),
                )],
            ),
        ),
        ..Default::default()
    };
    Mock::given(method("GET"))
        .and(path("/api/v1/namespaces/games/secrets/catalyst-uid-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(secret))
        .mount(&kube)
        .await;
    Mock::given(method("GET"))
        .and(path("/templates/runtime"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": template})),
        )
        .mount(&catalyst)
        .await;
    let actual = json!({"id": "id", "name": "game", "description": marker(&object, ""),
        "templateId": "runtime", "nodeId": "node", "locationId": "location", "status": state,
        "allocatedMemoryMb": 2048, "allocatedCpuCores": 2, "allocatedDiskMb": 20480,
        "primaryPort": 25565, "portBindings": {"25565": 25565}, "startupCommand": "java server.jar",
        "environment": {"SETTING": "old", "TEMPLATE_IMAGE": "java:21", "CATALYST_NETWORK_IP": "192.0.2.1"}
    });
    let backend = ServerApi {
        actual: Arc::new(Mutex::new(actual)),
        allocations: Arc::new(Mutex::new(Vec::new())),
    };

    for (verb, route) in [
        (
            "GET",
            "/servers/id",
        ),
        (
            "PUT",
            "/servers/id",
        ),
        (
            "POST",
            "/servers/id/stop",
        ),
        (
            "POST",
            "/servers/id/start",
        ),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(backend.clone())
            .mount(&catalyst)
            .await;
    }

    let allocations = backend.allocations.clone();
    Mock::given(method("GET"))
        .and(path("/nodes/node/allocations"))
        .respond_with(
            move |_: &Request| {
                ResponseTemplate::new(200).set_body_json(
                    json!({"data": allocations.lock().unwrap().clone()}),
                )
            },
        )
        .mount(&catalyst)
        .await;
    let mut fixture = ServerFixture {
        _kube: kube,
        catalyst,
        context,
        object,
        status,
        kube_object,
        backend,
    };
    fixture.reconcile().await.unwrap();
    fixture.status.running_digest = fixture
        .status
        .configuration_digest
        .clone();

    fixture
}

#[tokio::test]
async fn ambiguous_creation_is_journaled_and_never_blindly_retried() {
    let (_kube, catalyst, context, mut object) = fixture().await;
    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"success": true, "data": []})),
        )
        .mount(&catalyst)
        .await;
    Mock::given(method("POST"))
        .and(path("/servers"))
        .respond_with(
            ResponseTemplate::new(503)
                .set_body_json(json!({"code": "GATEWAY_NOT_AVAILABLE"})),
        )
        .expect(1)
        .mount(&catalyst)
        .await;
    let mut status = object.status();
    let body = json!({"name": "game", "description": marker(&object, "")});
    assert!(
        create(
            &mut object,
            &mut status,
            &context,
            "servers",
            &body,
            false
        )
        .await
        .is_err()
    );
    assert_eq!(
        object.status().operation.unwrap().kind,
        "Create"
    );
    let error = create(
        &mut object,
        &mut status,
        &context,
        "servers",
        &body,
        false,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("CreateOutcomeUnknown")
    );
}

#[tokio::test]
async fn lost_create_response_recovers_only_a_unique_owned_marker() {
    let (_kube, catalyst, context, mut object) = fixture().await;
    let mut status = ResourceStatus {
        operation: Some(operation("Create")),
        ..Default::default()
    };
    Mock::given(method("GET")).and(path("/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": "recovered", "name": "game", "description": marker(&object, "")}]})))
        .mount(&catalyst).await;
    let id = create(
        &mut object,
        &mut status,
        &context,
        "servers",
        &json!({"name": "game"}),
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        id,
        "recovered"
    );
    assert!(status.operation.is_none());
    assert!(
        !catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.method == "POST")
    );
}

#[tokio::test]
async fn existing_name_is_not_adopted() {
    let (_kube, catalyst, context, mut object) = fixture().await;
    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(
                json!({"data": [{"id": "someone-elses", "name": "game"}]}),
            ),
        )
        .mount(&catalyst)
        .await;
    let mut status = ResourceStatus::default();
    let error = create(
        &mut object,
        &mut status,
        &context,
        "servers",
        &json!({"name": "game"}),
        false,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("NameConflict")
    );
    assert!(status.operation.is_none());
}

#[tokio::test]
async fn initial_install_is_not_repeated_when_completion_was_not_observed() {
    let (_kube, catalyst, context, mut object) = fixture().await;
    Mock::given(method("POST"))
        .and(path("/servers/id/install"))
        .respond_with(
            ResponseTemplate::new(202).set_body_json(json!({"accepted": true})),
        )
        .expect(1)
        .mount(&catalyst)
        .await;
    let spec = object.spec.clone();
    let mut status = ResourceStatus::default();
    install(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "stopped",
    )
    .await
    .unwrap();
    assert!(
        status
            .operation
            .as_ref()
            .unwrap()
            .accepted
    );
    let error = install(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "stopped",
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("InstallationUnconfirmed")
    );
    assert!(!status.installed);
    install(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "installing",
    )
    .await
    .unwrap();
    install(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "stopped",
    )
    .await
    .unwrap();
    assert!(status.installed);
}

#[tokio::test]
async fn start_completion_does_not_claim_configuration_changed_mid_start() {
    let (_kube, _catalyst, context, mut object) = fixture().await;
    let spec = object.spec.clone();
    let mut status = ResourceStatus {
        installed: true,
        operation: Some(operation("Start")),
        operation_configuration_digest: Some("old".into()),
        operation_restart_token: Some(String::new()),
        ..Default::default()
    };
    power(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "running",
        "new",
    )
    .await
    .unwrap();
    assert_eq!(
        status.running_digest.as_deref(),
        Some("old")
    );
    assert!(!status.ready());
    assert_eq!(
        status.conditions[0].reason,
        "RestartRequired"
    );
}

#[tokio::test]
async fn exhausted_recovery_budget_prevents_another_start() {
    let (_kube, catalyst, context, mut object) = fixture().await;
    let spec = object.spec.clone();
    let mut status = ResourceStatus {
        installed: true,
        recovery_attempts: 5,
        ..Default::default()
    };
    let error = power(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "crashed",
        "digest",
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("RecoveryRequired")
    );
    assert!(
        catalyst
            .received_requests()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn cleanup_warnings_survive_a_missing_external_record() {
    let (_kube, catalyst, context, mut object) = fixture().await;
    object.spec.deletion_policy = DeletionPolicy::Delete;
    let mut status = ResourceStatus {
        external_id: Some("id".into()),
        ..Default::default()
    };
    Mock::given(method("GET"))
        .and(path("/servers/id"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"data": {"status": "stopped"}})),
        )
        .up_to_n_times(1)
        .mount(&catalyst)
        .await;
    Mock::given(method("GET"))
        .and(path("/servers/id"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"code": "SERVER_NOT_FOUND"})),
        )
        .with_priority(10)
        .mount(&catalyst)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/servers/id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"success": true, "agentCleanup": false, "warning": "offline"}),
        ))
        .expect(1)
        .mount(&catalyst)
        .await;
    assert!(
        cleanup(
            &mut object,
            &mut status,
            &context
        )
        .await
        .is_err()
    );
    assert_eq!(
        status.operation.as_ref().unwrap().kind,
        "CleanupUnconfirmed"
    );
    assert!(
        cleanup(
            &mut object,
            &mut status,
            &context
        )
        .await
        .is_err()
    );
    assert!(
        object
            .finalizers()
            .iter()
            .any(|v| v == FINALIZER)
    );
}

#[test]
fn condition_transition_time_changes_only_when_boolean_changes() {
    let object = server_object();
    let mut status = ResourceStatus::default();
    condition(
        &object,
        &mut status,
        false,
        "Installing",
        "installing",
        false,
    );
    let original = status.conditions[0]
        .last_transition_time
        .clone();
    condition(
        &object,
        &mut status,
        false,
        "Starting",
        "starting",
        false,
    );
    assert_eq!(
        status.conditions[0].last_transition_time,
        original
    );
    assert_eq!(
        status.conditions[0].reason,
        "Starting"
    );
}

#[test]
fn endpoint_falls_back_to_node_address_for_wildcard_bindings() {
    let result = endpoints(
        &json!({"primaryIp": "0.0.0.0", "primaryPort": 25565, "node": {"publicAddress": "games.example"}}),
    );
    assert_eq!(
        result.len(),
        2
    );
    assert_eq!(
        result[0].host,
        "games.example"
    );
}

#[tokio::test]
async fn running_secret_updates_stage_without_stopped_only_fields() {
    let mut fixture = server_fixture("running").await;
    fixture.object.spec.environment.insert(
        "SETTING".into(),
        EnvValue {
            value: None,
            secret_key_ref: Some(
                KeyRef {
                    name: "settings".into(),
                    key: "value".into(),
                },
            ),
        },
    );
    let secret = Secret {
        data: Some(
            std::collections::BTreeMap::from(
                [(
                    "value".into(),
                    k8s_openapi::ByteString(b"rotated".to_vec()),
                )],
            ),
        ),
        ..Default::default()
    };
    Mock::given(method("GET"))
        .and(path("/api/v1/namespaces/games/secrets/settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(secret))
        .mount(&fixture._kube)
        .await;
    fixture.reconcile().await.unwrap();
    assert_eq!(
        fixture.status.conditions[0].reason,
        "RestartRequired"
    );
    let actual = fixture
        .backend
        .actual
        .lock()
        .unwrap()
        .clone();
    assert_eq!(
        actual["status"],
        "running"
    );
    assert_eq!(
        actual["environment"]["SETTING"],
        "rotated"
    );
    assert_eq!(
        actual["environment"]["CATALYST_NETWORK_IP"],
        "192.0.2.1"
    );
    let requests = fixture
        .catalyst
        .received_requests()
        .await
        .unwrap();
    let updates: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "PUT")
        .collect();
    assert_eq!(
        updates.len(),
        1
    );
    let body: Value = serde_json::from_slice(&updates[0].body).unwrap();
    assert_eq!(
        body.as_object().unwrap().len(),
        1
    );
    assert!(body.get("environment").is_some());

    fixture.object.spec.restart_token = "rotate".into();
    fixture.reconcile().await.unwrap();
    assert_eq!(
        fixture.backend.actual.lock().unwrap()["status"],
        "stopped"
    );
}

#[tokio::test]
async fn resource_edits_wait_for_authorized_stop_then_apply_before_start() {
    let mut fixture = server_fixture("running").await;
    fixture.object.spec.resources.memory = "3Gi".into();
    fixture.reconcile().await.unwrap();
    assert_eq!(
        fixture.status.conditions[0].reason,
        "RestartRequired"
    );
    assert_eq!(
        fixture.backend.actual.lock().unwrap()["status"],
        "running"
    );
    assert_eq!(
        fixture.backend.actual.lock().unwrap()["allocatedMemoryMb"],
        2048
    );

    fixture.object.spec.restart_token = "resize".into();
    fixture.reconcile().await.unwrap();
    assert_eq!(
        fixture.backend.actual.lock().unwrap()["status"],
        "stopped"
    );
    fixture.status.last_power_attempt_at = None;
    fixture.reconcile().await.unwrap();
    fixture.reconcile().await.unwrap();
    assert!(fixture.status.ready());
    let actual = fixture
        .backend
        .actual
        .lock()
        .unwrap()
        .clone();
    assert_eq!(
        actual["status"],
        "running"
    );
    assert_eq!(
        actual["allocatedMemoryMb"],
        3072
    );
    let requests = fixture
        .catalyst
        .received_requests()
        .await
        .unwrap();
    let mutations: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "PUT" || request.method == "POST")
        .map(
            |request| {
                (
                    request.method.to_string(),
                    request.url.path().to_owned(),
                )
            },
        )
        .collect();
    assert_eq!(
        mutations,
        vec![
            (
                "POST".into(),
                "/servers/id/stop".into()
            ),
            (
                "PUT".into(),
                "/servers/id".into()
            ),
            (
                "POST".into(),
                "/servers/id/start".into()
            ),
        ]
    );
}

#[tokio::test]
async fn stopped_power_request_is_not_blocked_by_pending_resource_edits() {
    let mut fixture = server_fixture("running").await;
    fixture.object.spec.resources.cpu = 3;
    fixture.object.spec.power_state = PowerState::Stopped;
    fixture.reconcile().await.unwrap();
    fixture.reconcile().await.unwrap();
    assert!(fixture.status.ready());
    let actual = fixture.backend.actual.lock().unwrap();
    assert_eq!(
        actual["status"],
        "stopped"
    );
    assert_eq!(
        actual["allocatedCpuCores"],
        3
    );
}

#[tokio::test]
async fn allocation_switch_validates_selected_port_and_recovers_status() {
    let mut fixture = server_fixture("stopped").await;
    fixture.object.spec.power_state = PowerState::Stopped;
    fixture.status.allocation_id = Some("old".into());
    fixture.object.spec.allocation_id = Some("new".into());
    fixture.object.spec.primary_port = 25566;
    *fixture
        .backend
        .allocations
        .lock()
        .unwrap() = vec![
        json!({"id": "old", "serverId": "id", "ip": "192.0.2.1", "port": 25565}),
        json!({"id": "new", "serverId": null, "ip": "192.0.2.2", "port": 25566}),
    ];
    fixture.reconcile().await.unwrap();
    assert!(fixture.status.ready());
    assert_eq!(
        fixture.backend.actual.lock().unwrap()["primaryPort"],
        25566
    );
    assert_eq!(
        fixture.status.allocation_id.as_deref(),
        Some("new")
    );

    // Simulate losing the status write after Catalyst committed the allocation.
    fixture.status.allocation_id = Some("old".into());
    fixture.reconcile().await.unwrap();
    assert!(fixture.status.ready());
    assert_eq!(
        fixture.status.allocation_id.as_deref(),
        Some("new")
    );
    assert_eq!(
        fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method == "PUT")
            .count(),
        1
    );
    let allocations = fixture
        .backend
        .allocations
        .lock()
        .unwrap();
    assert!(allocations[0]["serverId"].is_null());
    assert_eq!(
        allocations[1]["serverId"],
        "id"
    );
}

#[tokio::test]
async fn allocation_removal_keeps_the_applied_reference_and_reports_guidance() {
    let mut fixture = server_fixture("stopped").await;
    fixture.status.allocation_id = Some("old".into());
    let error = fixture.reconcile().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("AllocationRemovalUnsupported")
    );
    assert!(
        error
            .to_string()
            .contains("Restore spec.allocationId")
    );
    assert_eq!(
        fixture.status.allocation_id.as_deref(),
        Some("old")
    );
    assert!(
        !fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.method == "PUT")
    );
}

#[tokio::test]
async fn primary_ip_is_rejected_before_external_provisioning() {
    let (_kube, catalyst, context, mut object) = fixture().await;
    object.spec.primary_ip = Some("192.0.2.1".into());
    let spec = object.spec.clone();
    let error = server(
        &mut object,
        &mut ResourceStatus::default(),
        &context,
        &spec,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Bridge networking does not support primaryIp")
    );
    assert!(
        catalyst
            .received_requests()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn server_rename_reports_conflicts_without_updating() {
    let mut fixture = server_fixture("stopped").await;
    fixture.object.spec.name = Some("taken".into());
    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [
            {"id": "id", "name": "game"}, {"id": "other", "name": "taken"}
        ]})))
        .mount(&fixture.catalyst).await;
    let error = fixture.reconcile().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("NameConflict")
    );
    assert!(
        !fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.method == "PUT")
    );
}

#[tokio::test]
async fn recovery_budget_counts_stable_running_time_instead_of_start_duration()
{
    let (_kube, _catalyst, context, mut object) = fixture().await;
    let spec = object.spec.clone();
    let old = (Utc::now() - chrono::Duration::minutes(6)).to_rfc3339();
    let mut status = ResourceStatus {
        installed: true,
        recovery_attempts: 5,
        last_power_attempt_at: Some(old.clone()),
        operation: Some(
            Operation {
                started_at: old.clone(),
                ..operation("Start")
            },
        ),
        operation_configuration_digest: Some("digest".into()),
        ..Default::default()
    };
    power(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "starting",
        "digest",
    )
    .await
    .unwrap();
    power(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "running",
        "digest",
    )
    .await
    .unwrap();
    assert_eq!(
        status.recovery_attempts,
        5
    );
    assert!(status.running_since.is_some());
    let error = power(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "crashed",
        "digest",
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("RecoveryRequired")
    );
    assert!(status.running_since.is_none());

    status.running_since = Some(old);
    power(
        &mut object,
        &mut status,
        &context,
        &spec,
        "id",
        "running",
        "digest",
    )
    .await
    .unwrap();
    assert_eq!(
        status.recovery_attempts,
        0
    );
}

#[tokio::test]
async fn native_template_omitted_entrypoint_converges_with_catalyst_defaults() {
    let mut object: Template = serde_yaml::from_str(
        include_str!("../../examples/native-template.yaml"),
    )
    .unwrap();
    object.metadata = server_object().metadata;
    let (_kube, catalyst, context, mut object, _) = fixture_for(object).await;
    let spec = object.spec.clone();
    let mut status = ResourceStatus::default();
    let mut body = template_body(
        &mut object,
        &mut status,
        &context,
        &spec,
    )
    .await
    .unwrap();
    assert_eq!(
        body["installEntrypoint"],
        "bash"
    );
    let mut actual = body.clone();
    actual["id"] = json!("template");
    actual["installEntrypoint"] = json!("bash");
    Mock::given(method("GET"))
        .and(path("/templates"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": []})),
        )
        .mount(&catalyst)
        .await;
    Mock::given(method("POST"))
        .and(path("/templates"))
        .respond_with(
            ResponseTemplate::new(201).set_body_json(json!({"data": actual})),
        )
        .expect(1)
        .mount(&catalyst)
        .await;
    Mock::given(method("GET"))
        .and(path("/templates/template"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": actual})),
        )
        .mount(&catalyst)
        .await;
    Mock::given(method("PUT"))
        .and(path("/templates/template"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&catalyst)
        .await;
    sync_resource(
        &mut object,
        &mut status,
        &context,
        &mut body,
    )
    .await
    .unwrap();
    sync_resource(
        &mut object,
        &mut status,
        &context,
        &mut body,
    )
    .await
    .unwrap();
    assert!(status.ready());
}

#[tokio::test]
async fn egg_redirects_cannot_downgrade_to_http() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::{TlsAcceptor, rustls};

    let insecure = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&insecure)
        .await;
    let certificate =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(
        certificate.key_pair.serialize_der(),
    );
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            key.into(),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let redirect = format!(
        "HTTP/1.1 302 Found\r\nLocation: {}/egg.json\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        insecure.uri()
    );
    let server = tokio::spawn(
        async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0; 1024];

            while !request.ends_with(b"\r\n\r\n") {
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0 && request.len() < 8192);
                request.extend_from_slice(&chunk[..read]);
            }

            stream
                .write_all(redirect.as_bytes())
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
        },
    );
    let root = reqwest::Certificate::from_der(certificate.cert.der()).unwrap();
    let client = super::catalog::egg_http_client()
        .add_root_certificate(root)
        .build()
        .unwrap();
    let error = client
        .get(
            format!(
                "https://localhost:{}/egg.json",
                address.port()
            ),
        )
        .send()
        .await
        .unwrap_err();
    assert!(error.is_redirect());
    server.await.unwrap();
}
