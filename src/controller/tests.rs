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
    let kube_server = MockServer::start().await;
    let catalyst_server = MockServer::start().await;
    let object = server_object();
    Mock::given(method("PATCH"))
        .and(path(
            "/apis/catalyst.catalystctl.io/v1alpha1/namespaces/games/servers/game/status",
        ))
        .respond_with(StatusApi(Arc::new(Mutex::new(
            serde_json::to_value(&object).unwrap(),
        ))))
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
    )
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
