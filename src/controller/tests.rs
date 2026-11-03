use super::server::{install, power};
use super::*;
use std::sync::Mutex;
use wiremock::{
    Mock, MockServer, Request, Respond, ResponseTemplate,
    matchers::{method, path},
};

#[derive(Clone)]
struct StatusApi(Arc<Mutex<Value>>);

#[derive(Clone, Default)]
struct NameRecords(Arc<Mutex<std::collections::BTreeMap<String, Value>>>);

impl Respond for NameRecords {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let mut records = self.0.lock().unwrap();

        if request.method == "GET" {
            let name = request
                .url
                .path()
                .rsplit('/')
                .next()
                .unwrap();

            return match records.get(name) {
                Some(record) => ResponseTemplate::new(200).set_body_json(record),
                None => ResponseTemplate::new(404).set_body_json(json!({"status": "Failure", "message": "not found", "reason": "NotFound", "code": 404})),
            };
        }

        let mut body: Value = serde_json::from_slice(&request.body).unwrap();
        let name = if request.method == "POST" {
            body["metadata"]["name"]
                .as_str()
                .unwrap()
                .to_owned()
        } else {
            request
                .url
                .path()
                .rsplit('/')
                .next()
                .unwrap()
                .to_owned()
        };

        if request.method == "POST" {
            if records.contains_key(&name) {
                return ResponseTemplate::new(409).set_body_json(json!({"message": "exists", "reason": "AlreadyExists", "code": 409}));
            }

            body["metadata"]["resourceVersion"] = json!("1");
            records.insert(
                name,
                body.clone(),
            );

            return ResponseTemplate::new(201).set_body_json(body);
        }

        let record = records.get_mut(&name).unwrap();

        if record["metadata"]["resourceVersion"]
            != body["metadata"]["resourceVersion"]
        {
            return ResponseTemplate::new(409).set_body_json(json!({"message": "changed", "reason": "Conflict", "code": 409}));
        }

        let version = record["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            + 1;
        record["data"] = body["data"].clone();
        record["metadata"]["resourceVersion"] = json!(version.to_string());

        ResponseTemplate::new(200).set_body_json(record.clone())
    }
}

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

#[derive(Clone)]
struct NameCatalog(Arc<Mutex<Vec<Value>>>);

impl Respond for NameCatalog {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let mut rows = self.0.lock().unwrap();

        if request.method == "GET" {
            if let Some(id) = request
                .url
                .path()
                .strip_prefix("/servers/")
            {
                return match rows.iter().find(|row| row["id"] == id) {
                    Some(row) => ResponseTemplate::new(200)
                        .set_body_json(json!({"data": row})),
                    None => ResponseTemplate::new(404)
                        .set_body_json(json!({"code": "SERVER_NOT_FOUND"})),
                };
            }

            // Capture the catalog before delaying, exposing overlapping checks.
            return ResponseTemplate::new(200)
                .set_body_json(json!({"data": rows.clone()}))
                .set_delay(Duration::from_millis(30));
        }

        let mut body: Value = serde_json::from_slice(&request.body).unwrap();

        if request.method == "POST" {
            body["id"] = json!(
                format!(
                    "new-{}",
                    rows.len()
                )
            );
            rows.push(body.clone());
        } else {
            let id = request
                .url
                .path()
                .rsplit('/')
                .next()
                .unwrap();
            let actual = rows
                .iter_mut()
                .find(|row| row["id"] == id)
                .unwrap();
            actual
                .as_object_mut()
                .unwrap()
                .extend(body.as_object().unwrap().clone());
            body = actual.clone();
        }

        ResponseTemplate::new(200).set_body_json(json!({"data": body}))
    }
}

async fn name_catalog(server: &MockServer, rows: Vec<Value>) -> NameCatalog {
    use wiremock::matchers::path_regex;

    let catalog = NameCatalog(Arc::new(Mutex::new(rows)));
    Mock::given(path_regex("^/servers(?:/[^/]+)?$"))
        .respond_with(catalog.clone())
        .mount(server)
        .await;

    catalog
}

fn one_name_claim<T, U>(left: Result<T, Error>, right: Result<U, Error>) {
    assert_ne!(
        left.is_ok(),
        right.is_ok()
    );
    let error = left.err().or(right.err()).unwrap();
    assert!(
        error
            .to_string()
            .contains("NameConflict")
    );
}

fn restarted(context: &Context) -> Context {
    Context {
        client: context.client.clone(),
        operator_namespace: context.operator_namespace.clone(),
        catalyst: context.catalyst.clone(),
        poll: context.poll,
        leader: context.leader.clone(),
        max_recovery: context.max_recovery,
        operation_timeout: context.operation_timeout,
        server_names: tokio::sync::Mutex::new(()),
    }
}

async fn delayed_name_claim(first_rename: bool, second_rename: bool) {
    let (kube, catalyst, context, mut first, stored) =
        fixture_for(server_object()).await;
    let (mut second, _) = peer_server(
        &kube,
        &first,
        stored.clone(),
    )
    .await;
    let mut rows = Vec::new();

    if first_rename {
        rows.push(json!({"id": "first", "name": "before-first"}));
    }

    if second_rename {
        rows.push(json!({"id": "second", "name": "before-second"}));
    }

    let catalog = name_catalog(
        &catalyst, rows,
    )
    .await;
    Mock::given(method(if first_rename { "PUT" } else { "POST" }))
        .and(
            path(
                if first_rename {
                    "/servers/first"
                } else {
                    "/servers"
                },
            ),
        )
        .respond_with(
            ResponseTemplate::new(504)
                .set_body_json(json!({"code": "GATEWAY_TIMEOUT"})),
        )
        .with_priority(1)
        .up_to_n_times(1)
        .expect(1)
        .mount(&catalyst)
        .await;
    let body = json!({"name": "claimed", "description": marker(&first, "")});
    let updates = json!({"name": "claimed"});
    let mut first_status = ResourceStatus::default();
    let result = if first_rename {
        super::server::update_server(
            &mut first,
            &mut first_status,
            &context,
            "first",
            &updates,
        )
        .await
    } else {
        create(
            &mut first,
            &mut first_status,
            &context,
            "servers",
            &body,
            false,
        )
        .await
        .map(|_| ())
    };
    assert!(result.is_err());
    assert!(first_status.name_operation.is_some());

    // Lose in-memory state and the mutex while the backend has not yet committed.
    let context = restarted(&context);
    let mut first: Server =
        serde_json::from_value(stored.lock().unwrap().clone()).unwrap();
    let mut first_status = first.status();
    let mut second_status = ResourceStatus::default();
    let second_body =
        json!({"name": "claimed", "description": marker(&second, "")});
    let result = if second_rename {
        super::server::update_server(
            &mut second,
            &mut second_status,
            &context,
            "second",
            &updates,
        )
        .await
    } else {
        create(
            &mut second,
            &mut second_status,
            &context,
            "servers",
            &second_body,
            false,
        )
        .await
        .map(|_| ())
    };
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("NameConflict")
    );
    assert!(
        cleanup(
            &mut first,
            &mut first_status,
            &context
        )
        .await
        .is_err()
    );
    assert!(
        first
            .finalizers()
            .iter()
            .any(|value| value == FINALIZER)
    );

    // The original backend request commits after the conflicting CR was blocked.
    if first_rename {
        catalog
            .0
            .lock()
            .unwrap()
            .iter_mut()
            .find(|row| row["id"] == "first")
            .unwrap()["name"] = json!("claimed");
        names::recover_rename(
            &mut first,
            &mut first_status,
            &context,
        )
        .await
        .unwrap();
    } else {
        let mut committed = body.clone();
        committed["id"] = json!("late-first");
        catalog
            .0
            .lock()
            .unwrap()
            .push(committed);
        let id = create(
            &mut first,
            &mut first_status,
            &context,
            "servers",
            &body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            id,
            "late-first"
        );
        first_status.external_id = Some(id);
        save(
            &mut first,
            &first_status,
            &context,
        )
        .await
        .unwrap();
    }

    assert!(first_status.name_operation.is_none());
    assert!(stored.lock().unwrap()["status"]["nameOperation"].is_null());
    assert_eq!(
        catalog
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["name"] == "claimed")
            .count(),
        1
    );
    assert_eq!(
        catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(
                |request| request.method == "POST" || request.method == "PUT"
            )
            .count(),
        1
    );
}

async fn stale_confirmed_name(
    first_rename: bool,
    second_rename: bool,
    retained: bool,
) {
    let (kube, catalyst, context, mut first, stored) =
        fixture_for(server_object()).await;
    let (mut second, peer_stored) = peer_server(
        &kube,
        &first,
        stored.clone(),
    )
    .await;
    let mut rows = Vec::new();

    if first_rename {
        rows.push(json!({"id": "first", "name": "before-first"}));
    }

    if second_rename {
        rows.push(json!({"id": "second", "name": "before-second"}));
    }

    let catalog = name_catalog(
        &catalyst, rows,
    )
    .await;
    let body = json!({"name": "claimed", "description": marker(&first, "")});
    let mut first_status = ResourceStatus::default();

    if first_rename {
        first_status.external_id = Some("first".into());
        super::server::update_server(
            &mut first,
            &mut first_status,
            &context,
            "first",
            &body,
        )
        .await
        .unwrap();
    } else {
        first_status.external_id = Some(
            create(
                &mut first,
                &mut first_status,
                &context,
                "servers",
                &body,
                false,
            )
            .await
            .unwrap(),
        );
    }

    save(
        &mut first,
        &first_status,
        &context,
    )
    .await
    .unwrap();
    assert!(stored.lock().unwrap()["status"]["nameOperation"].is_null());
    assert_eq!(
        stored.lock().unwrap()["status"]["appliedName"],
        "claimed"
    );

    if retained {
        // The CR is gone while its confirmed external identity remains retained.
        server_list(
            &kube,
            vec![peer_stored],
            1,
        )
        .await;
    }

    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": []})),
        )
        .with_priority(1)
        .mount(&catalyst)
        .await;
    let context = restarted(&context);
    let mut second_status = ResourceStatus::default();
    let body = json!({"name": "claimed", "description": marker(&second, "")});
    let result = if second_rename {
        second_status.external_id = Some("second".into());
        super::server::update_server(
            &mut second,
            &mut second_status,
            &context,
            "second",
            &body,
        )
        .await
    } else {
        create(
            &mut second,
            &mut second_status,
            &context,
            "servers",
            &body,
            false,
        )
        .await
        .map(|_| ())
    };
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("NameConflict")
    );
    assert_eq!(
        catalog
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["name"] == "claimed")
            .count(),
        1
    );
    assert_eq!(
        catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(
                |request| request.method == "POST" || request.method == "PUT"
            )
            .count(),
        1
    );
}

#[tokio::test]
async fn confirmed_create_blocks_stale_create_lists_after_restart() {
    for retained in [false, true] {
        stale_confirmed_name(
            false, false, retained,
        )
        .await;
    }
}

#[tokio::test]
async fn confirmed_create_blocks_stale_rename_lists_after_restart() {
    for retained in [false, true] {
        stale_confirmed_name(
            false, true, retained,
        )
        .await;
    }
}

#[tokio::test]
async fn confirmed_rename_blocks_stale_create_lists_after_restart() {
    for retained in [false, true] {
        stale_confirmed_name(
            true, false, retained,
        )
        .await;
    }
}

#[tokio::test]
async fn confirmed_rename_blocks_stale_rename_lists_after_restart() {
    for retained in [false, true] {
        stale_confirmed_name(
            true, true, retained,
        )
        .await;
    }
}

#[tokio::test]
async fn legacy_identity_blocks_claims_when_server_list_is_stale() {
    let mut first = server_object();
    first.status = Some(
        ResourceStatus {
            external_id: Some("legacy".into()),
            ..Default::default()
        },
    );
    let (kube, catalyst, context, first, stored) = fixture_for(first).await;
    let (mut peer, _) = peer_server(
        &kube, &first, stored,
    )
    .await;
    name_catalog(
        &catalyst,
        vec![json!({"id": "legacy", "name": "claimed"})],
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": []})),
        )
        .with_priority(1)
        .mount(&catalyst)
        .await;
    let error = create(
        &mut peer,
        &mut ResourceStatus::default(),
        &context,
        "servers",
        &json!({"name": "claimed"}),
        false,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("NameConflict")
    );
}

#[tokio::test]
async fn confirmed_name_can_be_reused_after_identity_moves_or_disappears() {
    for deleted in [false, true] {
        let (kube, catalyst, context, mut first, stored) =
            fixture_for(server_object()).await;
        let (mut peer, peer_stored) = peer_server(
            &kube, &first, stored,
        )
        .await;
        let catalog = name_catalog(
            &catalyst,
            Vec::new(),
        )
        .await;
        let body = json!({"name": "before", "description": marker(&first, "")});
        let mut status = ResourceStatus::default();
        let id = create(
            &mut first,
            &mut status,
            &context,
            "servers",
            &body,
            false,
        )
        .await
        .unwrap();
        status.external_id = Some(id.clone());
        save(
            &mut first, &status, &context,
        )
        .await
        .unwrap();

        if deleted {
            catalog.0.lock().unwrap().clear();
            server_list(
                &kube,
                vec![peer_stored],
                1,
            )
            .await;
        } else {
            super::server::update_server(
                &mut first,
                &mut status,
                &context,
                &id,
                &json!({"name": "after"}),
            )
            .await
            .unwrap();
        }

        let body = json!({"name": "before", "description": marker(&peer, "")});
        create(
            &mut peer,
            &mut ResourceStatus::default(),
            &context,
            "servers",
            &body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            catalog
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|row| row["name"] == "before")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn ambiguous_create_reserves_name_against_create_after_restart() {
    delayed_name_claim(
        false, false,
    )
    .await;
}

#[tokio::test]
async fn ambiguous_create_reserves_name_against_rename_after_restart() {
    delayed_name_claim(
        false, true,
    )
    .await;
}

#[tokio::test]
async fn ambiguous_rename_reserves_name_against_create_after_restart() {
    delayed_name_claim(
        true, false,
    )
    .await;
}

#[tokio::test]
async fn ambiguous_rename_reserves_name_against_rename_after_restart() {
    delayed_name_claim(
        true, true,
    )
    .await;
}

#[tokio::test]
async fn definitive_name_rejections_release_claims() {
    for rename in [false, true] {
        let (kube, catalyst, context, mut first, stored) =
            fixture_for(server_object()).await;
        let (mut second, _) = peer_server(
            &kube,
            &first,
            stored.clone(),
        )
        .await;
        let catalog = name_catalog(
            &catalyst,
            vec![json!({"id": "first", "name": "before"})],
        )
        .await;
        Mock::given(method(if rename { "PUT" } else { "POST" }))
            .and(path(if rename { "/servers/first" } else { "/servers" }))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(json!({"code": "VALIDATION_ERROR"})),
            )
            .with_priority(1)
            .up_to_n_times(1)
            .mount(&catalyst)
            .await;
        let body =
            json!({"name": "claimed", "description": marker(&first, "")});
        let mut status = ResourceStatus::default();
        let result = if rename {
            super::server::update_server(
                &mut first,
                &mut status,
                &context,
                "first",
                &body,
            )
            .await
        } else {
            create(
                &mut first,
                &mut status,
                &context,
                "servers",
                &body,
                false,
            )
            .await
            .map(|_| ())
        };
        assert!(result.is_err());
        assert!(status.name_operation.is_none());
        save(
            &mut first, &status, &context,
        )
        .await
        .unwrap();
        let second_body =
            json!({"name": "claimed", "description": marker(&second, "")});
        create(
            &mut second,
            &mut ResourceStatus::default(),
            &context,
            "servers",
            &second_body,
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            catalog
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|row| row["name"] == "claimed")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn legacy_ambiguous_creation_blocks_unknown_name_claims() {
    let (kube, _catalyst, context, object, stored) =
        fixture_for(server_object()).await;
    stored.lock().unwrap()["status"] =
        json!({"operation": operation("Create")});
    let (peer, _) = peer_server(
        &kube, &object, stored,
    )
    .await;
    let error = names::check(
        &peer,
        &ResourceStatus::default(),
        &context,
        "another-name",
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("NameConflict")
    );
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
    Mock::given(
        wiremock::matchers::path_regex(
            "^/api/v1/namespaces/operator/configmaps(?:/[^/]+)?$",
        ),
    )
    .respond_with(NameRecords::default())
    .mount(&kube_server)
    .await;
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
    server_list(
        &kube_server,
        vec![kube_object.clone()],
        10,
    )
    .await;
    let patched_object = kube_object.clone();
    Mock::given(method("PATCH"))
        .and(path(format!(
            "/apis/catalyst.catalystctl.io/v1alpha1/namespaces/games/{}/game",
            K::COLLECTION,
        )))
        .respond_with(move |request: &Request| {
            let patch: Value = serde_json::from_slice(&request.body).unwrap();
            let mut object = patched_object.lock().unwrap();
            object["metadata"]["finalizers"] = patch["metadata"]["finalizers"].clone();
            let version = object["metadata"]["resourceVersion"].as_str().unwrap().parse::<u64>().unwrap() + 1;
            object["metadata"]["resourceVersion"] = json!(version.to_string());

            ResponseTemplate::new(200).set_body_json(object.clone())
        })
        .mount(&kube_server)
        .await;
    let config = kube::Config::new(kube_server.uri().parse().unwrap());
    let leader = Arc::new(crate::runtime::Leadership::default());
    leader.renew();
    let context = Context {
        client: Client::try_from(config).unwrap(),
        operator_namespace: "operator".into(),
        catalyst: Catalyst::new(
            &catalyst_server.uri(),
            "test".into(),
        )
        .unwrap(),
        leader,
        poll: Duration::from_secs(30),
        max_recovery: 5,
        operation_timeout: 900,
        server_names: tokio::sync::Mutex::new(()),
    };

    (
        kube_server,
        catalyst_server,
        context,
        object,
        kube_object,
    )
}

async fn server_list(
    kube: &MockServer,
    objects: Vec<Arc<Mutex<Value>>>,
    priority: u8,
) {
    Mock::given(method("GET"))
        .and(path("/apis/catalyst.catalystctl.io/v1alpha1/servers"))
        .respond_with(move |_: &Request| {
            let items: Vec<_> = objects.iter().map(|object| object.lock().unwrap().clone()).collect();

            ResponseTemplate::new(200).set_body_json(json!({
                "apiVersion": "catalyst.catalystctl.io/v1alpha1", "kind": "ServerList", "metadata": {}, "items": items
            }))
        })
        .with_priority(priority)
        .mount(kube)
        .await;
}

async fn peer_server(
    kube: &MockServer,
    original: &Server,
    first: Arc<Mutex<Value>>,
) -> (
    Server,
    Arc<Mutex<Value>>,
) {
    let mut peer = original.clone();
    peer.metadata.namespace = Some("other-games".into());
    peer.metadata.uid = Some("another-uid".into());
    let stored = Arc::new(Mutex::new(serde_json::to_value(&peer).unwrap()));
    Mock::given(method("PATCH"))
        .and(path("/apis/catalyst.catalystctl.io/v1alpha1/namespaces/other-games/servers/game/status"))
        .respond_with(StatusApi(stored.clone()))
        .mount(kube).await;
    server_list(
        kube,
        vec![first, stored.clone()],
        5,
    )
    .await;

    (
        peer, stored,
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

        if request.method == "GET" && actual.is_null() {
            return ResponseTemplate::new(404)
                .set_body_json(json!({"code": "SERVER_NOT_FOUND"}));
        }

        if request.method == "DELETE" {
            *actual = Value::Null;

            return ResponseTemplate::new(200)
                .set_body_json(json!({"success": true}));
        }

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

            // Catalyst validates bridge ports before resolving allocationId.
            let ip = body["environment"]["CATALYST_NETWORK_IP"]
                .as_str()
                .or_else(|| actual["primaryIp"].as_str())
                .unwrap_or("0.0.0.0");
            let bindings = body
                .get("portBindings")
                .unwrap_or(&actual["portBindings"]);

            if self
                .allocations
                .lock()
                .unwrap()
                .iter()
                .any(
                    |row| {
                        row["serverId"]
                            .as_str()
                            .is_some_and(|owner| owner != "id")
                            && (row["ip"] == ip
                                || ["0.0.0.0", "::"].contains(&ip))
                            && bindings
                                .as_object()
                                .unwrap()
                                .values()
                                .any(|port| *port == row["port"])
                    },
                )
            {
                return ResponseTemplate::new(400)
                    .set_body_json(json!({"code": "PORT_ALREADY_IN_USE"}));
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
        } else if request.method == "PATCH" {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            actual["restartPolicy"] = body["restartPolicy"].clone();

            return ResponseTemplate::new(200).set_body_json(
                json!({"success": true, "restartPolicy": actual["restartPolicy"]}),
            );
        } else if request.method == "POST" {
            if request.url.path().ends_with("/start") {
                actual["agentAutoRestartEnabled"] =
                    json!(actual["restartPolicy"] != "never");
            }

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
    async fn reconcile_persisted(&mut self) {
        self.object.status = Some(self.status.clone());
        self.kube_object.lock().unwrap()["spec"] =
            serde_json::to_value(&self.object.spec).unwrap();
        reconcile(
            Arc::new(self.object.clone()),
            Arc::new(restarted(&self.context)),
        )
        .await
        .unwrap();
        self.object =
            serde_json::from_value(self.kube_object.lock().unwrap().clone())
                .unwrap();
        self.status = self.object.status();
    }

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
        "templateId": "runtime", "nodeId": "node", "locationId": "location", "status": state, "restartPolicy": "never",
        "allocatedMemoryMb": 2048, "allocatedCpuCores": 2, "allocatedDiskMb": 20480,
        "primaryIp": "192.0.2.1", "primaryPort": 25565, "portBindings": {"25565": 25565}, "startupCommand": "java server.jar",
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
        (
            "PATCH",
            "/servers/id/restart-policy",
        ),
        (
            "DELETE",
            "/servers/id",
        ),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(backend.clone())
            .mount(&catalyst)
            .await;
    }

    Mock::given(method("DELETE"))
        .and(path("/templates/runtime"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"success": true})),
        )
        .mount(&catalyst)
        .await;
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
    Mock::given(method("GET")).and(path("/servers/recovered"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": {
            "id": "recovered", "name": "game", "description": marker(&object, "")
        }}))).mount(&catalyst).await;
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
async fn missing_name_journal_schema_prevents_external_creation() {
    let (kube, catalyst, context, mut object) = fixture().await;
    let original = serde_json::to_value(&object).unwrap();
    Mock::given(method("PATCH"))
        .and(path("/apis/catalyst.catalystctl.io/v1alpha1/namespaces/games/servers/game/status"))
        .respond_with(move |request: &Request| {
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            let mut response = original.clone();
            response["status"] = body["status"].clone();
            response["status"].as_object_mut().unwrap().remove("nameOperation");
            response["metadata"]["resourceVersion"] = json!("2");

            ResponseTemplate::new(200).set_body_json(response)
        }).with_priority(1).mount(&kube).await;
    name_catalog(
        &catalyst,
        Vec::new(),
    )
    .await;
    let body = json!({"name": "claimed", "description": marker(&object, "")});
    let error = create(
        &mut object,
        &mut ResourceStatus::default(),
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
            .contains("SchemaUpgradeRequired")
    );
    assert!(
        !catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.method == "POST")
    );
}

#[tokio::test]
async fn malformed_creation_recovery_preserves_name_reservation() {
    let (_kube, catalyst, context, mut object) = fixture().await;
    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(
                json!({"data": [
                    {"name": "claimed", "description": marker(&object, "")}
                ]}),
            ),
        )
        .mount(&catalyst)
        .await;
    let mut status = ResourceStatus {
        operation: Some(operation("Create")),
        name_operation: Some(
            names::operation(
                "claimed", None,
            ),
        ),
        ..Default::default()
    };
    assert!(
        create(
            &mut object,
            &mut status,
            &context,
            "servers",
            &json!({"name": "claimed"}),
            false
        )
        .await
        .is_err()
    );
    assert!(status.operation.is_some());
    assert!(status.name_operation.is_some());
}

#[tokio::test]
async fn cached_creation_recovery_requires_a_live_uid_marker() {
    for missing in [false, true] {
        let (_kube, catalyst, context, mut object) = fixture().await;
        Mock::given(method("GET")).and(path("/servers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [
                {"id": "cached", "name": "claimed", "description": marker(&object, "")}
            ]}))).mount(&catalyst).await;
        Mock::given(method("GET")).and(path("/servers/cached"))
            .respond_with(if missing {
                ResponseTemplate::new(404).set_body_json(json!({"code": "SERVER_NOT_FOUND"}))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({"data": {"id": "cached", "name": "claimed", "description": "another owner"}}))
            }).mount(&catalyst).await;
        let mut status = ResourceStatus {
            operation: Some(operation("Create")),
            name_operation: Some(
                names::operation(
                    "claimed", None,
                ),
            ),
            ..Default::default()
        };
        let error = create(
            &mut object,
            &mut status,
            &context,
            "servers",
            &json!({"name": "claimed"}),
            false,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("CreateOutcomeUnknown")
        );
        assert!(status.operation.is_some());
        assert!(status.name_operation.is_some());
    }
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
async fn concurrent_creates_across_namespaces_allow_one_name_claim() {
    let (kube, catalyst, context, mut first, first_stored) =
        fixture_for(server_object()).await;
    let catalog = name_catalog(
        &catalyst,
        Vec::new(),
    )
    .await;
    let (mut second, _) = peer_server(
        &kube,
        &first,
        first_stored,
    )
    .await;
    let mut first_status = ResourceStatus::default();
    let mut second_status = ResourceStatus::default();
    let first_body =
        json!({"name": "claimed", "description": marker(&first, "")});
    let second_body =
        json!({"name": "claimed", "description": marker(&second, "")});
    let (left, right) = tokio::join!(
        create(
            &mut first,
            &mut first_status,
            &context,
            "servers",
            &first_body,
            false
        ),
        create(
            &mut second,
            &mut second_status,
            &context,
            "servers",
            &second_body,
            false
        ),
    );
    one_name_claim(
        left, right,
    );
    assert_eq!(
        catalog.0.lock().unwrap().len(),
        1
    );
}

#[tokio::test]
async fn concurrent_renames_allow_one_name_claim() {
    let (kube, catalyst, context, mut first, first_stored) =
        fixture_for(server_object()).await;
    let (mut second, _) = peer_server(
        &kube,
        &first,
        first_stored,
    )
    .await;
    let mut first_status = ResourceStatus::default();
    let mut second_status = ResourceStatus::default();
    let catalog = name_catalog(
        &catalyst,
        vec![
            json!({"id": "first", "name": "before-first"}),
            json!({"id": "second", "name": "before-second"}),
        ],
    )
    .await;
    let updates = json!({"name": "claimed"});
    let (left, right) = tokio::join!(
        super::server::update_server(
            &mut first,
            &mut first_status,
            &context,
            "first",
            &updates
        ),
        super::server::update_server(
            &mut second,
            &mut second_status,
            &context,
            "second",
            &updates
        ),
    );
    one_name_claim(
        left, right,
    );
    assert_eq!(
        catalog
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["name"] == "claimed")
            .count(),
        1
    );
}

#[tokio::test]
async fn concurrent_create_and_rename_allow_one_name_claim() {
    let (kube, catalyst, context, mut object, stored) =
        fixture_for(server_object()).await;
    let (mut other, _) = peer_server(
        &kube, &object, stored,
    )
    .await;
    let mut other_status = ResourceStatus::default();
    let catalog = name_catalog(
        &catalyst,
        vec![json!({"id": "existing", "name": "before"})],
    )
    .await;
    let body = json!({"name": "claimed", "description": marker(&object, "")});
    let updates = json!({"name": "claimed"});
    let mut status = ResourceStatus::default();
    let (left, right) = tokio::join!(
        create(
            &mut object,
            &mut status,
            &context,
            "servers",
            &body,
            false
        ),
        super::server::update_server(
            &mut other,
            &mut other_status,
            &context,
            "existing",
            &updates
        ),
    );
    one_name_claim(
        left, right,
    );
    assert_eq!(
        catalog
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["name"] == "claimed")
            .count(),
        1
    );
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

fn timed_out_power(
    fixture: &mut ServerFixture,
    kind: &str,
    state: &str,
) -> String {
    fixture.backend.actual.lock().unwrap()["status"] = json!(state);
    fixture.object.spec.power_state = if kind == "Stop" {
        PowerState::Stopped
    } else {
        PowerState::Running
    };

    if kind == "RestartStop" {
        fixture.object.spec.restart_token = "restart-request".into();
    }

    let started = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    fixture.status.operation = Some(
        Operation {
            started_at: started.clone(),
            accepted: true,
            retry_token: Some(fixture.object.spec.retry_token.clone()),
            ..operation(kind)
        },
    );
    fixture.status.last_power_attempt_at = Some(started.clone());
    fixture
        .status
        .operation_configuration_digest = fixture
        .status
        .configuration_digest
        .clone();
    fixture.status.operation_restart_token = Some(
        fixture
            .object
            .spec
            .restart_token
            .clone(),
    );
    fixture.status.recovery_attempts = 5;

    started
}

#[tokio::test]
async fn starts_disable_agent_restarts_and_crashes_exhaust_the_budget() {
    for policy in ["on-failure", "always"] {
        let mut fixture = server_fixture("stopped").await;
        fixture.backend.actual.lock().unwrap()["restartPolicy"] = json!(policy);
        fixture.object.spec.power_state = PowerState::Running;

        for attempt in 1..=5 {
            fixture.status.last_power_attempt_at = None;
            fixture.reconcile_persisted().await;
            fixture.reconcile_persisted().await;
            assert!(fixture.status.ready());
            assert_eq!(
                fixture.status.recovery_attempts,
                attempt
            );
            let actual = fixture.backend.actual.lock().unwrap();
            assert_eq!(
                actual["restartPolicy"],
                "never"
            );
            assert_eq!(
                actual["agentAutoRestartEnabled"],
                false
            );
            drop(actual);
            fixture.backend.actual.lock().unwrap()["status"] = json!("crashed");
        }

        fixture.status.last_power_attempt_at = None;
        fixture.reconcile_persisted().await;
        assert_eq!(
            fixture.status.conditions[0].reason,
            "RecoveryRequired"
        );
        let requests = fixture
            .catalyst
            .received_requests()
            .await
            .unwrap();
        let mutations: Vec<_> = requests
            .iter()
            .filter(
                |request| ["PATCH", "POST"].contains(&request.method.as_str()),
            )
            .map(|request| request.url.path())
            .collect();
        assert_eq!(
            mutations[0],
            "/servers/id/restart-policy"
        );
        assert_eq!(
            mutations
                .iter()
                .filter(|path| **path == "/servers/id/start")
                .count(),
            5
        );
    }
}

#[tokio::test]
async fn restart_policy_failure_blocks_start_with_permission_guidance() {
    let mut fixture = server_fixture("stopped").await;
    fixture.backend.actual.lock().unwrap()["restartPolicy"] =
        json!("on-failure");
    fixture.object.spec.power_state = PowerState::Running;
    Mock::given(method("PATCH"))
        .and(path("/servers/id/restart-policy"))
        .respond_with(
            ResponseTemplate::new(403)
                .set_body_json(json!({"code": "PERMISSION_DENIED"})),
        )
        .with_priority(1)
        .mount(&fixture.catalyst)
        .await;
    fixture.reconcile_persisted().await;
    assert_eq!(
        fixture.status.conditions[0].reason,
        "RestartPolicyUnavailable"
    );
    assert!(
        fixture
            .status
            .recovery_action
            .as_deref()
            .unwrap()
            .contains("server.update")
    );
    assert_eq!(
        fixture.status.recovery_attempts,
        0
    );
    assert!(fixture.status.operation.is_none());
    assert!(
        !fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.method == "POST")
    );
}

#[tokio::test]
async fn repaired_running_policy_needs_an_authorized_process_restart() {
    let mut fixture = server_fixture("running").await;
    fixture.backend.actual.lock().unwrap()["restartPolicy"] =
        json!("on-failure");
    fixture.reconcile_persisted().await;
    assert_eq!(
        fixture.status.conditions[0].reason,
        "RestartRequired"
    );
    assert!(fixture.status.running_digest.is_none());
    assert_eq!(
        fixture.backend.actual.lock().unwrap()["restartPolicy"],
        "never"
    );
    fixture.reconcile_persisted().await;
    assert_eq!(
        fixture.status.conditions[0].reason,
        "RestartRequired"
    );
    fixture.object.spec.restart_token = "disable-agent-restarts".into();
    fixture.reconcile_persisted().await;
    fixture.status.last_power_attempt_at = None;
    fixture.reconcile_persisted().await;
    fixture.reconcile_persisted().await;
    assert!(fixture.status.ready());
    assert_eq!(
        fixture.backend.actual.lock().unwrap()["agentAutoRestartEnabled"],
        false
    );
}

#[tokio::test]
async fn expired_power_transitions_keep_recovery_status_after_restart() {
    for (kind, state) in [
        (
            "Start", "starting",
        ),
        (
            "Stop", "stopping",
        ),
        (
            "RestartStop",
            "stopping",
        ),
    ] {
        for attempts in [1, 5] {
            let mut fixture = server_fixture("running").await;
            let started = timed_out_power(
                &mut fixture,
                kind,
                state,
            );
            // Journals from older versions have no recovery-token baseline.
            fixture
                .status
                .operation
                .as_mut()
                .unwrap()
                .retry_token = None;
            fixture.status.recovery_attempts = attempts;

            for _ in 0..3 {
                fixture.reconcile_persisted().await;
                assert_eq!(
                    fixture.status.conditions[0].reason,
                    "RecoveryRequired"
                );
                assert!(!fixture.status.ready());
                assert!(
                    fixture.status.conditions.iter().any(
                        |condition| condition.type_ == "Stalled"
                            && condition.status == "True"
                    )
                );
                let pending = fixture
                    .status
                    .operation
                    .as_ref()
                    .unwrap();
                assert_eq!(
                    pending.kind,
                    kind
                );
                assert_eq!(
                    pending.started_at,
                    started
                );
                let guidance = fixture
                    .status
                    .recovery_action
                    .as_deref()
                    .unwrap();
                assert!(
                    guidance.contains(kind)
                        && guidance.contains(state)
                        && guidance.contains("retryToken")
                );
                assert_eq!(
                    fixture.status.recovery_attempts,
                    attempts
                );
            }

            assert!(
                !fixture
                    .catalyst
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .any(|request| request.method == "POST")
            );
        }
    }
}

#[tokio::test]
async fn timed_out_power_completes_when_the_requested_state_arrives() {
    for (kind, pending, reached) in [
        (
            "Start", "starting", "running",
        ),
        (
            "Stop", "stopping", "stopped",
        ),
    ] {
        let mut fixture = server_fixture("running").await;
        timed_out_power(
            &mut fixture,
            kind,
            pending,
        );
        fixture.reconcile_persisted().await;
        assert_eq!(
            fixture.status.conditions[0].reason,
            "RecoveryRequired"
        );
        fixture.backend.actual.lock().unwrap()["status"] = json!(reached);
        fixture.reconcile_persisted().await;
        assert!(fixture.status.ready());
        assert!(fixture.status.operation.is_none());
        assert!(fixture.status.recovery_action.is_none());
        assert!(
            fixture
                .status
                .last_error
                .as_deref()
                .unwrap()
                .contains("RecoveryRequired")
        );
        assert!(
            !fixture
                .catalyst
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| request.method == "POST")
        );
    }
}

#[tokio::test]
async fn resolved_restart_stop_still_obeys_the_start_budget() {
    let mut fixture = server_fixture("running").await;
    timed_out_power(
        &mut fixture,
        "RestartStop",
        "stopping",
    );
    fixture.reconcile_persisted().await;
    fixture.backend.actual.lock().unwrap()["status"] = json!("stopped");
    fixture.reconcile_persisted().await;
    assert!(fixture.status.operation.is_none());
    assert_eq!(
        fixture.status.conditions[0].reason,
        "RecoveryRequired"
    );
    assert_eq!(
        fixture.status.recovery_attempts,
        5
    );
    fixture.object.spec.retry_token = "resume".into();
    fixture.reconcile_persisted().await;
    fixture.reconcile_persisted().await;
    assert!(fixture.status.ready());
    assert_eq!(
        fixture.status.recovery_attempts,
        1
    );
    assert_eq!(
        fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method == "POST")
            .count(),
        1
    );
}

#[tokio::test]
async fn timeout_retry_requires_a_fresh_token_after_external_resolution() {
    for (kind, pending, cancelled) in [
        (
            "Start", "starting", "stopped",
        ),
        (
            "Stop", "stopping", "running",
        ),
    ] {
        let mut fixture = server_fixture("running").await;
        let started = timed_out_power(
            &mut fixture,
            kind,
            pending,
        );
        fixture.reconcile_persisted().await;
        fixture.object.spec.retry_token = "early".into();
        fixture.reconcile_persisted().await;
        assert_eq!(
            fixture
                .status
                .operation
                .as_ref()
                .unwrap()
                .retry_token
                .as_deref(),
            Some("early")
        );
        fixture.backend.actual.lock().unwrap()["status"] = json!(cancelled);
        fixture.reconcile_persisted().await;
        assert_eq!(
            fixture.status.conditions[0].reason,
            "RecoveryRequired"
        );
        assert_eq!(
            fixture
                .status
                .operation
                .as_ref()
                .unwrap()
                .started_at,
            started
        );
        assert!(
            !fixture
                .catalyst
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| request.method == "POST")
        );
        fixture.object.spec.retry_token = "resolved".into();
        fixture.reconcile_persisted().await;
        assert_ne!(
            fixture
                .status
                .operation
                .as_ref()
                .unwrap()
                .started_at,
            started
        );
        fixture.reconcile_persisted().await;
        assert!(fixture.status.ready());
        assert_eq!(
            fixture
                .catalyst
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|request| request.method == "POST")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn power_intent_requires_a_persisted_recovery_token() {
    let mut fixture = server_fixture("stopped").await;
    fixture.object.spec.power_state = PowerState::Running;
    let status_api = StatusApi(fixture.kube_object.clone());
    Mock::given(method("PATCH"))
        .and(path("/apis/catalyst.catalystctl.io/v1alpha1/namespaces/games/servers/game/status"))
        .respond_with(move |request: &Request| {
            let mut request = request.clone();
            let mut body: Value = serde_json::from_slice(&request.body).unwrap();

            if let Some(operation) = body["status"]["operation"].as_object_mut() {
                operation.remove("retryToken");
            }

            request.body = serde_json::to_vec(&body).unwrap();

            status_api.respond(&request)
        }).with_priority(1).mount(&fixture._kube).await;
    fixture.reconcile_persisted().await;
    assert_eq!(
        fixture.status.conditions[0].reason,
        "SchemaUpgradeRequired"
    );
    assert!(fixture.status.operation.is_none());
    assert!(
        !fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.method == "POST")
    );
}

#[tokio::test]
async fn definitive_power_rejections_clear_intent_and_allow_bounded_retry() {
    for (state, desired, endpoint) in [
        (
            "stopped",
            PowerState::Running,
            "start",
        ),
        (
            "running",
            PowerState::Stopped,
            "stop",
        ),
    ] {
        let mut fixture = server_fixture(state).await;
        fixture.object.spec.power_state = desired;
        Mock::given(method("POST"))
            .and(path(format!("/servers/id/{endpoint}")))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_json(json!({"code": "PERMISSION_DENIED"})),
            )
            .with_priority(1)
            .up_to_n_times(1)
            .mount(&fixture.catalyst)
            .await;
        fixture.reconcile_persisted().await;
        assert_eq!(
            fixture.status.conditions[0].reason,
            "CatalystApiError"
        );
        assert!(fixture.status.operation.is_none());
        fixture.status.last_power_attempt_at = None;
        fixture.reconcile_persisted().await;
        fixture.reconcile_persisted().await;
        assert!(fixture.status.ready());
        assert_eq!(
            fixture
                .catalyst
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|request| request.method == "POST")
                .count(),
            2
        );
    }
}

#[tokio::test]
async fn ambiguous_power_response_keeps_intent_without_repeating_request() {
    let mut fixture = server_fixture("stopped").await;
    fixture.object.spec.power_state = PowerState::Running;
    Mock::given(method("POST"))
        .and(path("/servers/id/start"))
        .respond_with(
            ResponseTemplate::new(504)
                .set_body_json(json!({"code": "GATEWAY_TIMEOUT"})),
        )
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&fixture.catalyst)
        .await;
    fixture.reconcile_persisted().await;
    fixture.reconcile_persisted().await;
    assert_eq!(
        fixture
            .status
            .operation
            .as_ref()
            .unwrap()
            .kind,
        "Start"
    );
    assert!(
        !fixture
            .status
            .operation
            .as_ref()
            .unwrap()
            .accepted
    );
    assert_eq!(
        fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method == "POST")
            .count(),
        1
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

async fn deleting_server_fixture(state: &str) -> ServerFixture {
    let mut fixture = server_fixture(state).await;
    fixture.object.spec.deletion_policy = DeletionPolicy::Delete;
    fixture
        .object
        .metadata
        .deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::now()));
    fixture.kube_object.lock().unwrap()["metadata"]["deletionTimestamp"] =
        serde_json::to_value(
            &fixture
                .object
                .metadata
                .deletion_timestamp,
        )
        .unwrap();

    fixture
}

#[tokio::test]
async fn deletion_stop_timeout_retains_intent_until_fresh_authorization() {
    let mut fixture = deleting_server_fixture("running").await;
    Mock::given(method("POST"))
        .and(path("/servers/id/stop"))
        .respond_with(
            ResponseTemplate::new(504)
                .set_body_json(json!({"code": "GATEWAY_TIMEOUT"})),
        )
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&fixture.catalyst)
        .await;
    fixture.reconcile_persisted().await;
    assert_eq!(
        fixture
            .status
            .operation
            .as_ref()
            .unwrap()
            .kind,
        "DeleteStop"
    );
    let old = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    fixture
        .status
        .operation
        .as_mut()
        .unwrap()
        .started_at = old.clone();

    for state in ["starting", "stopping", "running", "running"] {
        fixture.backend.actual.lock().unwrap()["status"] = json!(state);

        if state != "running" {
            fixture.object.spec.retry_token = "early".into();
        }

        fixture.reconcile_persisted().await;
        assert_eq!(
            fixture.status.conditions[0].reason,
            "RecoveryRequired"
        );
        assert_eq!(
            fixture
                .status
                .operation
                .as_ref()
                .unwrap()
                .started_at,
            old
        );
        assert!(
            fixture
                .object
                .finalizers()
                .iter()
                .any(|value| value == FINALIZER)
        );
    }

    let requests = fixture
        .catalyst
        .received_requests()
        .await
        .unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST")
            .count(),
        1
    );
    fixture.object.spec.retry_token = "resolved".into();
    fixture.status.last_power_attempt_at = None;
    fixture.reconcile_persisted().await;
    assert_ne!(
        fixture
            .status
            .operation
            .as_ref()
            .unwrap()
            .started_at,
        old
    );
    fixture.reconcile_persisted().await;
    assert!(fixture.object.finalizers().is_empty());
    assert!(
        fixture
            .backend
            .actual
            .lock()
            .unwrap()
            .is_null()
    );
    let requests = fixture
        .catalyst
        .received_requests()
        .await
        .unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST")
            .count(),
        2
    );
}

#[tokio::test]
async fn deletion_completes_a_timed_out_stop_without_another_dispatch() {
    let mut fixture = deleting_server_fixture("stopped").await;
    fixture.status.operation = Some(
        Operation {
            started_at: (Utc::now() - chrono::Duration::hours(1)).to_rfc3339(),
            retry_token: Some(String::new()),
            ..operation("DeleteStop")
        },
    );
    fixture.reconcile_persisted().await;
    assert!(fixture.object.finalizers().is_empty());
    assert!(
        fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method != "POST")
    );
}

#[tokio::test]
async fn deletion_preserves_pending_start_and_install_outcomes() {
    for kind in ["Start", "Install"] {
        let mut fixture = deleting_server_fixture("stopped").await;
        let mut pending = operation(kind);
        pending.started_at =
            (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        pending.retry_token = Some(String::new());
        fixture.status.operation = Some(pending.clone());
        fixture.reconcile_persisted().await;
        assert_eq!(
            fixture.status.operation,
            Some(pending)
        );
        assert!(
            fixture
                .object
                .finalizers()
                .iter()
                .any(|value| value == FINALIZER)
        );
        assert!(
            fixture
                .catalyst
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.method == "GET")
        );

        if kind == "Start" {
            fixture.backend.actual.lock().unwrap()["status"] = json!("running");
        } else {
            fixture
                .object
                .spec
                .installation_confirmation_token = "confirmed".into();
        }

        fixture.reconcile_persisted().await;
        fixture.reconcile_persisted().await;
        assert!(fixture.object.finalizers().is_empty());
        assert!(
            fixture
                .backend
                .actual
                .lock()
                .unwrap()
                .is_null()
        );
    }
}

#[tokio::test]
async fn ambiguous_delete_is_preserved_while_definite_rejection_can_retry() {
    for code in [403, 504] {
        let mut fixture = deleting_server_fixture("stopped").await;
        Mock::given(method("DELETE"))
            .and(path("/servers/id"))
            .respond_with(
                ResponseTemplate::new(code)
                    .set_body_json(json!({"code": "TEST_FAILURE"})),
            )
            .with_priority(1)
            .up_to_n_times(1)
            .mount(&fixture.catalyst)
            .await;
        fixture.reconcile_persisted().await;

        if code == 504 {
            fixture.object.spec.deletion_policy = DeletionPolicy::Retain;
        }

        fixture.reconcile_persisted().await;
        let requests = fixture
            .catalyst
            .received_requests()
            .await
            .unwrap();
        let deletes = requests
            .iter()
            .filter(
                |request| {
                    request.method == "DELETE"
                        && request.url.path() == "/servers/id"
                },
            )
            .count();

        if code == 504 {
            assert_eq!(
                deletes,
                1
            );
            assert_eq!(
                fixture.status.conditions[0].reason,
                "CleanupUnconfirmed"
            );
            assert!(
                fixture
                    .object
                    .finalizers()
                    .iter()
                    .any(|value| value == FINALIZER)
            );
            assert_eq!(
                fixture
                    .status
                    .operation
                    .as_ref()
                    .unwrap()
                    .kind,
                "Delete"
            );
        } else {
            assert_eq!(
                deletes,
                2
            );
            assert!(fixture.object.finalizers().is_empty());
        }
    }
}

async fn retain_pending_creation<K: Managed>(mut object: K, runtime: bool) {
    object.meta_mut().deletion_timestamp =
        Some(Time(k8s_openapi::jiff::Timestamp::now()));
    let pending = operation(
        if runtime {
            "CreateRuntimeTemplate"
        } else {
            "Create"
        },
    );
    object.set_status(
        ResourceStatus {
            operation: Some(pending.clone()),
            ..Default::default()
        },
    );
    let (kube, catalyst, context, mut object, stored) =
        fixture_for(object).await;
    let collection = if runtime { "templates" } else { K::COLLECTION };
    let rows = Arc::new(Mutex::new(Vec::<Value>::new()));
    let listed = rows.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{collection}")))
        .respond_with(
            move |_: &Request| {
                ResponseTemplate::new(200).set_body_json(
                    json!({"data": listed.lock().unwrap().clone()}),
                )
            },
        )
        .mount(&catalyst)
        .await;
    let returned = rows.clone();
    let visible = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let live = visible.clone();
    Mock::given(method("GET"))
        .and(path(format!("/{collection}/recovered")))
        .respond_with(
            move |_: &Request| {
                if !live.load(std::sync::atomic::Ordering::SeqCst) {
                    return ResponseTemplate::new(404)
                        .set_body_json(json!({"code": "NOT_FOUND"}));
                }

                ResponseTemplate::new(200)
                    .set_body_json(json!({"data": returned.lock().unwrap()[0]}))
            },
        )
        .mount(&catalyst)
        .await;

    for _ in 0..2 {
        reconcile(
            Arc::new(object),
            Arc::new(restarted(&context)),
        )
        .await
        .unwrap();
        object =
            serde_json::from_value(stored.lock().unwrap().clone()).unwrap();
        assert!(
            object
                .finalizers()
                .iter()
                .any(|value| value == FINALIZER)
        );
        assert_eq!(
            object.status().operation,
            Some(pending.clone())
        );
        assert!(!object.status().ready());
    }

    rows.lock().unwrap().push(json!({"id": "recovered", "name": "game", "description": marker(&object, if runtime { ":runtime" } else { "" })}));
    reconcile(
        Arc::new(object),
        Arc::new(restarted(&context)),
    )
    .await
    .unwrap();
    let object: K =
        serde_json::from_value(stored.lock().unwrap().clone()).unwrap();
    assert!(
        object
            .finalizers()
            .iter()
            .any(|value| value == FINALIZER)
    );
    assert_eq!(
        object.status().operation,
        Some(pending)
    );
    visible.store(
        true,
        std::sync::atomic::Ordering::SeqCst,
    );
    reconcile(
        Arc::new(object),
        Arc::new(restarted(&context)),
    )
    .await
    .unwrap();
    let object: K =
        serde_json::from_value(stored.lock().unwrap().clone()).unwrap();
    assert!(object.finalizers().is_empty());
    assert!(object.status().operation.is_none());
    let status = object.status();
    assert_eq!(
        if runtime {
            status.runtime_template_id.as_deref()
        } else {
            status.external_id.as_deref()
        },
        Some("recovered")
    );
    assert!(
        catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method == "GET")
    );
    drop(kube);
}

#[tokio::test]
async fn retain_preserves_all_unresolved_creation_journals_until_recovery() {
    let server = server_object();
    let metadata = server.metadata.clone();
    retain_pending_creation(
        server.clone(),
        true,
    )
    .await;
    retain_pending_creation(
        server, false,
    )
    .await;
    let template: Template = serde_json::from_value(json!({"metadata": metadata, "spec": {"egg": {"configMapKeyRef": {"name": "egg", "key": "egg.json"}}}})).unwrap();
    retain_pending_creation(
        template, false,
    )
    .await;
    let nest: Nest =
        serde_json::from_value(json!({"metadata": metadata, "spec": {}}))
            .unwrap();
    retain_pending_creation(
        nest, false,
    )
    .await;
}

#[tokio::test]
async fn retain_waits_for_power_and_install_without_dispatching_commands() {
    for (kind, active, reached) in [
        (
            "Start", "starting", "running",
        ),
        (
            "Stop", "stopping", "stopped",
        ),
        (
            "RestartStop",
            "stopping",
            "stopped",
        ),
        (
            "DeleteStop",
            "stopping",
            "stopped",
        ),
        (
            "Install",
            "installing",
            "stopped",
        ),
    ] {
        let mut fixture = deleting_server_fixture("running").await;
        fixture.object.spec.deletion_policy = DeletionPolicy::Retain;
        let pending = operation(kind);
        fixture.status.operation = Some(pending.clone());
        fixture.backend.actual.lock().unwrap()["status"] = json!(active);

        for _ in 0..2 {
            fixture.reconcile_persisted().await;
            assert!(
                fixture
                    .object
                    .finalizers()
                    .iter()
                    .any(|value| value == FINALIZER)
            );
            assert_eq!(
                fixture
                    .status
                    .operation
                    .as_ref()
                    .unwrap()
                    .started_at,
                pending.started_at
            );
            assert!(!fixture.status.ready());
        }

        fixture.backend.actual.lock().unwrap()["status"] = json!(reached);
        fixture.reconcile_persisted().await;

        if kind == "Install" {
            fixture.reconcile_persisted().await;
        }

        assert!(fixture.object.finalizers().is_empty());
        assert!(fixture.status.operation.is_none());
        assert!(
            !fixture
                .backend
                .actual
                .lock()
                .unwrap()
                .is_null()
        );
        assert!(
            fixture
                .catalyst
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.method == "GET")
        );
    }
}

#[tokio::test]
async fn retained_ambiguous_install_requires_confirmation_before_release() {
    let mut fixture = deleting_server_fixture("stopped").await;
    fixture.object.spec.deletion_policy = DeletionPolicy::Retain;
    fixture.status.operation = Some(operation("Install"));
    fixture.reconcile_persisted().await;
    assert_eq!(
        fixture.status.conditions[0].reason,
        "InstallationUnconfirmed"
    );
    assert!(
        fixture
            .object
            .finalizers()
            .iter()
            .any(|value| value == FINALIZER)
    );
    fixture
        .object
        .spec
        .installation_confirmation_token = "inspected".into();
    fixture.reconcile_persisted().await;
    fixture.reconcile_persisted().await;
    assert!(fixture.object.finalizers().is_empty());
    assert!(
        fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method == "GET")
    );
}

#[tokio::test]
async fn nest_deletion_requires_a_live_empty_dependency_list() {
    let mut object: Nest = serde_json::from_value(json!({"metadata": server_object().metadata, "spec": {"deletionPolicy": "Delete"}})).unwrap();
    object.metadata.deletion_timestamp =
        Some(Time(k8s_openapi::jiff::Timestamp::now()));
    object.status = Some(
        ResourceStatus {
            external_id: Some("nest".into()),
            ..Default::default()
        },
    );
    let (kube, catalyst, context, mut object, stored) =
        fixture_for(object).await;
    Mock::given(method("GET"))
        .and(path("/apis/catalyst.catalystctl.io/v1alpha1/namespaces/games/templates"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"apiVersion": "catalyst.catalystctl.io/v1alpha1", "kind": "TemplateList", "metadata": {}, "items": []})))
        .mount(&kube).await;
    Mock::given(method("GET"))
        .and(path("/templates"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": []})),
        )
        .mount(&catalyst)
        .await;
    let actual = Arc::new(
        Mutex::new(
            json!({"id": "nest", "templates": [{"id": "retained-dependent", "nestId": "nest"}]}),
        ),
    );
    let live = actual.clone();
    Mock::given(method("GET"))
        .and(path("/nests/nest"))
        .respond_with(
            move |_: &Request| {
                ResponseTemplate::new(200).set_body_json(
                    json!({"data": live.lock().unwrap().clone()}),
                )
            },
        )
        .mount(&catalyst)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/nests/nest"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"success": true})),
        )
        .mount(&catalyst)
        .await;

    for dependencies in [
        json!([{"id": "retained-dependent"}]),
        Value::Null,
        json!({}),
    ] {
        actual.lock().unwrap()["templates"] = dependencies;
        reconcile(
            Arc::new(object),
            Arc::new(restarted(&context)),
        )
        .await
        .unwrap();
        object =
            serde_json::from_value(stored.lock().unwrap().clone()).unwrap();
        assert!(
            object
                .finalizers()
                .iter()
                .any(|value| value == FINALIZER)
        );
        assert!(!object.status().ready());
        assert!(
            catalyst
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.method != "DELETE")
        );
    }

    actual.lock().unwrap()["templates"] = json!([]);
    reconcile(
        Arc::new(object),
        Arc::new(restarted(&context)),
    )
    .await
    .unwrap();
    let object: Nest =
        serde_json::from_value(stored.lock().unwrap().clone()).unwrap();
    assert!(object.finalizers().is_empty());
}

#[tokio::test]
async fn cleanup_warnings_survive_a_missing_external_record() {
    let mut object = server_object();
    object.spec.deletion_policy = DeletionPolicy::Delete;
    let (_kube, catalyst, context, mut object, _) = fixture_for(object).await;
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
    object.spec.deletion_policy = DeletionPolicy::Retain;
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
async fn running_allocation_binding_drift_waits_for_authorized_repair() {
    let mut fixture = server_fixture("running").await;
    fixture.object.spec.allocation_id = Some("primary".into());
    fixture.backend.allocations.lock().unwrap().push(json!({"id": "primary", "serverId": "id", "ip": "192.0.2.1", "port": 25565}));
    fixture.backend.actual.lock().unwrap()["portBindings"]["25565"] =
        json!(30000);
    fixture.reconcile_persisted().await;
    assert_eq!(
        fixture.status.conditions[0].reason,
        "RestartRequired"
    );
    assert_eq!(
        fixture.status.allocation_id.as_deref(),
        Some("primary")
    );
    assert!(
        fixture
            .status
            .endpoints
            .iter()
            .all(|endpoint| endpoint.port == 30000)
    );
    assert!(
        fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method == "GET")
    );
    fixture.object.spec.restart_token = "repair-binding".into();
    fixture.reconcile_persisted().await;
    fixture.status.last_power_attempt_at = None;
    fixture.reconcile_persisted().await;
    fixture.reconcile_persisted().await;
    assert!(fixture.status.ready());
    assert!(
        fixture
            .status
            .endpoints
            .iter()
            .all(|endpoint| endpoint.port == 25565)
    );
}

#[tokio::test]
async fn allocation_primary_binding_rejects_incompatible_creation_and_edits() {
    let (_kube, catalyst, context, mut object) = fixture().await;
    object.spec.allocation_id = Some("primary".into());
    object.spec.port_bindings.insert(
        "25565".into(),
        30000,
    );
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
            .contains("AllocationPortBindingMismatch")
    );
    assert!(
        catalyst
            .received_requests()
            .await
            .unwrap()
            .is_empty()
    );

    let mut fixture = server_fixture("stopped").await;
    fixture.object.spec.allocation_id = Some("primary".into());
    fixture
        .object
        .spec
        .port_bindings
        .insert(
            "25565".into(),
            30000,
        );
    fixture.backend.allocations.lock().unwrap().push(json!({"id": "primary", "serverId": "id", "ip": "192.0.2.1", "port": 25565}));
    fixture.reconcile_persisted().await;
    assert_eq!(
        fixture.status.conditions[0].reason,
        "AllocationPortBindingMismatch"
    );
    assert!(!fixture.status.ready());
    assert_eq!(
        fixture.status.allocation_id.as_deref(),
        Some("primary")
    );
    assert!(
        fixture
            .catalyst
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method != "PUT")
    );

    fixture
        .object
        .spec
        .port_bindings
        .clear();
    fixture.backend.actual.lock().unwrap()["portBindings"]["25565"] =
        json!(30000);
    fixture.reconcile_persisted().await;
    fixture.reconcile_persisted().await;
    assert!(fixture.status.ready());
    assert!(
        fixture
            .status
            .endpoints
            .iter()
            .all(|endpoint| endpoint.port == 25565)
    );
    assert_eq!(
        fixture.status.allocation_id.as_deref(),
        Some("primary")
    );
}

#[tokio::test]
async fn allocation_switch_checks_destination_ip_and_preserves_environment() {
    for occupied_ip in ["192.0.2.1", "192.0.2.2"] {
        for edit_environment in [false, true] {
            for state in ["stopped", "running"] {
                let mut fixture = server_fixture(state).await;
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
                    json!({"id": "occupied", "serverId": "other", "ip": occupied_ip, "port": 25566}),
                ];

                if edit_environment {
                    fixture
                        .object
                        .spec
                        .environment
                        .get_mut("SETTING")
                        .unwrap()
                        .value = Some("new-value".into());
                }

                if state == "running" {
                    fixture.reconcile_persisted().await;
                    assert_eq!(
                        fixture.status.conditions[0].reason,
                        "RestartRequired"
                    );
                    assert_eq!(
                        fixture.backend.actual.lock().unwrap()["status"],
                        "running"
                    );
                    fixture.object.spec.restart_token = "move-address".into();
                    fixture.reconcile_persisted().await;
                    fixture.status.last_power_attempt_at = None;
                }

                fixture.reconcile_persisted().await;

                if occupied_ip == "192.0.2.1" {
                    fixture.reconcile_persisted().await;
                    assert!(fixture.status.ready());
                    let actual = fixture.backend.actual.lock().unwrap();
                    assert_eq!(
                        actual["primaryIp"],
                        "192.0.2.2"
                    );
                    assert_eq!(
                        actual["primaryPort"],
                        25566
                    );
                    assert_eq!(
                        actual["environment"]["SETTING"],
                        if edit_environment { "new-value" } else { "old" }
                    );
                    assert_eq!(
                        actual["environment"]["TEMPLATE_IMAGE"],
                        "java:21"
                    );
                } else {
                    assert!(!fixture.status.ready());
                    assert!(
                        fixture
                            .status
                            .last_error
                            .as_deref()
                            .unwrap()
                            .contains("PORT_ALREADY_IN_USE")
                    );
                    let actual = fixture.backend.actual.lock().unwrap();
                    assert_eq!(
                        actual["primaryIp"],
                        "192.0.2.1"
                    );
                    assert_eq!(
                        actual["primaryPort"],
                        25565
                    );
                    assert_eq!(
                        fixture.status.allocation_id.as_deref(),
                        Some("old")
                    );
                }
            }
        }
    }
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
async fn selecting_an_owned_secondary_allocation_reports_primary_mismatch() {
    for secondary_port in [25565, 25566] {
        let mut fixture = server_fixture("stopped").await;
        fixture.backend.actual.lock().unwrap()["primaryIp"] =
            json!("192.0.2.1");
        fixture.status.allocation_id = Some("primary".into());
        fixture.object.spec.allocation_id = Some("secondary".into());
        fixture.object.spec.primary_port = secondary_port;
        *fixture
            .backend
            .allocations
            .lock()
            .unwrap() = vec![
            json!({"id": "primary", "serverId": "id", "ip": "192.0.2.1", "port": 25565}),
            json!({"id": "secondary", "serverId": "id", "ip": "192.0.2.2", "port": secondary_port}),
        ];
        fixture.object.status = Some(fixture.status.clone());
        fixture.kube_object.lock().unwrap()["spec"] =
            serde_json::to_value(&fixture.object.spec).unwrap();
        let observed = fixture.kube_object.clone();
        reconcile(
            Arc::new(fixture.object),
            Arc::new(fixture.context),
        )
        .await
        .unwrap();
        let status: ResourceStatus =
            serde_json::from_value(observed.lock().unwrap()["status"].clone())
                .unwrap();
        assert_eq!(
            status.allocation_id.as_deref(),
            Some("primary")
        );
        assert!(!status.ready());
        assert_eq!(
            status.conditions[0].reason,
            "AllocationPromotionUnsupported"
        );
        assert_eq!(
            fixture.backend.actual.lock().unwrap()["primaryIp"],
            "192.0.2.1"
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
async fn allocation_removal_recovers_assignment_after_lost_status_write() {
    let mut fixture = server_fixture("stopped").await;
    let persisted = fixture.status.clone();
    fixture.object.spec.allocation_id = Some("assigned".into());
    *fixture
        .backend
        .allocations
        .lock()
        .unwrap() = vec![
        json!({"id": "assigned", "serverId": null, "ip": "192.0.2.2", "port": 25565}),
    ];
    fixture.reconcile().await.unwrap();
    assert_eq!(
        fixture
            .backend
            .allocations
            .lock()
            .unwrap()[0]["serverId"],
        "id"
    );

    // Catalyst committed the assignment, but Kubernetes retained the old status.
    fixture.object.status = Some(persisted);
    fixture.object.spec.allocation_id = None;
    fixture.kube_object.lock().unwrap()["spec"] =
        serde_json::to_value(&fixture.object.spec).unwrap();
    let observed = fixture.kube_object.clone();
    reconcile(
        Arc::new(fixture.object),
        Arc::new(fixture.context),
    )
    .await
    .unwrap();
    let status: ResourceStatus =
        serde_json::from_value(observed.lock().unwrap()["status"].clone())
            .unwrap();
    assert_eq!(
        status.allocation_id.as_deref(),
        Some("assigned")
    );
    assert!(!status.ready());
    assert_eq!(
        status.conditions[0].reason,
        "AllocationRemovalUnsupported"
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
