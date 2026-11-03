use catalyst_operator::{
    api::{Nest, Server, Template, quantity_mb},
    catalyst::Catalyst,
    egg,
};
use kube::CustomResourceExt;
use reqwest::Method;
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[test]
fn quantities_use_catalyst_binary_megabytes_and_round_up() {
    assert_eq!(
        quantity_mb("2Gi").unwrap(),
        2048
    );
    assert_eq!(
        quantity_mb("512Mi").unwrap(),
        512
    );
    assert_eq!(
        quantity_mb("1G").unwrap(),
        954
    );
    assert!(quantity_mb("-1Gi").is_err());
    assert!(quantity_mb("garbage").is_err());
    assert!(quantity_mb("0").is_err());
}

#[test]
fn egg_conversion_preserves_install_and_config_semantics() {
    let egg = json!({
        "meta": {"version": "PTDL_v2"}, "name": "Game",
        "docker_images": {"java": "example/java:21"},
        "startup": "java -Xmx${MEMORY}M -jar {{JAR}}", "config": {
            "stop": "^C", "files": "{\"server.properties\":{\"parser\":\"properties\",\"find\":{\"server-port\":\"{{server.build.default.port}}\"}}}",
            "startup": "{\"done\":\"Done\"}"
        }, "variables": [{"env_variable": "MEMORY", "default_value": "2048", "rules": "required|integer|min:512"}],
        "scripts": {"installation": {"script": "cd /mnt/server\necho ready", "container": "install:1", "entrypoint": "bash"}}
    });
    let (body, warnings) = egg::convert(&egg).unwrap();
    assert_eq!(
        body["startup"],
        "java -Xmx{{MEMORY}}M -jar {{JAR}}"
    );
    assert_eq!(
        body["installScript"],
        "cd /mnt/server\necho ready"
    );
    assert_eq!(
        body["sendSignalTo"],
        "SIGINT"
    );
    assert_eq!(
        body["features"]["startupDonePattern"],
        "Done"
    );
    assert!(
        body["features"]["pterodactylConfigFiles"]["server.properties"]
            .is_object()
    );
    assert_eq!(
        body["variables"][0]["required"],
        true
    );
    assert!(!warnings.is_empty());
}

#[test]
fn malformed_eggs_fail_instead_of_discarding_important_configuration() {
    assert!(egg::convert(&json!({"startup": "run", "images": []})).is_err());
    assert!(
        egg::convert(
            &json!({"startup": "run", "images": ["game:1"], "config": {"files": "broken json"}})
        )
        .is_err()
    );
    assert!(
        egg::convert(
            &json!({"startup": "run", "images": ["game:1"], "variables": [
                {"env_variable": "X"}, {"env_variable": "X"}
            ]})
        )
        .is_err()
    );
}

#[test]
fn generated_resources_are_namespaced_with_status_subresources() {
    for crd in [Server::crd(), Template::crd(), Nest::crd()] {
        assert_eq!(
            crd.spec.scope,
            "Namespaced"
        );
        assert!(
            crd.spec.versions[0]
                .subresources
                .as_ref()
                .unwrap()
                .status
                .is_some()
        );
        assert_eq!(
            crd.spec.group,
            "catalyst.catalystctl.io"
        );
    }
}

#[tokio::test]
async fn allocation_lookup_uses_page_pagination() {
    use wiremock::matchers::query_param;

    let server = MockServer::start().await;
    let rows: Vec<_> = (0..100)
        .map(|index| json!({"id": index.to_string(), "port": 25565, "serverId": null}))
        .collect();
    Mock::given(method("GET"))
        .and(path("/nodes/node/allocations"))
        .and(
            query_param(
                "page", "1",
            ),
        )
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": rows})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/nodes/node/allocations"))
        .and(
            query_param(
                "page", "2",
            ),
        )
        .respond_with(
            ResponseTemplate::new(200).set_body_json(
                json!({"data": [
                    {"id": "selected", "port": 25566, "serverId": null}
                ]}),
            ),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = Catalyst::new(
        &server.uri(),
        "test".into(),
    )
    .unwrap();
    let allocation = client
        .allocation(
            "node", "selected",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        allocation["port"],
        25566
    );
}

#[tokio::test]
async fn assigned_allocation_lookup_filters_owner_and_primary_binding() {
    use wiremock::matchers::query_param;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/nodes/node/allocations"))
        .and(query_param("serverId", "owner"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [
            {"id": "foreign", "serverId": "another", "ip": "192.0.2.1", "port": 25565},
            {"id": "secondary", "serverId": "owner", "ip": "192.0.2.1", "port": 25566},
            {"id": "another-ip", "serverId": "owner", "ip": "192.0.2.2", "port": 25565},
            {"id": "primary", "serverId": "owner", "ip": "192.0.2.1", "port": 25565}
        ]})))
        .expect(1)
        .mount(&server)
        .await;
    let client = Catalyst::new(
        &server.uri(),
        "test".into(),
    )
    .unwrap();
    let allocation = client
        .assigned_allocation(
            "node",
            "owner",
            "192.0.2.1",
            25565,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        allocation["id"],
        "primary"
    );
}

#[tokio::test]
async fn api_errors_never_echo_secret_values() {
    let server = MockServer::start().await;
    Mock::given(method("PUT")).and(path("/api/servers/id"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({"code": "VALIDATION_ERROR", "error": "password=super-secret", "params": {"value": "super-secret"}})))
        .mount(&server).await;
    let client = Catalyst::new(
        &format!(
            "{}/api",
            server.uri()
        ),
        "credential".into(),
    )
    .unwrap();
    let error = client
        .request(
            Method::PUT,
            "servers/id",
            Some(&json!({})),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("VALIDATION_ERROR")
    );
    assert!(
        !error
            .to_string()
            .contains("super-secret")
    );
    assert!(!error.to_string().contains("credential"));
}

#[tokio::test]
async fn paginated_server_listing_does_not_miss_later_owned_servers() {
    use wiremock::matchers::query_param;

    let server = MockServer::start().await;
    let rows: Vec<_> = (0..100)
        .map(|i| json!({"id": i.to_string()}))
        .collect();
    Mock::given(method("GET"))
        .and(path("/servers"))
        .and(
            query_param(
                "offset", "0",
            ),
        )
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data": rows})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/servers"))
        .and(
            query_param(
                "offset", "100",
            ),
        )
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"data": [{"id": "last"}]})),
        )
        .mount(&server)
        .await;
    let client = Catalyst::new(
        &server.uri(),
        "test".into(),
    )
    .unwrap();
    let rows = client.list("servers").await.unwrap();
    assert_eq!(
        rows.len(),
        101
    );
    assert_eq!(
        rows[100]["id"],
        "last"
    );
}

#[test]
fn image_preference_preserves_egg_order_and_digests_ignore_object_order() {
    let egg = serde_json::from_str(r#"{"startup":"run","docker_images":{"Zulu default":"java:21","Alpha alternative":"java:25"}}"#).unwrap();
    let (body, _) = egg::convert(&egg).unwrap();
    assert_eq!(
        body["image"],
        "java:21"
    );
    let first: serde_json::Value =
        serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
    let second: serde_json::Value =
        serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
    assert_eq!(
        catalyst_operator::api::digest(&first),
        catalyst_operator::api::digest(&second)
    );
}

#[tokio::test]
async fn malformed_list_responses_do_not_look_like_an_empty_catalog() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"success": true, "unexpected": []})),
        )
        .mount(&server)
        .await;
    let client = Catalyst::new(
        &server.uri(),
        "test".into(),
    )
    .unwrap();
    let error = client
        .list("servers")
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        "INVALID_RESPONSE"
    );
}

#[test]
fn gateway_timeouts_and_redirects_are_ambiguous_creation_outcomes() {
    use catalyst_operator::catalyst::ApiError;

    for (status, ambiguous) in [
        (
            0, true,
        ),
        (
            408, true,
        ),
        (
            503, true,
        ),
        (
            302, true,
        ),
        (
            400, false,
        ),
        (
            409, false,
        ),
    ] {
        let error = ApiError {
            method: "POST".into(),
            path: "servers".into(),
            status,
            code: "TEST".into(),
            guidance: String::new(),
        };
        assert_eq!(
            error.ambiguous(),
            ambiguous
        );
    }
}
