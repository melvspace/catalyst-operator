use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const GROUP: &str = "catalyst.catalystctl.io";
pub const FINALIZER: &str = "catalyst.catalystctl.io/cleanup";

#[derive(
    Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
pub enum DeletionPolicy {
    #[default]
    Retain,
    Delete,
}

#[derive(
    Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
pub enum PowerState {
    #[default]
    Running,
    Stopped,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalRef {
    #[schemars(length(min = 1))]
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeyRef {
    #[schemars(length(min = 1))]
    pub name: String,
    #[schemars(length(min = 1))]
    pub key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnvValue {
    pub value: Option<String>,
    pub secret_key_ref: Option<KeyRef>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Resources {
    #[schemars(range(min = 1, max = 128))]
    pub cpu: u32,
    pub memory: String,
    pub disk: String,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "catalyst.catalystctl.io",
    version = "v1alpha1",
    kind = "Server",
    namespaced,
    status = "ResourceStatus",
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#,
    printcolumn = r#"{"name":"Reason","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].reason"}"#,
    printcolumn = r#"{"name":"State","type":"string","jsonPath":".status.externalState"}"#,
    printcolumn = r#"{"name":"ID","type":"string","jsonPath":".status.externalId"}"#
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServerSpec {
    pub name: Option<String>,
    #[serde(default)]
    #[schemars(length(max = 350))]
    pub description: String,
    pub template_ref: LocalRef,
    pub node_id: String,
    pub location_id: String,
    pub owner_id: Option<String>,
    pub resources: Resources,
    #[schemars(range(min = 1, max = 65535))]
    pub primary_port: u16,
    pub primary_ip: Option<String>,
    pub allocation_id: Option<String>,
    #[serde(default)]
    pub port_bindings: BTreeMap<String, u16>,
    #[serde(default)]
    pub environment: BTreeMap<String, EnvValue>,
    pub startup_command: Option<String>,
    #[serde(default)]
    pub power_state: PowerState,
    #[serde(default)]
    pub deletion_policy: DeletionPolicy,
    #[serde(default)]
    pub restart_token: String,
    #[serde(default)]
    pub retry_token: String,
    /// Change after confirming that a missing external server should be replaced.
    #[serde(default)]
    pub replacement_token: String,
    /// After inspecting an ambiguous initial installation, confirm it finished.
    #[serde(default)]
    pub installation_confirmation_token: String,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "catalyst.catalystctl.io",
    version = "v1alpha1",
    kind = "Nest",
    namespaced,
    status = "ResourceStatus",
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#,
    printcolumn = r#"{"name":"Reason","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].reason"}"#
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NestSpec {
    pub name: Option<String>,
    #[serde(default)]
    pub description: String,
    pub icon: Option<String>,
    pub author: Option<String>,
    #[serde(default)]
    pub deletion_policy: DeletionPolicy,
    #[serde(default)]
    pub retry_token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EggSource {
    pub url: Option<String>,
    pub config_map_key_ref: Option<KeyRef>,
    #[serde(default)]
    pub revision: String,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "catalyst.catalystctl.io",
    version = "v1alpha1",
    kind = "Template",
    namespaced,
    status = "ResourceStatus",
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#,
    printcolumn = r#"{"name":"Reason","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].reason"}"#
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TemplateSpec {
    pub name: Option<String>,
    pub nest_ref: Option<LocalRef>,
    /// Native Catalyst template fields. Mutually exclusive with egg.
    pub native: Option<NativeTemplate>,
    pub egg: Option<EggSource>,
    #[serde(default)]
    pub deletion_policy: DeletionPolicy,
    #[serde(default)]
    pub retry_token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeTemplate {
    pub image: String,
    pub startup: String,
    pub stop_command: String,
    pub send_signal_to: String,
    pub author: String,
    pub version: String,
    pub description: Option<String>,
    pub install_image: Option<String>,
    pub install_entrypoint: Option<String>,
    pub install_script: Option<String>,
    pub default_image: Option<String>,
    #[serde(default)]
    #[schemars(schema_with = "object_array")]
    pub images: Vec<Value>,
    #[serde(default)]
    #[schemars(schema_with = "object_array")]
    pub variables: Vec<Value>,
    pub supported_ports: Vec<u16>,
    pub allocated_memory_mb: u32,
    pub allocated_cpu_cores: u32,
    #[serde(default = "empty_object")]
    #[schemars(schema_with = "arbitrary_object")]
    pub features: Value,
}

fn arbitrary_object(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    serde_json::json!({"type": "object", "x-kubernetes-preserve-unknown-fields": true})
        .try_into()
        .unwrap()
}

fn object_array(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    serde_json::json!({"type": "array", "items": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}})
        .try_into()
        .unwrap()
}

fn empty_object() -> Value {
    serde_json::json!({})
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub kind: String,
    pub started_at: String,
    #[serde(default)]
    pub accepted: bool,
    #[serde(default)]
    pub observed_active: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub protocol: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NameOperation {
    pub name: String,
    pub external_id: Option<String>,
    pub started_at: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceStatus {
    #[serde(default)]
    pub conditions: Vec<Condition>,
    pub observed_generation: Option<i64>,
    pub last_observed_at: Option<String>,
    pub external_id: Option<String>,
    pub external_state: Option<String>,
    pub applied_name: Option<String>,
    pub last_error: Option<String>,
    pub last_error_at: Option<String>,
    pub allocation_id: Option<String>,
    pub operation: Option<Operation>,
    pub name_operation: Option<NameOperation>,
    pub runtime_template_id: Option<String>,
    pub snapshot_ref: Option<String>,
    pub source_digest: Option<String>,
    pub source_revision: Option<String>,
    pub immutable_digest: Option<String>,
    pub configuration_digest: Option<String>,
    pub operation_configuration_digest: Option<String>,
    pub operation_restart_token: Option<String>,
    pub last_power_attempt_at: Option<String>,
    pub running_since: Option<String>,
    #[serde(default)]
    pub managed_environment_keys: Vec<String>,
    pub running_digest: Option<String>,
    #[serde(default)]
    pub installed: bool,
    #[serde(default)]
    pub recovery_attempts: u32,
    #[serde(default)]
    pub api_failures: u32,
    #[serde(default)]
    pub retry_token: String,
    #[serde(default)]
    pub replacement_token: String,
    #[serde(default)]
    pub restart_token: String,
    #[serde(default)]
    pub installation_confirmation_token: String,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub endpoints: Vec<Endpoint>,
    pub recovery_action: Option<String>,
}

impl ResourceStatus {
    pub fn ready(&self) -> bool {
        self.conditions
            .iter()
            .any(|c| c.type_ == "Ready" && c.status == "True")
    }
}

pub fn digest(value: &impl Serialize) -> String {
    use sha2::{Digest, Sha256};

    let mut value = serde_json::to_value(value).expect("serializable value");
    value.sort_all_objects();

    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).unwrap())
    )
}

pub fn quantity_mb(value: &str) -> Result<u64, String> {
    let re =
        regex::Regex::new(r"^([0-9]+(?:\.[0-9]+)?)(Ki|Mi|Gi|Ti|k|M|G|T)?$")
            .unwrap();
    let captures = re
        .captures(value)
        .ok_or("Use a positive quantity such as 512Mi or 2Gi")?;
    let number: f64 = captures[1]
        .parse()
        .map_err(|_| "Invalid quantity")?;
    let multiplier: f64 = match captures
        .get(2)
        .map(|m| m.as_str())
        .unwrap_or("")
    {
        "Ki" => 1024.0,
        "Mi" => 1024.0_f64.powi(2),
        "Gi" => 1024.0_f64.powi(3),
        "Ti" => 1024.0_f64.powi(4),
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        _ => 1.0,
    };
    let mb = number * multiplier / 1024.0_f64.powi(2);

    if !mb.is_finite() || mb <= 0.0 || mb > 1_048_576.0 {
        return Err("Quantity is outside Catalyst's supported range".into());
    }

    Ok(mb.ceil() as u64)
}

/// Add cross-field/transition validation at admission, not just reconciliation.
pub fn crds()
-> Vec<k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition>
{
    use kube::CustomResourceExt;

    [Nest::crd(), Template::crd(), Server::crd()]
        .into_iter()
        .map(|crd| {
            let mut value = serde_json::to_value(crd).unwrap();
            let kind = value["spec"]["names"]["kind"].as_str().unwrap().to_owned();
            let conditions = &mut value["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["status"]["properties"]["conditions"];
            conditions["x-kubernetes-list-type"] = serde_json::json!("map");
            conditions["x-kubernetes-list-map-keys"] = serde_json::json!(["type"]);
            let spec = &mut value["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];

            if kind == "Template" {
                spec["x-kubernetes-validations"] = serde_json::json!([{
                    "rule": "has(self.native) != has(self.egg)",
                    "message": "Specify exactly one native or egg source"
                }]);
                spec["properties"]["egg"]["x-kubernetes-validations"] = serde_json::json!([{
                    "rule": "has(self.url) != has(self.configMapKeyRef)",
                    "message": "Specify exactly one egg URL or ConfigMap key reference"
                }]);
            }

            if kind == "Server" {
                spec["x-kubernetes-validations"] = serde_json::json!([
                    {"rule": "self.nodeId == oldSelf.nodeId", "message": "nodeId is immutable"},
                    {"rule": "self.locationId == oldSelf.locationId", "message": "locationId is immutable"},
                    {"rule": "self.templateRef == oldSelf.templateRef", "message": "templateRef is immutable"},
                    {"rule": "has(self.ownerId) == has(oldSelf.ownerId) && (!has(self.ownerId) || self.ownerId == oldSelf.ownerId)", "message": "ownerId is immutable"},
                    {"rule": "!has(self.primaryIp)", "message": "Bridge networking does not support primaryIp; select an allocationId to bind an address"}
                ]);
                spec["properties"]["environment"]["additionalProperties"]["x-kubernetes-validations"] = serde_json::json!([{
                    "rule": "has(self.value) != has(self.secretKeyRef)",
                    "message": "Specify exactly one value or secretKeyRef"
                }]);
            }

            serde_json::from_value(value).expect("valid CRD schema")
        })
        .collect()
}
