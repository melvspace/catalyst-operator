use std::time::Duration;

use reqwest::{Method, StatusCode, Url};
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Catalyst {method} {path}: {code} (HTTP {status}); {guidance}")]
pub struct ApiError {
    pub method: String,
    pub path: String,
    pub status: u16,
    pub code: String,
    pub guidance: String,
}

impl ApiError {
    pub fn ambiguous(&self) -> bool {
        self.status == 0
            || self.status == 408
            || (300..400).contains(&self.status)
            || self.status >= 500
    }
}

#[derive(Clone)]
pub struct Catalyst {
    http: reqwest::Client,
    base: Url,
    token: String,
}

impl Catalyst {
    pub fn new(base: &str, token: String) -> anyhow::Result<Self> {
        let mut base = Url::parse(base)?;

        anyhow::ensure!(
            matches!(
                base.scheme(),
                "http" | "https"
            ),
            "Catalyst URL must use HTTP(S)"
        );
        anyhow::ensure!(
            base.username().is_empty() && base.password().is_none(),
            "Use a Secret for API credentials"
        );
        base.set_path(
            &format!(
                "{}/",
                base.path().trim_end_matches('/')
            ),
        );
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        Ok(Self { http, base, token })
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, ApiError> {
        let url = self
            .base
            .join(path.trim_start_matches('/'))
            .expect("validated API path");
        let mut request = self
            .http
            .request(
                method.clone(),
                url,
            )
            .bearer_auth(&self.token);

        if let Some(body) = body {
            request = request.json(body);
        }

        let response = request.send().await.map_err(|_| ApiError {
            method: method.to_string(),
            path: path.into(),
            status: 0,
            code: "TRANSPORT_ERROR".into(),
            guidance: "Check Catalyst connectivity and request outcome before retrying a non-idempotent operation".into(),
        })?;
        let status = response.status();
        let value: Value = response.json().await.map_err(
            |_| ApiError {
                method: method.to_string(),
                path: path.into(),
                status: if status.is_success() {
                    0
                } else {
                    status.as_u16()
                },
                code: "INVALID_RESPONSE".into(),
                guidance: "Check Catalyst/proxy logs for a malformed response"
                    .into(),
            },
        )?;

        if !status.is_success() || value.get("success") == Some(&json!(false)) {
            let code = value["code"]
                .as_str()
                .unwrap_or("API_ERROR");
            // Never reflect API bodies: validation errors can echo environment secrets.
            let code = if code.len() <= 100
                && code.chars().all(
                    |c| {
                        c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'
                    },
                ) {
                code
            } else {
                "API_ERROR"
            };

            return Err(ApiError {
                method: method.to_string(),
                path: path.into(),
                status: status.as_u16(),
                code: code.into(),
                guidance: match code {
                    "NODE_OFFLINE" => "Catalyst node is offline; restore node/agent connectivity",
                    "INSUFFICIENT_RESOURCES" => "Node capacity is insufficient; lower quotas or free capacity",
                    "SERVER_TEMPLATE_MINIMUM_DISK" => "Disk quota is below the template minimum; increase spec.resources.disk",
                    "PORT_ALREADY_IN_USE" => "The requested host port is already allocated on this Catalyst node",
                    "AGENT_COMMAND_FAILED" => "Catalyst could not deliver the operation to its node agent",
                    "TEMPLATE_IN_USE" => "Catalyst servers still reference this template; remove those dependencies first",
                    _ => match status {
                    StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                        "Check API credential permissions"
                    }
                    StatusCode::CONFLICT => {
                        "Resolve the name, state, allocation, or dependency conflict in Catalyst"
                    }
                    StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                        "Check manifest fields and Catalyst validation logs"
                    }
                    _ => "Inspect Catalyst logs and node connectivity",
                    },
                }
                .into(),
            });
        }

        Ok(value)
    }

    pub async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<Value>, ApiError> {
        match self
            .request(
                Method::GET,
                &format!("{collection}/{id}"),
                None,
            )
            .await
        {
            Ok(v) if v["data"].is_object() => Ok(Some(v["data"].clone())),
            Ok(_) => Err(
                invalid_response(
                    collection,
                    "Object response is missing its data object",
                ),
            ),
            Err(e) if e.status == 404 => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn list(&self, collection: &str) -> Result<Vec<Value>, ApiError> {
        let mut all = Vec::new();
        let mut offset = 0;

        loop {
            let page = self
                .request(
                    Method::GET,
                    &format!("{collection}?limit=100&offset={offset}"),
                    None,
                )
                .await?;
            let rows = page["data"].as_array().cloned().ok_or_else(|| {
                invalid_response(
                    collection,
                    "List response is missing its data array; creation is blocked",
                )
            })?;
            let count = rows.len();
            all.extend(rows);

            if collection == "nests" || count < 100 {
                break;
            }

            offset += count;
        }

        Ok(all)
    }

    pub async fn allocation(
        &self,
        node_id: &str,
        allocation_id: &str,
    ) -> Result<Option<Value>, ApiError> {
        let collection = format!("nodes/{node_id}/allocations");
        let mut page = 1;

        loop {
            let response = self
                .request(
                    Method::GET,
                    &format!("{collection}?limit=100&page={page}"),
                    None,
                )
                .await?;
            let rows = response["data"].as_array().ok_or_else(
                || {
                    invalid_response(
                        &collection,
                        "Allocation response is missing its data array",
                    )
                },
            )?;

            if let Some(allocation) = rows
                .iter()
                .find(|value| value["id"] == allocation_id)
            {
                return Ok(Some(allocation.clone()));
            }

            if rows.len() < 100 {
                return Ok(None);
            }

            page += 1;
        }
    }
}

fn invalid_response(path: &str, guidance: &str) -> ApiError {
    ApiError {
        method: "GET".into(),
        path: path.into(),
        status: 0,
        code: "INVALID_RESPONSE".into(),
        guidance: guidance.into(),
    }
}
