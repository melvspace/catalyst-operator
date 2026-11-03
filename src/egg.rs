use std::collections::BTreeSet;

use anyhow::{Context, bail, ensure};
use serde_json::{Value, json};

/// Convert an egg into native Catalyst fields without creating temporary API objects.
/// Preserve runtime features rather than discarding configuration-file substitutions.
pub fn convert(
    egg: &Value,
) -> anyhow::Result<(
    Value,
    Vec<String>,
)> {
    ensure!(
        egg.is_object(),
        "Egg must be a JSON object"
    );
    let startup = egg["startup"]
        .as_str()
        .context("Egg startup must be a string")?;
    ensure!(
        !startup.trim().is_empty(),
        "Egg startup is empty"
    );
    let mut images = Vec::new();

    if let Some(values) = egg["images"].as_array() {
        for (i, image) in values.iter().enumerate() {
            let image = image
                .as_str()
                .context("Egg image must be a string")?;
            images.push(json!({"name": format!("image-{i}"), "image": image}));
        }
    } else if let Some(values) = egg["docker_images"].as_object() {
        for (label, image) in values {
            let image = image
                .as_str()
                .context("Egg image must be a string")?;
            images.push(json!({"name": label, "label": label, "image": image}));
        }
    }

    ensure!(
        !images.is_empty(),
        "Egg must contain docker_images or images"
    );
    ensure!(
        images.iter().all(
            |v| !v["image"]
                .as_str()
                .unwrap_or("")
                .is_empty()
        ),
        "Egg contains an empty image"
    );
    let mut variables = Vec::new();
    let mut names = BTreeSet::new();
    let raw_variables = egg["variables"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        raw_variables.len() <= 256,
        "Egg exceeds 256 variables"
    );

    for v in raw_variables {
        let name = v["env_variable"]
            .as_str()
            .context("Egg variable missing env_variable")?;
        ensure!(
            !name.is_empty() && names.insert(name.to_owned()),
            "Egg has an empty or duplicate variable name"
        );
        let default = v
            .get("default_value")
            .map(
                |v| {
                    v.as_str()
                        .context("Variable default must be a string")
                },
            )
            .transpose()?
            .unwrap_or("");
        let rules: Vec<&str> = v["rules"]
            .as_str()
            .unwrap_or("")
            .split('|')
            .filter(|v| !v.is_empty())
            .collect();
        let input = if rules
            .iter()
            .any(|r| r.starts_with("in:"))
        {
            "select"
        } else if rules.contains(&"boolean") {
            "checkbox"
        } else if rules.contains(&"integer") || rules.contains(&"numeric") {
            "number"
        } else {
            "text"
        };
        let mut variable = json!({
            "name": name, "description": v["description"].as_str().unwrap_or(name),
            "default": default, "required": rules.contains(&"required"), "rules": rules,
            "userViewable": v["user_viewable"].as_bool().unwrap_or(true),
            "userEditable": v["user_editable"].as_bool().unwrap_or(true), "input": input,
        });

        if input == "checkbox" {
            variable["trueValue"] = json!("1");
            variable["falseValue"] = json!("0");
        }

        if let Some(rule) = rules
            .iter()
            .find(|r| r.starts_with("in:"))
        {
            variable["options"] =
                json!(rule[3..].split(',').collect::<Vec<_>>());
        }

        variables.push(variable);
    }

    let config = &egg["config"];
    let parsed = |key: &str| -> anyhow::Result<Value> {
        match &config[key] {
            Value::String(s) if key != "stop" => serde_json::from_str(s)
                .with_context(|| format!("Invalid egg config.{key} JSON")),
            v => Ok(v.clone()),
        }
    };
    let stop = config["stop"]
        .as_str()
        .unwrap_or("stop");
    let stop =
        serde_json::from_str::<String>(stop).unwrap_or_else(|_| stop.into());
    let (stop_command, signal) = match stop.as_str() {
        "^C" | "SIGINT" => (
            "", "SIGINT",
        ),
        "^D" | "SIGTERM" => (
            "", "SIGTERM",
        ),
        "SIGKILL" | "^9" => (
            "", "SIGKILL",
        ),
        v if v.starts_with('^') => {
            bail!("Unsupported egg stop signal; specify a native template")
        }
        v => (
            v.trim_start_matches('/'),
            "SIGTERM",
        ),
    };
    let installation = &egg["scripts"]["installation"];
    let script = installation["script"]
        .as_str()
        .unwrap_or("")
        .replace(
            "\\/", "/",
        );
    ensure!(
        script.len() <= 512 * 1024,
        "Egg installation script exceeds snapshot size limit"
    );
    let mut features = json!({"restartOnExit": false});

    for (source, target) in [
        (
            "startup",
            "startupDetection",
        ),
        (
            "logs",
            "logDetection",
        ),
        (
            "files",
            "pterodactylConfigFiles",
        ),
    ] {
        let value = parsed(source)?;

        if !value.is_null() {
            features[target] = value;
        }
    }

    if let Some(done) = features["startupDetection"]
        .get("done")
        .cloned()
    {
        features["startupDonePattern"] = done;
    }

    if let Some(files) = features["pterodactylConfigFiles"].as_object() {
        let keys: Vec<String> = files.keys().cloned().collect();
        features["configFiles"] = json!(keys);
        features["configFile"] = json!(keys.first());
    }

    for (source, target) in [
        (
            "features",
            "pterodactylFeatures",
        ),
        (
            "file_denylist",
            "fileDenylist",
        ),
        (
            "tags", "tags",
        ),
        (
            "export_files",
            "exportFiles",
        ),
    ] {
        if let Some(v) = egg.get(source) {
            features[target] = v.clone();
        }
    }

    let mut denylist = Vec::new();

    for value in [&egg["file_denylist"], &config["file_denylist"]] {
        if let Some(entries) = value.as_array() {
            denylist.extend(entries.iter().cloned());
        }
    }

    if !denylist.is_empty() {
        features["fileDenylist"] = json!(denylist);
    }

    if let Some(url) = egg
        .get("update_url")
        .or_else(|| egg["meta"].get("update_url"))
    {
        features["updateUrl"] = url.clone();
    }

    features["pterodactylSpecVersion"] = egg["meta"]["version"].clone();
    let mut warnings = Vec::new();

    if let Some(version) = egg["meta"]["version"].as_str()
        && !["PTDL_v1", "PTDL_v2", "PTDL_v3"].contains(&version)
    {
        warnings.push(
            "Unknown egg format version; verify compatibility before creating servers".into(),
        );
    }

    if egg.get("_category").is_some() {
        warnings.push("Egg category is ignored; use spec.nestRef".into());
    }

    let startup =
        regex::Regex::new(r"\$\{([A-Z_][A-Z0-9_]*)\}|\$([A-Z_][A-Z0-9_]*)")?
            .replace_all(
                startup,
                |captures: &regex::Captures| {
                    let name = captures
                        .get(1)
                        .or_else(|| captures.get(2))
                        .unwrap()
                        .as_str();

                    if names.contains(name)
                        || [
                            "SERVER_MEMORY",
                            "SERVER_PORT",
                            "SERVER_IP",
                            "SERVER_DIR",
                        ]
                        .contains(&name)
                    {
                        format!("{{{{{name}}}}}")
                    } else {
                        captures[0].to_owned()
                    }
                },
            )
            .into_owned();
    let port = variables
        .iter()
        .find(
            |v| {
                ["SERVER_PORT", "PORT"]
                    .contains(&v["name"].as_str().unwrap_or(""))
            },
        )
        .and_then(
            |v| {
                v["default"]
                    .as_str()?
                    .parse::<u16>()
                    .ok()
            },
        )
        .unwrap_or(25565);
    warnings.push("Egg resource/port defaults are advisory; each Server must declare its own quotas and primaryPort".into());

    Ok(
        (
            json!({
                "image": images[0]["image"], "defaultImage": images[0]["image"], "images": images,
                "description": egg["description"].as_str().unwrap_or(""),
                "author": egg["author"].as_str().unwrap_or("Egg import"), "version": "1.0.0",
                "startup": startup, "stopCommand": stop_command, "sendSignalTo": signal,
                "installScript": script, "installImage": installation["container"],
                "installEntrypoint": installation["entrypoint"].as_str().unwrap_or("bash"),
                "variables": variables, "supportedPorts": [port],
                "allocatedMemoryMb": 1024, "allocatedCpuCores": 2, "features": features,
            }),
            warnings,
        ),
    )
}
