use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use crate::config::find_config_file; 

#[derive(Deserialize)]
pub struct AliasesFile {
    pub aliases: HashMap<String, String>,
}

pub fn resolve_alias(alias: &str) -> Result<String> {
    let config_path = find_config_file()
        .context("Aliases file 'nymx.json' not found in standard locations.")?;

    let content = fs::read_to_string(&config_path).with_context(|| {
        format!("Failed to read aliases file at {:?}", config_path)
    })?;

    let aliases: AliasesFile =
        serde_json::from_str(&content).context("Failed to parse aliases file")?;

    aliases
        .aliases
        .get(alias)
        .cloned()
        .with_context(|| format!("Alias '{}' not found in {:?}", alias, config_path))
}
