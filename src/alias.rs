use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

pub const ALIASES_FILE: &str = "nymx.json";

#[derive(Deserialize)]
pub struct AliasesFile {
    pub aliases: HashMap<String, String>,
}

pub fn resolve_alias(alias: &str) -> Result<String> {
    let content = fs::read_to_string(ALIASES_FILE).with_context(|| {
        format!(
            "Aliases file '{}' not found. Create it with:\n{{\n  \"aliases\": {{\n    \"alice\": \"AliceNymAddressHere\",\n    \"bob\": \"BobNymAddressHere\"\n  }}\n}}",
            ALIASES_FILE
        )
    })?;

    let aliases: AliasesFile =
        serde_json::from_str(&content).context("Failed to parse aliases file")?;

    aliases
        .aliases
        .get(alias)
        .cloned()
        .with_context(|| format!("Alias '{}' not found in {}", alias, ALIASES_FILE))
}
