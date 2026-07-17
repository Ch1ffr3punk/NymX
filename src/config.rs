use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub aliases: HashMap<String, String>,
    #[serde(default)]
    pub ssh: Option<SshConfig>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SshConfig {
    pub host: String,
    #[serde(default = "default_ssh_port")]
    pub port: u16,
    pub username: String,
    #[serde(default = "default_socks5")]
    pub socks5_proxy: String,
    #[serde(default = "default_remote_path")]
    pub remote_path: String,
}

fn default_ssh_port() -> u16 { 22 }
fn default_socks5() -> String { "127.0.0.1:9050".to_string() }
fn default_remote_path() -> String { "received".to_string() }

pub fn get_base_dir() -> PathBuf {
    let exe_dir = env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| env::current_dir().unwrap_or_default());

    if cfg!(target_os = "windows") {
        return exe_dir;
    }

    let config_in_exe = exe_dir.join("nymx.json");
    if config_in_exe.exists() {
        return exe_dir;
    }

    if let Some(home) = env::var_os("HOME") {
        let home_path = PathBuf::from(home);
        let config_in_home = home_path.join("nymx.json");
        if config_in_home.exists() {
            return home_path;
        }
    }

    if let Some(home) = env::var_os("HOME") {
        return PathBuf::from(home);
    }

    exe_dir
}

impl Config {
    pub fn load() -> Self {
        let base_dir = get_base_dir();
        let config_path = base_dir.join("nymx.json");
        if config_path.exists() {
            let content = std::fs::read_to_string(config_path).unwrap_or_default();
            serde_json::from_str(&content).unwrap_or_default()
        } else {
            Config::default()
        }
    }

    pub fn resolve(&self, input: &str) -> Option<String> {
        self.aliases.get(input).cloned()
    }
}
