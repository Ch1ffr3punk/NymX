use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Deserialize, Default)]
pub struct NymxConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub socks5_proxy: Option<String>,
    pub aliases: Option<HashMap<String, String>>,
}

pub fn find_config_file() -> Option<PathBuf> {
    let mut search_paths = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        search_paths.push(cwd.join("nymx.json"));
    }
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            search_paths.push(exe_dir.join("nymx.json"));
        }
    }
    #[cfg(target_os = "windows")]
    {
        if let Ok(appdata) = std::env::var("APPDATA") {
            search_paths.push(PathBuf::from(appdata).join("nymx").join("nymx.json"));
        }
        if let Ok(userprofile) = std::env::var("USERPROFILE") {
            search_paths.push(PathBuf::from(userprofile).join(".config").join("nymx").join("nymx.json"));
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        if let Ok(home) = std::env::var("HOME") {
            search_paths.push(PathBuf::from(home).join(".config").join("nymx").join("nymx.json"));
        }
    }
    search_paths.into_iter().find(|path| path.exists())
}

pub fn load_config() -> NymxConfig {
    if let Some(config_path) = find_config_file() {
        match std::fs::read_to_string(&config_path) {
            Ok(content) => serde_json::from_str::<NymxConfig>(&content).unwrap_or_else(|e| {
                eprintln!("Warning: Failed to parse {}: {}", config_path.display(), e);
                NymxConfig::default()
            }),
            Err(e) => {
                eprintln!("Warning: Failed to read {}: {}", config_path.display(), e);
                NymxConfig::default()
            }
        }
    } else {
        NymxConfig::default()
    }
}
