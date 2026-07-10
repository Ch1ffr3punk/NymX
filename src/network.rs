use anyhow::{Context, Result};
use nym_sdk::mixnet::{MixnetClient, MixnetClientBuilder, MixnetMessageSender, Recipient, StoragePaths};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

pub struct NymNode {
    pub client: MixnetClient,
}

impl NymNode {
    pub async fn connect() -> Result<Self> {
        eprintln!("Initializing NymX...");
        eprintln!("Loading or generating keys...");

        let config_dir = PathBuf::from("nymx");
        if !config_dir.exists() {
            fs::create_dir_all(&config_dir).context("Failed to create storage directory")?;
        }

        let storage_paths = StoragePaths::new_from_dir(&config_dir)
            .context("Failed to create storage paths")?;
        let builder = MixnetClientBuilder::new_with_default_storage(storage_paths)
            .await
            .context("Failed to initialize Nym client builder")?;
        let unconnected = builder.build().context("Failed to build Nym client")?;
        let client = unconnected
            .connect_to_mixnet()
            .await
            .context("Failed to connect to Nym gateway")?;

        let address = client.nym_address();
        eprintln!("Connected to Nym Mixnet! Our address:");
        eprintln!("  {}", address);
        eprintln!();

        Ok(Self { client })
    }

    pub async fn send_bytes(&mut self, recipient: &Recipient, data: &[u8]) -> Result<()> {
        self.client
            .send_plain_message(*recipient, data.to_vec())
            .await
            .context("Failed to send message via Mixnet")?;
        Ok(())
    }

    pub async fn receive(&mut self) -> Result<Vec<(String, Vec<u8>)>> {
        match tokio::time::timeout(Duration::from_secs(2), self.client.wait_for_messages()).await {
            Ok(Some(messages)) => Ok(messages
                .into_iter()
                .map(|m| {
                    let tag = format!("{:?}", m.sender_tag);
                    (tag, m.message)
                })
                .collect()),
            Ok(None) => Ok(Vec::new()),
            Err(_) => Ok(Vec::new()),
        }
    }

    pub async fn disconnect(self) {
        eprintln!("Disconnecting from Nym Mixnet...");
        self.client.disconnect().await;
    }
}
