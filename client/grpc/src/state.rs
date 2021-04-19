use anyhow::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct DatasetConfig {
    /// Unique dataset id
    id: String,

    /// Last committed valid server state from previous session in base64 format.
    /// Should be empty string if it's first session with server.
    merkle_root: String,

    /// Number of version for current dataset state
    version: usize,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        DatasetConfig {
            id: "default_dataset".to_string(),
            merkle_root: "".to_string(),
            version: 0,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ClientState {
    datasets: Vec<DatasetConfig>,
}

impl Default for ClientState {
    fn default() -> Self {
        ClientState {
            datasets: vec![DatasetConfig::default()],
        }
    }
}

impl ClientState {
    /// Reads state from json file
    pub fn read(path: PathBuf) -> Result<Self> {
        let json_file = File::open(path).context("Client state file not found")?;
        let state: ClientState = serde_json::from_reader(json_file)?;
        Ok(state)
    }

    /// Saves state to json file
    pub fn write(&self, path: PathBuf) -> Result<()> {
        let json_file = File::create(path).context("Can't create/truncate file")?;
        serde_json::to_writer(json_file, self)?;
        Ok(())
    }

    /// Tries read state from json file, if file is not exists,
    /// creates default state and safe to json file.
    pub fn read_or_create(path: PathBuf) -> Result<ClientState> {
        let state = ClientState::read(path.clone()).unwrap_or_else(|_| {
            // safe default config to json file
            let st = ClientState::default();
            st.write(path)
                .context("Can't write state to file")
                .unwrap();
            st
        });

        Ok(state)
    }
}
