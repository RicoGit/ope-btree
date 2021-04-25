use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::iter::FromIterator;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatasetConfig {
    /// Last committed valid server state from previous session in base64 format.
    /// Should be empty string if it's first session with server.
    pub merkle_root: String,

    /// Number of version for current dataset state
    pub version: usize,
}

impl DatasetConfig {
    pub fn state(&self) -> String {
        format!("{}, {}", self.version, self.merkle_root)
    }
}

impl Default for DatasetConfig {
    fn default() -> Self {
        DatasetConfig {
            merkle_root: "".to_string(),
            version: 0,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClientConfig {
    pub datasets: HashMap<String, DatasetConfig>,
}

impl ClientConfig {
    pub fn default_ds_name() -> String {
        "default_dataset".to_string()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            datasets: HashMap::from_iter(vec![(
                ClientConfig::default_ds_name(),
                DatasetConfig::default(),
            )]),
        }
    }
}
