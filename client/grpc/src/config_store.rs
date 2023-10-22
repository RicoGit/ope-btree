use anyhow::*;
use client::ope_db::config::ClientConfig;
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug)]
pub struct ConfigStore {
    pub config: ClientConfig,
    state_file: PathBuf,
}

impl ConfigStore {
    /// Tries read config from json file, if file is not exists,
    /// creates default state and safe to json file.
    pub fn read_or_create(path: PathBuf) -> ConfigStore {
        ConfigStore::read(path.clone()).unwrap_or_else(|_| {
            // safe default config to json file
            let store = ConfigStore {
                config: ClientConfig::default(),
                state_file: path.clone(),
            };

            store.write().context("Can't write state to file").unwrap();
            store
        })
    }

    /// Reads config from json file
    pub fn read(path: PathBuf) -> Result<ConfigStore> {
        let json_file = File::open(&path).context("Client state file not found")?;
        let state: ClientConfig = serde_json::from_reader(json_file)?;
        Ok(ConfigStore {
            config: state,
            state_file: path,
        })
    }

    /// Safes specified config to json file
    pub fn set_and_safe(&mut self, state: ClientConfig) -> Result<()> {
        self.config = state;
        self.write()
    }

    /// Saves config to json file
    fn write(&self) -> Result<()> {
        let json_file =
            File::create(self.state_file.clone()).context("Can't create/truncate file")?;
        serde_json::to_writer(json_file, &self.config)?;
        Ok(())
    }
}
