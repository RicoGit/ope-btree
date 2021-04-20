use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum Cmd {
    Get(String),
    Put(String, String),
    New(String),
    Exit,
}

impl FromStr for Cmd {
    type Err = String;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let mut words = str.trim().split_ascii_whitespace();
        match words.next() {
            Some("get") => Ok(Cmd::Get(
                words.next().ok_or("usage: get <Key>")?.to_string(),
            )),
            Some("put") => Ok(Cmd::Put(
                words.next().ok_or("usage: put <Key> <Value>")?.to_string(),
                words.next().ok_or("usage: put <Key> <Value>")?.to_string(),
            )),
            Some("new") => Ok(Cmd::New(
                words.next().ok_or("usage: use <Dataset id>")?.to_string(),
            )),
            Some("exit") => Ok(Cmd::Exit),
            Some(_) | None => Err("Unknown command, required [get, put, use, exit]".to_string()),
        }
    }
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
