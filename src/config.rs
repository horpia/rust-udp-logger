use std::collections::HashMap;
use ini::{Ini};

const CONFIG_PATH: &str = "./config.ini";
const DEFAULT_IP: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 12345;
const DEFAULT_THREADS: u8 = 3;
const DEFAULT_DB_PATH: &str =  "./db.sqlite";

pub struct Config {
    pub ip: String,
    pub port: u16,
    pub threads: u8,
    pub db_path: String
}

impl Config {
    pub fn new() -> Config {
        let map = Config::read_file();
        Config {
            ip: map.get("server.ip").unwrap_or(&String::from(DEFAULT_IP)).to_string(),
            port: str::parse(map.get("server.port").unwrap_or(&String::from("")))
                .unwrap_or(DEFAULT_PORT),
            threads: str::parse(map.get("server.threads").unwrap_or(&String::from("")))
                .unwrap_or(DEFAULT_THREADS),
            db_path: map.get("db.path").unwrap_or(&String::from(DEFAULT_DB_PATH)).to_string(),
        }
    }

    fn read_file() -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();
        let i = Ini::load_from_file(CONFIG_PATH);
        if i.is_err() {
            return map;
        }

        for (sec, prop) in i.unwrap().iter() {
            if sec.is_none() {
                continue;
            }

            for (k, v) in prop.iter() {
                map.insert(format!("{}.{}", sec.unwrap(), k), String::from(v));
            }
        }

        map
    }
}