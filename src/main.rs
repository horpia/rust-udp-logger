extern crate logger;

use std::error;
use std::net::UdpSocket;
use std::sync::{mpsc, RwLock};
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time;
use sqlite::Connection;
use logger::config::Config;
use logger::errors::ErrLogger;
use logger::message::*;
use std::sync::{Arc};

const MAIN_LOOP_TIMEOUT_MS: u64 = 50;
const MESSAGE_BUFFER_LENGTH: usize = 65535;
const FLUSH_MESSAGES_COUNT: usize = 500;
const FLUSH_TIME_PERIOD_MS: u128 = 4500;

fn main() {
    let config = Config::new();
    let err_logger = ErrLogger::new();

    println!("Start UDP server on {}:{}", config.ip, config.port);

    let socket = match UdpSocket::bind(format!("{}:{}", config.ip, config.port)) {
        Ok(v) => v,
        Err(e) => {
            err_logger.log(format!("Cannot connect to server. {:?}", e).as_str());
            std::process::exit(1);
        }
    };

    println!("Server is up");

    let (sender, receiver): (_, Receiver<Message>) = mpsc::channel();

    start_udp_threads(config.threads, &err_logger, &sender, &socket);
    start_main_loop(&config, &err_logger, &receiver);
}

fn start_main_loop(config: &Config, err_logger: &ErrLogger, receiver: &Receiver<Message>) {
    let mut buffer: Vec<Message> = Vec::new();
    let mut time = time::SystemTime::now();
    let lock = Arc::new(RwLock::new(false));

    loop {
        thread::sleep(time::Duration::from_millis(MAIN_LOOP_TIMEOUT_MS));

        for msg in receiver.try_iter() {
            buffer.push(msg);
        }

        if time.elapsed().unwrap().as_millis() > FLUSH_TIME_PERIOD_MS
            || buffer.len() > FLUSH_MESSAGES_COUNT {

            if *lock.read().unwrap() {
                // database is locked, previous thread not finished yet
                continue;
            }

            time = time::SystemTime::now();
            let buffer_cloned = buffer.clone();
            buffer.clear();
            flush_messages(&lock, config, &err_logger, buffer_cloned);
        }
    }
}

fn flush_messages(lock: &Arc<RwLock<bool>>, config: &Config, err_logger: &ErrLogger, buffer: Vec<Message>) {
    let thread_err_logger = err_logger.clone();
    if buffer.len() == 0 {
        return;
    }

    println!("Save logs to DB: {}", config.db_path);

    let lock_cloned = Arc::clone(lock);
    let db_file = config.db_path.clone();

    thread::spawn(move || {
        let mut lock_write = lock_cloned.write().unwrap();
        *lock_write = true;

        let mut queries_count: u32 = 0;

        let counters = collapse_counters(&buffer);
        let mut sql = String::new();
        let db = match get_db(&db_file) {
            Ok(db) => db,
            Err(e) => {
                *lock_write = false;
                thread_err_logger.log(format!("Cannot open connection to database. {:?}", e).as_str());
                return;
            }
        };

        for msg in buffer.iter() {
            if let Message::Log { group_name, time, value } = msg {
                queries_count += 1;
                sql.push_str(format!("
                    INSERT INTO logs
                        (name, time, value)
                    VALUES
                        ('{0}', {1}, '{2}');
                ", group_name, time, value.replace("'", "''")).as_str());
            }
        }

        for counter in counters.iter() {
            if let Message::Counter { counter_name, time, value } = counter {
                queries_count += 1;
                sql.push_str(format!("
                    INSERT INTO counters
                        (name, time, value)
                    VALUES
                        ('{0}', {1}, {2})
                    ON CONFLICT(name, time) DO UPDATE SET
                        value = value + ({2});
                ", counter_name, time, value).as_str());
            }
        }

        db.execute(sql).unwrap_or_else(|e| {
            thread_err_logger.log(format!("Cannot save messages to DB. {:?}", e).as_str());
        });

        println!("Saved. Total queries: {}", queries_count);

        *lock_write = false;
    });
}

fn start_udp_threads(num: u8, err_logger: &ErrLogger, sender: &Sender<Message>, socket: &UdpSocket) {
    for _ in 0..num {
        let thread_socket = socket.try_clone().unwrap();
        let thread_sender = sender.clone();
        let thread_err_logger = err_logger.clone();
        let mut buf = [0; MESSAGE_BUFFER_LENGTH];

        thread::spawn(move || loop {
            match thread_socket.recv_from(&mut buf) {
                Ok((len, _)) => {
                    match Message::new(&buf[0..len]) {
                        Ok(msg) => thread_sender.send(msg).unwrap(),
                        Err(text) => thread_err_logger.log(text)
                    };
                }
                Err(e) => {
                    let msg = format!("Error while reading socket: {:?}", e);
                    thread_err_logger.log(msg.as_str());
                }
            };
        });
    }
}

fn get_db(db_file: &String) -> Result<Connection, Box<dyn error::Error>> {
    let connection = sqlite::open(db_file).unwrap();

    connection.execute("
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            time INTEGER NOT NULL,
            value TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS logs_name_idx ON logs (name);

        CREATE TABLE IF NOT EXISTS counters (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            time INTEGER NOT NULL,
            value INTEGER NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS counters_idx ON counters (name, time);
        CREATE INDEX IF NOT EXISTS counters_name_idx ON counters (name);
    ")?;

    Ok(connection)
}