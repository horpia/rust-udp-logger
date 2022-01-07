use std::collections::HashMap;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor, Read};
use std::error;

#[derive(Debug, Clone)]
pub enum Message {
    Log {group_name: String, time: u32, value: String},
    Counter {counter_name: String, time: u32, value: i64},
}

impl Message {
    pub fn new(data: &[u8]) -> Result<Message, &'static str> {
        let mut reader = Cursor::new(data);

        let cmd = reader.read_u8()
            .expect("Cannot read message command");

        let msg = match cmd {
            1 => Message::parse_log_cmd(&mut reader),
            2 => Message::parse_counter_cmd(&mut reader, 1),
            3 => Message::parse_counter_cmd(&mut reader, -1),
            _ => return Err("Unknown type of command")
        };

        Ok(msg.expect("Cannot parse command"))
    }

    fn parse_log_cmd(reader: &mut Cursor<&[u8]>) -> Result<Message, Box<dyn error::Error>> {
        let mut group_name = String::new();
        let len = reader.read_u8()? as u64;
        reader.take(len).read_to_string(&mut group_name)?;

        let time = reader.read_u32::<BigEndian>()?;

        let mut value = String::new();
        let len = reader.read_u16::<BigEndian>()? as u64;
        reader.take(len).read_to_string(&mut value)?;

        Ok(Message::Log {
            group_name,
            time,
            value
        })
    }

    fn parse_counter_cmd(reader: &mut Cursor<&[u8]>, value: i64) -> Result<Message, Box<dyn error::Error>> {
        let mut counter_name = String::new();
        let len = reader.read_u8()? as u64;
        reader.take(len).read_to_string(&mut counter_name)?;

        let time = reader.read_u32::<BigEndian>()?;

        Ok(Message::Counter {
            counter_name,
            time,
            value
        })
    }
}

pub fn collapse_counters(list: &Vec<Message>) -> Vec<Message> {
    let mut counters: HashMap<&String, HashMap<&u32, i64>> = HashMap::new();

    for msg in list.iter() {
        if let Message::Counter{counter_name, value, time} = msg {
            if !counters.contains_key(counter_name) {
                counters.insert(counter_name, HashMap::from([
                    (time, 0)
                ]));
            }

            let periods = counters.get_mut(counter_name).unwrap();
            if !periods.contains_key(time) {
                periods.insert(time, 0);
            }

            let v = periods.get_mut(time).unwrap();
            *v += value;
        }
    }

    let mut out: Vec<Message> = Vec::new();

    for (counter_name, periods) in counters {
        for (time, value) in periods {
            out.push(Message::Counter {
                counter_name: counter_name.clone(),
                time: *time,
                value
            });
        }
    }

    out
}