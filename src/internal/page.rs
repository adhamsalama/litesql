use crate::internal::table::Table;
use serde::{Deserialize, Serialize};

use std::{
    fs,
    io::{self, Write},
};

#[derive(Serialize, Deserialize)]
pub struct Page {
    number: i64,
    content: Vec<u8>,
    // page_size: i64,
}
impl Page {
    pub fn create(table: &Table) -> Result<(), io::Error> {
        let files = fs::read_dir(&table.name)?.count();
        println!("{files}");
        fs::write(format!("{}/page_{}", &table.name, files - 1), "")?;
        Ok(())
    }
    pub fn write(table: &Table, page_num: i64, bytes: &String) -> Result<(), io::Error> {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{}/page_{}", &table.name, page_num))?;
        let content = format!("{}\r\n", bytes);
        file.write(content.as_bytes())?;

        Ok(())
    }
    pub fn write_bytes(table: &Table, page_num: i64, bytes: &Vec<u8>) -> Result<(), io::Error> {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(format!("{}/page_{}", &table.name, page_num))?;
        file.write(bytes)?;

        Ok(())
    }
    pub fn read(table: &Table, page_num: i64) -> Result<Vec<u8>, io::Error> {
        Ok(fs::read(format!("{}/page_{}", &table.name, page_num))?)
    }
}
