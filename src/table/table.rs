use std::{fs, io};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    // path: String,
    pub columns: Vec<Column>,
    // pub options: TableOptions,
}
impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Table {
        Table { name, columns }
    }
    pub fn create(&self) -> Result<(), io::Error> {
        fs::create_dir(&self.name)?;
        let mut table_metadata = format!("{}\r\n", &self.name);
        for column in &self.columns {
            let c_type = match column._type {
                ColumnType::Int => "int",
            };
            let line = format!("{}:{}\r\n", column.name, c_type);
            table_metadata.push_str(&line);
        }
        fs::write(format!("{}/table", &self.name), table_metadata)?;
        Ok(())
    }
    pub fn insert_row(&self, row: Vec<ColumnValue>) -> Result<(), io::Error> {
        let mut bytes_to_write = String::new();
        for column in row {
            match column {
                ColumnValue::Int(value) => bytes_to_write.push_str(&value.to_string()),
            }
        }
        Page::write(&self, 0, bytes_to_write)?;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub _type: ColumnType,
}

#[derive(Serialize, Deserialize)]
pub enum ColumnType {
    Int,
}

pub enum ColumnValue {
    Int(i64),
}

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
    pub fn write(table: &Table, page_num: i64, bytes: String) -> Result<(), io::Error> {
        Ok(fs::write(
            format!("{}/page_{}", &table.name, page_num),
            bytes,
        )?)
    }
    pub fn read(table: &Table, page_num: i64) -> Result<Vec<u8>, io::Error> {
        Ok(fs::read(format!("{}/page_{}", &table.name, page_num))?)
    }
}
