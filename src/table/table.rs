use csv;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    fs,
    io::{self, Write},
};
static PAGE_SIZE: i32 = 4096;

#[derive(Debug, Serialize, Deserialize)]
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

    pub fn load(name: &String) -> Table {
        let metadata = fs::read_to_string(format!("{}/table.json", &name)).unwrap();
        let metadata: Table = serde_json::from_str(&metadata).unwrap();
        metadata
    }
    pub fn create(&self) -> Result<(), io::Error> {
        fs::create_dir(&self.name)?;
        let serialized = serde_json::to_string(&self).unwrap();
        fs::write(format!("{}/table.json", &self.name), serialized)?;
        Ok(())
    }
    pub fn insert_row(&self, row: Vec<ColumnValue>) -> Result<(), InsertRowError> {
        if &row.len() != &self.columns.len() {
            return Err(InsertRowError::InsertedValuesDoNotMatchNumberOfTableColumns);
        }
        let mut row_size = 0;
        for i in 0..self.columns.len() {
            let field = row.get(i).unwrap();
            let should_be = &self.columns[i];
            match field {
                ColumnValue::Int(_) => {
                    if let ColumnType::Int = should_be._type {
                        row_size += std::mem::size_of::<i64>();
                    } else {
                        panic!("Fields don't match. Expected Int.")
                    }
                }
                ColumnValue::Str(value) => {
                    if let ColumnType::Str = should_be._type {
                        row_size += value.capacity();
                    } else {
                        panic!("Fields don't match. Expected Str.")
                    }
                }
            }
        }
        if (row_size as i32) >= PAGE_SIZE {
            panic!("ROW SIZE IS BIGGER THAN PAGE_SIZE");
        }
        let pages = fs::read_dir(&self.name)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_name().to_str().unwrap().contains("page_"))
            .collect::<Vec<_>>();
        let last_page = pages.last();
        let mut buffer = io::Cursor::new(Vec::new());
        {
            let mut writer = csv::WriterBuilder::new()
                .has_headers(true)
                .from_writer(&mut buffer);
            writer.serialize(row).unwrap();

            writer.flush().unwrap();
        }
        let bytes = buffer.into_inner();

        if let Some(last_page) = last_page {
            // Walk over the directory and get the file size of each file
            let metadata = fs::metadata(last_page.path()).unwrap();
            let file_size = metadata.len();
            let row_size: u64 = row_size.try_into().unwrap();
            // println!("filesize = {}, rowsize = {}", file_size, row_size);
            if file_size + row_size < PAGE_SIZE as u64 {
                Page::write_bytes(&self, (pages.len() - 1) as i64, &bytes).unwrap();
            } else {
                // println!("Page {} is too not empty enough", pages.len() - 1);
                Page::write_bytes(&self, (pages.len()) as i64, &bytes).unwrap();
            }
        } else {
            Page::write_bytes(&self, 0, &bytes).unwrap();
        }
        Ok(())
    }
    // pub fn read_row()
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub _type: ColumnType,
}

#[derive(Debug)]
pub enum InsertRowError {
    IOError(io::Error),
    InsertedValuesDoNotMatchNumberOfTableColumns,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ColumnType {
    Int,
    Str,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ColumnValue {
    Int(i64),
    Str(String),
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
