use crate::internal::{errors, page::Page};
use csv;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    fs,
    io::{self},
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
    pub fn save(&self) -> Result<(), io::Error> {
        fs::create_dir(&self.name)?;
        let serialized = serde_json::to_string(&self).unwrap();
        fs::write(format!("{}/table.json", &self.name), serialized)?;
        Ok(())
    }
    pub fn insert_row(&self, row: Vec<ColumnValue>) -> Result<(), errors::InsertRowError> {
        if &row.len() != &self.columns.len() {
            return Err(errors::InsertRowError::InsertedValuesDoNotMatchNumberOfTableColumns);
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
    pub fn select(&self, columns: &Vec<&String>) -> Vec<ColumnValue> {
        // indexes of selected table columns
        let column_indexes: Vec<usize> = columns
            .iter()
            .map(|c| {
                let column = self.columns.iter().position(|col| col.name == **c).unwrap();
                column
            })
            .collect();
        let mut results: Vec<u8> = Vec::new();
        let pages = fs::read_dir(&self.name)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_name().to_str().unwrap().contains("page_"))
            .collect::<Vec<_>>();
        for (index, _) in pages.iter().enumerate() {
            let mut page_content = Page::read(&self, index as i64).unwrap();
            results.append(&mut page_content);
        }
        let data = match std::str::from_utf8(&results) {
            Ok(s) => s.to_owned(),
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        };
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(data.as_bytes());
        // let mut results = Vec::new();
        let mut rows = Vec::new();
        for result in csv_reader.records() {
            // let mut seslected_columns = Vec::new();
            let record: csv::StringRecord = result.unwrap();
            for index in column_indexes.iter() {
                let column = &self.columns[*index];
                match column._type {
                    ColumnType::Int => {
                        let column = record.get(*index).unwrap().parse::<i64>().unwrap();
                        rows.push(ColumnValue::Int(column));
                    }
                    ColumnType::Str => {
                        let column = record.get(*index).unwrap().to_string();
                        rows.push(ColumnValue::Str(column));
                    }
                }
            }
        }
        rows
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub _type: ColumnType,
}

pub enum QueryResult {
    Rows(Vec<ColumnValue>),
    InsertRowSucceeded,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ColumnType {
    Int,
    Str,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ColumnValue {
    Int(i64),
    // Float(f64),
    Str(String),
}
