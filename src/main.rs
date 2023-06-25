use sqldb::table::table::{self, Column, ColumnValue, Table};
fn main() {
    let table = Table {
        name: String::from("users"),
        columns: Vec::from([Column {
            name: String::from("id"),
            _type: table::ColumnType::Int,
        }]),
    };
    // create_table(table).unwrap();
    table.insert_row(vec![ColumnValue::Int(69)]).unwrap();
}
