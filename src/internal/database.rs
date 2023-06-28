use crate::internal::{
    errors,
    table::{Column, QueryResult, Table},
};
use csv;
use serde::{Deserialize, Serialize};
use serde_json;
use sqlparser::ast::{Query, Select, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::{
    fs,
    io::{self, Write},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    tables: Vec<Table>,
}
impl Database {
    pub fn new() -> Database {
        Database { tables: Vec::new() }
    }
    pub fn create_table(&mut self, name: &String, columns: Vec<Column>) -> Result<(), io::Error> {
        let table = Table::new(name.clone(), columns);
        table.save()?;
        self.tables.push(table);
        self.save();
        Ok(())
    }
    pub fn save(&self) -> () {
        let serialized = serde_json::to_string(&self).unwrap();
        fs::write(format!("database.json"), serialized).unwrap();
        ()
    }
    pub fn load() -> Database {
        let metadata = fs::read_to_string("database.json").unwrap();
        let metadata: Database = serde_json::from_str(&metadata).unwrap();
        metadata
    }
    pub fn query(&self, sql: String) -> Result<QueryResult, errors::QueryError> {
        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let statements = Parser::parse_sql(&dialect, &sql).unwrap();
        let first = statements.first().unwrap();
        // match select statement
        let mut selected_columns = Vec::new();

        match first {
            Statement::Query(query) => match *query.body.clone() {
                SetExpr::Select(select) => {
                    let table_name = match &select.from[0].relation {
                        sqlparser::ast::TableFactor::Table {
                            name,
                            alias,
                            args,
                            with_hints,
                        } => name.to_string(),
                        _ => todo!("only simple selects are implemented"),
                    };
                    let table = &self.tables.iter().find(|t| t.name == table_name);
                    match table {
                        None => return Err(errors::QueryError::UnknownTable),
                        Some(table) => {
                            for i in 0..select.projection.len() {
                                let column = select.projection.get(i).unwrap();
                                match column {
                                    sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                                        selected_columns.push(expr.to_string());
                                        if selected_columns.len() > table.columns.len() {
                                            return Err(errors::QueryError::UnknownColumn);
                                        }
                                        let known_columns: Vec<_> = selected_columns
                                            .iter()
                                            .filter(|c| {
                                                let column = table
                                                    .columns
                                                    .iter()
                                                    .find(|col| col.name == **c);
                                                match column {
                                                    Some(_) => true,
                                                    None => false,
                                                }
                                            })
                                            .collect();
                                        if known_columns.len() != selected_columns.len() {
                                            return Err(errors::QueryError::UnknownColumn);
                                        }
                                        println!("known_columns = {:?}", known_columns);
                                        let r = table.select(&known_columns);
                                        return Ok(QueryResult::Rows(r));
                                    }
                                    // sqlparser::ast::SelectItem::Wildcard(expr) => {
                                    //     let name = String::from("*");
                                    //     let column = Column {
                                    //         name,
                                    //         _type: ColumnType::Int,
                                    //     };
                                    //     columns.push(column);
                                    // }
                                    // sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                                    //     let name = alias.value.clone();
                                    //     let column = Column {
                                    //         name,
                                    //         _type: ColumnType::Int,
                                    //     };
                                    //     columns.push(column);
                                    // }
                                    _ => todo!("not implemented"),
                                }
                            }
                            return Ok(QueryResult::Rows(Vec::new()));
                        }
                    };
                }
                _ => panic!("no"),
            },
            Statement::Insert {
                or,
                into,
                table_name,
                columns,
                overwrite,
                source,
                partitioned,
                after_columns,
                table,
                on,
                returning,
            } => {
                println!("table {}", table_name);
                todo!("insert")
            }
            _ => panic!("Err(SelectRowError::UnkownOperation)"),
        };
    }
}
