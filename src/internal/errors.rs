use std::io;

#[derive(Debug)]
pub enum InsertRowError {
    IOError(io::Error),
    InsertedValuesDoNotMatchNumberOfTableColumns,
    UnmatchingType,
}

#[derive(Debug)]
pub enum SelectRowError {
    IOError(io::Error),
    SyntaxError,
    UnknownColumn,
    UnkownOperation,
}

#[derive(Debug)]
pub enum QueryError {
    UnknownTable,
    SyntaxError,
    UnknownColumn,
    UnkownOperation,
    InsertMustSpecifyAllColumns,
    InsertRowError(InsertRowError),
}
