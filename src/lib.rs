pub mod table;

use std::io::Error;
use table::table::*;

pub fn create_table(table: Table) -> Result<(), Error> {
    table.create()?;
    Page::create(&table)?;
    // Page::create(&table)?;
    Ok(())
    // let table_metafile = fs::write(
    //     format!("{}/table", table.name),
    //     ,
    // );
}
