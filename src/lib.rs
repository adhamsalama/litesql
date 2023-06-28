pub mod internal;

use internal::page::Page;
use internal::table::Table;
use std::io::Error;

pub fn create_table(table: Table) -> Result<(), Error> {
    table.save()?;
    Page::create(&table)?;
    // Page::create(&table)?;
    Ok(())
    // let table_metafile = fs::write(
    //     format!("{}/table", table.name),
    //     ,
    // );
}
