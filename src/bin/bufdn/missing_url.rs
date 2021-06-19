use bufkit_data::BufkitDataErr;
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

pub struct MissingUrlDb {
    db_conn: Connection,
}

impl MissingUrlDb {
    pub fn open_or_create_404_db(root: &Path) -> Result<Self, BufkitDataErr> {
        let db_file = &root.join("404.db");

        let db404 = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        db404.execute(
            "CREATE TABLE IF NOT EXISTS missing (
                url TEXT PRIMARY KEY
            )",
            [],
        )?;

        Ok(MissingUrlDb { db_conn: db404 })
    }

    pub fn is_missing(&self, url: &str) -> Result<bool, BufkitDataErr> {
        let num_missing: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM missing WHERE url = ?1",
            &[url],
            |row| row.get(0),
        )?;

        Ok(num_missing > 0)
    }

    pub fn add_url(&self, url: &str) -> Result<(), BufkitDataErr> {
        self.db_conn
            .execute("INSERT INTO missing (url) VALUES (?1)", &[url])?;

        Ok(())
    }
}
