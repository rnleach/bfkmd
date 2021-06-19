use bufkit_data::{BufkitDataErr, StationNumber};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

pub struct AutoDownloadListDb {
    db_conn: Connection,
}

impl AutoDownloadListDb {
    pub fn open_or_create(root: &Path) -> Result<Self, BufkitDataErr> {
        let db_file = &root.join("auto_download.db");

        let db_auto_dl = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        db_auto_dl.execute(
            "CREATE TABLE IF NOT EXISTS download (
                station_num INT PRIMARY KEY
            )",
            [],
        )?;

        Ok(AutoDownloadListDb {
            db_conn: db_auto_dl,
        })
    }

    pub fn get_list(&self) -> Result<Vec<StationNumber>, BufkitDataErr> {
        let mut stmt = self.db_conn.prepare("SELECT station_num FROM download")?;

        let parse_row = |row: &rusqlite::Row| -> Result<StationNumber, rusqlite::Error> {
            let station_num: u32 = row.get(0)?;

            Ok(StationNumber::from(station_num))
        };

        let results: Result<Vec<StationNumber>, BufkitDataErr> = stmt
            .query_and_then([], parse_row)?
            .map(|res| res.map_err(BufkitDataErr::Database))
            .collect();

        results
    }

    pub fn add_site(&self, station_num: StationNumber) -> Result<(), BufkitDataErr> {
        let station_num: u32 = station_num.into();

        self.db_conn.execute(
            "INSERT OR IGNORE INTO download (station_num) VALUES (?1)",
            &[&station_num],
        )?;

        Ok(())
    }

    pub fn remove_site(&self, station_num: StationNumber) -> Result<(), BufkitDataErr> {
        let station_num: u32 = station_num.into();

        self.db_conn.execute(
            "DELETE FROM download WHERE station_num = ?1",
            &[&station_num],
        )?;

        Ok(())
    }

    pub fn is_auto_downloaded(&self, station_num: StationNumber) -> Result<bool, BufkitDataErr> {
        let station_num: u32 = station_num.into();

        let count: u32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM download WHERE station_num = ?1",
            &[&station_num],
            |row| row.get::<_, u32>(0),
        )?;

        Ok(count == 1)
    }
}
