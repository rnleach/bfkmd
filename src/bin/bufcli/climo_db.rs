use bufkit_data::Model;
use chrono::{Datelike, NaiveDateTime, Timelike, Utc};
use crossbeam_channel::{self, Receiver, Sender};
use failure::Error;
use rusqlite::types::ToSql;
use rusqlite::{Connection, OpenFlags, Statement, NO_PARAMS};
use std::fs::create_dir;
use std::path::Path;
use std::{thread, thread::JoinHandle};
use strum::AsStaticRef;

pub struct ClimoDB {
    conn: Connection,
}

impl ClimoDB {
    pub fn open_or_create(arch_root: &Path) -> Result<Self, Error> {
        const CLIMO_DIR: &str = "climo";
        const CLIMO_DB: &str = "climo.db";

        let climo_path = arch_root.join(CLIMO_DIR);
        if !climo_path.is_dir() {
            create_dir(&climo_path)?;
        }

        let db_file = climo_path.join(CLIMO_DB);

        // Create and set up the database
        let conn = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        //
        //  Create the inventory
        //
        conn.execute(
            "
               CREATE TABLE IF NOT EXISTS inventory (
                   site TEXT NOT NULL,
                   model TEXT NOT NULL,
                   start_date TEXT NOT NULL,
                   end_date TEXT NOT NULL,
                   PRIMARY KEY (site, model)
               )
            ",
            NO_PARAMS,
        )?;

        //
        //  Create the locations
        //
        conn.execute(
            "CREATE TABLE IF NOT EXISTS locations (
                site        TEXT NOT NULL,
                model       TEXT NOT NULL,
                start_date  TEXT NOT NULL,
                latitude    NUM  NOT NULL,
                longitude   NUM  NOT NULL,
                elevation_m NUM  NOT NULL,
                UNIQUE(site, model, latitude, longitude, elevation_m)
            )",
            NO_PARAMS,
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS locations_idx ON locations (site, model)",
            NO_PARAMS,
        )?;

        //
        // Create the the fire climate table
        //
        conn.execute(
            "CREATE TABLE IF NOT EXISTS fire (
                site          TEXT NOT NULL,
                model         TEXT NOT NULL,
                year          INT  NOT NULL,
                month         INT  NOT NULL,
                day           INT  NOT NULL,
                hour          INT  NOT NULL,
                haines_high   INT,
                haines_mid    INT,
                haines_low    INT,
                hdw           INT,
                PRIMARY KEY (site, model, year, month, day, hour)
            )",
            NO_PARAMS,
        )?;

        Ok(ClimoDB { conn })
    }
}

/// The struct creates and caches several prepared statements.
pub struct ClimoDBInterface<'a> {
    get_date_range_query: Statement<'a>,
    update_inventory_query: Statement<'a>,
    add_location_query: Statement<'a>,
    add_fire_data_query: Statement<'a>,
}

impl<'a> ClimoDBInterface<'a> {
    pub fn initialize(climo_db: &'a ClimoDB, site: &str, model: Model) -> Result<Self, Error> {
        let conn = &climo_db.conn;
        let site = site.to_uppercase();
        let model = model.as_static().to_string();

        let get_date_range_query = conn.prepare(&format!(
            "
                    SELECT start_date, end_date FROM inventory WHERE site = '{}' and model = '{}'
                ",
            site, model,
        ))?;

        let update_inventory_query = conn.prepare(&format!(
            "
                    INSERT OR REPLACE INTO inventory (site, model, start_date, end_date)
                    VALUES ('{}', '{}', $1, $2)
                ",
            site, model,
        ))?;

        let add_location_query = conn.prepare(&format!(
            "
                    INSERT OR IGNORE INTO 
                    locations (site, model, start_date, latitude, longitude, elevation_m)
                    VALUES('{}', '{}', ?1, ?2, ?3, ?4)
                ",
            site, model,
        ))?;

        let add_fire_data_query = conn.prepare(
            &format!(
                "
                    INSERT OR REPLACE INTO
                    fire (site, model, year, month, day, hour, haines_high, haines_mid, haines_low, hdw)
                    VALUES ('{}', '{}', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ",
                site, model,
            ),
        )?;

        Ok(ClimoDBInterface {
            get_date_range_query,
            update_inventory_query,
            add_location_query,
            add_fire_data_query,
        })
    }

    #[inline]
    pub fn get_current_climo_date_range(
        &mut self,
    ) -> Result<(NaiveDateTime, NaiveDateTime), Error> {
        let res: Result<(Result<NaiveDateTime, _>, Result<NaiveDateTime, _>), _> = self
            .get_date_range_query
            .query_row(NO_PARAMS, |row| (row.get_checked(0), row.get_checked(1)));

        if let Ok(res) = res {
            Ok((res.0?, res.1?))
        } else {
            let now = Utc::now().naive_utc();
            Ok((now, now))
        }
    }

    #[inline]
    pub fn update_inventory(
        &mut self,
        new_start_date: NaiveDateTime,
        new_end_date: NaiveDateTime,
    ) -> Result<(), Error> {
        self.update_inventory_query
            .execute(&[&new_start_date as &ToSql, &new_end_date as &ToSql])?;

        Ok(())
    }

    #[inline]
    pub fn add_location(
        &mut self,
        valid_time: NaiveDateTime,
        lat: f64,
        lon: f64,
        elev_m: f64,
    ) -> Result<(), Error> {
        self.add_location_query.execute(&[
            &valid_time as &ToSql,
            &lat as &ToSql,
            &lon as &ToSql,
            &elev_m as &ToSql,
        ])?;

        Ok(())
    }

    #[inline]
    pub fn add_fire(
        &mut self,
        valid_time: NaiveDateTime,
        hns_high_mid_low: (i32, i32, i32),
        hdw: i32,
    ) -> Result<(), Error> {
        let year = valid_time.year();
        let month = valid_time.month();
        let day = valid_time.day();
        let hour = valid_time.hour();

        self.add_fire_data_query.execute(&[
            &year as &ToSql,
            &month as &ToSql,
            &day as &ToSql,
            &hour as &ToSql,
            &hns_high_mid_low.0 as &ToSql,
            &hns_high_mid_low.1 as &ToSql,
            &hns_high_mid_low.2 as &ToSql,
            &hdw as &ToSql,
        ])?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum DBRequest {
    GetClimoDateRange(Sender<DBResponse>),
    UpdateInventory(NaiveDateTime, NaiveDateTime),
    AddLocation(NaiveDateTime, f64, f64, f64),
    AddFire(NaiveDateTime, (i32, i32, i32), i32),
}

#[derive(Debug)]
pub enum DBResponse {
    ClimoDateRange(NaiveDateTime, NaiveDateTime),
}

pub fn start_climo_db_thread(
    arch_root: &Path,
    site: &str,
    model: Model,
    quitter: Sender<Error>,
) -> (JoinHandle<()>, Sender<DBRequest>) {
    use self::DBRequest::*;
    use self::DBResponse::*;

    let (sender, recv): (Sender<DBRequest>, Receiver<_>) = crossbeam_channel::bounded(256);
    let root = arch_root.to_owned();
    let site = site.to_string();

    let join_handle = thread::spawn(move || -> () {
        let climo_db = match ClimoDB::open_or_create(&root) {
            Ok(db) => db,
            Err(err) => {
                quitter.send(err);
                return;
            }
        };

        let mut climo_db = match ClimoDBInterface::initialize(&climo_db, &site, model) {
            Ok(iface) => iface,
            Err(err) => {
                quitter.send(err);
                return;
            }
        };

        for req in recv {
            let res = match req {
                GetClimoDateRange(sender) => climo_db
                    .get_current_climo_date_range()
                    .map(|(start, end)| ClimoDateRange(start, end))
                    .and_then(|response| {
                        sender.send(response);
                        Ok(())
                    }),
                UpdateInventory(start, end) => climo_db.update_inventory(start, end),
                AddLocation(start_time, lat, lon, elev_m) => {
                    climo_db.add_location(start_time, lat, lon, elev_m)
                }
                AddFire(valid_time, haines, hdw) => climo_db.add_fire(valid_time, haines, hdw),
            };

            if let Err(err) = res {
                quitter.send(err);
                return;
            }
        }
    });

    (join_handle, sender)
}
