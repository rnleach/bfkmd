use super::builder::ErrorMessage;
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
                valid_time    TEXT NOT NULL,
                year          INT  NOT NULL,
                month         INT  NOT NULL,
                day           INT  NOT NULL,
                hour          INT  NOT NULL,
                haines_high   INT,
                haines_mid    INT,
                haines_low    INT,
                hdw           INT,
                PRIMARY KEY (site, valid_time, model, year, month, day, hour)
            )",
            NO_PARAMS,
        )?;

        Ok(ClimoDB { conn })
    }
}

/// The struct creates and caches several prepared statements.
pub struct ClimoDBInterface<'a, 'b: 'a> {
    conn: &'a Connection,
    add_location_query: Statement<'b>,
    add_fire_data_query: Statement<'b>,
}

impl<'a, 'b: 'a> ClimoDBInterface<'a, 'b> {
    pub fn initialize(climo_db: &'b ClimoDB, site: &str, model: Model) -> Result<Self, Error> {
        let conn = &climo_db.conn;
        let site = site.to_uppercase();
        let model = model.as_static().to_string();

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
                    fire (site, model, valid_time, year, month, day, hour, haines_high, haines_mid, haines_low, hdw)
                    VALUES ('{}', '{}', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ",
                site, model,
            ),
        )?;

        Ok(ClimoDBInterface {
            conn,
            add_location_query,
            add_fire_data_query,
        })
    }

    #[inline]
    pub fn get_current_climo_date_range(
        &mut self,
        site: &str,
        model: Model,
    ) -> Result<(NaiveDateTime, NaiveDateTime), Error> {
        let site = site.to_uppercase();
        let model_str = model.as_static();

        let start_res: Result<Result<NaiveDateTime, _>, _> = self.conn.query_row(
            "
                SELECT valid_time 
                FROM fire 
                WHERE site = ?1 AND model = ?2 
                ORDER BY valid_time ASC 
                LIMIT 1
            ",
            &[&site, model_str],
            |row| row.get_checked(0),
        );

        let end_res: Result<Result<NaiveDateTime, _>, _> = self.conn.query_row(
            "
                SELECT valid_time 
                FROM fire 
                WHERE site = ?1 AND model = ?2 
                ORDER BY valid_time DESC 
                LIMIT 1
            ",
            &[&site, model_str],
            |row| row.get_checked(0),
        );

        if let (Ok(Ok(start)), Ok(Ok(end))) = (start_res, end_res) {
            Ok((start, end))
        } else {
            let now = Utc::now().naive_utc();
            Ok((now, now))
        }
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
            &valid_time as &ToSql,
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
    error_stream: Sender<ErrorMessage>,
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
                error_stream.send(ErrorMessage::Critical(err));
                return;
            }
        };

        let mut climo_db = match ClimoDBInterface::initialize(&climo_db, &site, model) {
            Ok(iface) => iface,
            Err(err) => {
                error_stream.send(ErrorMessage::Critical(err));
                return;
            }
        };

        for req in recv {
            let res = match req {
                GetClimoDateRange(sender) => climo_db
                    .get_current_climo_date_range(&site, model)
                    .map(|(start, end)| ClimoDateRange(start, end))
                    .and_then(|response| {
                        sender.send(response);
                        Ok(())
                    }),
                AddLocation(start_time, lat, lon, elev_m) => {
                    climo_db.add_location(start_time, lat, lon, elev_m)
                }
                AddFire(valid_time, haines, hdw) => climo_db.add_fire(valid_time, haines, hdw),
            };

            if let Err(err) = res {
                error_stream.send(ErrorMessage::Critical(err));
                return;
            }
        }
    });

    (join_handle, sender)
}
