use chrono::{NaiveDateTime, Utc};
use failure::Error;
use rusqlite::types::ToSql;
use rusqlite::{Connection, OpenFlags, Statement, NO_PARAMS};
use std::fs::create_dir;
use std::path::Path;

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

        //
        // Create tables for extremes.
        //
        conn.execute(
            "CREATE TABLE IF NOT EXISTS max (
                site     TEXT NOT NULL,
                model    TEXT NOT NULL,
                hdw      INT,
                hdw_date TEXT,
                PRIMARY KEY (site, model)
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
    get_max_query: Statement<'a>,
    update_max_query: Statement<'a>,
    add_location_query: Statement<'a>,
    add_fire_data_query: Statement<'a>,
}

impl<'a> ClimoDBInterface<'a> {
    pub fn initialize(climo_db: &'a ClimoDB) -> Result<Self, Error> {
        let conn = &climo_db.conn;

        let get_date_range_query = conn.prepare(
            "
                SELECT start_date, end_date FROM inventory WHERE site = ?1 and model = ?2
            ",
        )?;

        let update_inventory_query = conn.prepare(
            "
                INSERT OR REPLACE INTO inventory (site, model, start_date, end_date)
                VALUES ($1, $2, $3, $4)
            ",
        )?;

        let get_max_query = conn.prepare(
            "
                SELECT hdw, hdw_date FROM max WHERE site = ?1 and model = ?2
            ",
        )?;

        let update_max_query = conn.prepare(
            "
                INSERT OR REPLACE INTO max (site, model, hdw, hdw_date)
                VALUES ($1, $2, $3, $4)
            ",
        )?;

        let add_location_query = conn.prepare(
            "
                INSERT OR IGNORE INTO 
                locations (site, model, start_date, latitude, longitude, elevation_m)
                VALUES(?1, ?2, ?3, ?4, ?5, ?6)
            ",
        )?;

        let add_fire_data_query = conn.prepare(
            "
                INSERT OR REPLACE INTO
                fire (site, model, year, month, day, hour, haines_high, haines_mid, haines_low, hdw)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ",
        )?;

        Ok(ClimoDBInterface {
            get_date_range_query,
            update_inventory_query,
            get_max_query,
            update_max_query,
            add_location_query,
            add_fire_data_query,
        })
    }

    #[inline]
    pub fn get_current_climo_date_range(
        &mut self,
        site: &str,
        model_str: &str,
    ) -> Result<(NaiveDateTime, NaiveDateTime), Error> {
        let res: Result<(Result<NaiveDateTime, _>, Result<NaiveDateTime, _>), _> = self
            .get_date_range_query
            .query_row(&[&site as &ToSql, &model_str as &ToSql], |row| {
                (row.get_checked(0), row.get_checked(1))
            });

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
        site: &str,
        model: &str,
        new_start_date: &NaiveDateTime,
        new_end_date: &NaiveDateTime,
    ) -> Result<(), Error> {
        self.update_inventory_query.execute(&[
            &site as &ToSql,
            &model as &ToSql,
            new_start_date as &ToSql,
            new_end_date as &ToSql,
        ])?;

        Ok(())
    }

    #[inline]
    pub fn get_current_maximums(
        &mut self,
        site: &str,
        model_str: &str,
    ) -> Result<(i32, NaiveDateTime), Error> {
        let res: Result<(Result<f64, _>, Result<NaiveDateTime, _>), _> =
            self.get_max_query.query_row(&[&site, &model_str], |row| {
                (row.get_checked(0), row.get_checked(1))
            });
        if let Ok(res) = res {
            Ok((res.0? as i32, res.1?))
        } else {
            let now = Utc::now().naive_utc();
            Ok((0, now))
        }
    }

    #[inline]
    pub fn update_maximums(
        &mut self,
        site: &str,
        model: &str,
        hdw: i32,
        hdw_date: &NaiveDateTime,
    ) -> Result<(), Error> {
        self.update_max_query.execute(&[
            &site as &ToSql,
            &model as &ToSql,
            &hdw as &ToSql,
            &hdw_date as &ToSql,
        ])?;

        Ok(())
    }

    #[inline]
    pub fn add_location(
        &mut self,
        site: &str,
        model: &str,
        valid_time: &NaiveDateTime,
        lat: f64,
        lon: f64,
        elev_m: f64,
    ) -> Result<(), Error> {
        self.add_location_query.execute(&[
            &site as &ToSql,
            &model as &ToSql,
            valid_time as &ToSql,
            &lat as &ToSql,
            &lon as &ToSql,
            &elev_m as &ToSql,
        ])?;

        Ok(())
    }

    #[inline]
    pub fn add_fire(
        &mut self,
        site: &str,
        model: &str,
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        hns_high: i32,
        hns_mid: i32,
        hns_low: i32,
        hdw: i32,
    ) -> Result<(), Error> {
        self.add_fire_data_query.execute(&[
            &site as &ToSql,
            &model as &ToSql,
            &year as &ToSql,
            &month as &ToSql,
            &day as &ToSql,
            &hour as &ToSql,
            &hns_high as &ToSql,
            &hns_mid as &ToSql,
            &hns_low as &ToSql,
            &hdw as &ToSql,
        ])?;

        Ok(())
    }
}
