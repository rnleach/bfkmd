use bufkit_data::{Model, Site};
use chrono::{Datelike, FixedOffset, NaiveDateTime, TimeZone, Timelike};
use rusqlite::types::ToSql;
use rusqlite::{Connection, OpenFlags, Statement, NO_PARAMS};
use std::error::Error;
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use strum::AsStaticRef;

pub struct ClimoDB {
    conn: Connection,
}

impl ClimoDB {
    pub const CLIMO_DIR: &'static str = "climo";
    pub const CLIMO_DB: &'static str = "climo.db";

    pub fn path_to_climo_db(arch_root: &Path) -> PathBuf {
        arch_root.join(Self::CLIMO_DIR).join(Self::CLIMO_DB)
    }

    pub fn connect_or_create(arch_root: &Path) -> Result<Self, Box<dyn Error>> {
        let climo_path = arch_root.join(Self::CLIMO_DIR);
        if !climo_path.is_dir() {
            create_dir(&climo_path)?;
        }

        let db_file = climo_path.join(Self::CLIMO_DB);

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
                site         TEXT NOT NULL,
                model        TEXT NOT NULL,
                valid_time   TEXT NOT NULL,
                year_lcl     INT  NOT NULL,
                month_lcl    INT  NOT NULL,
                day_lcl      INT  NOT NULL,
                hour_lcl     INT  NOT NULL,
                haines_high  INT,
                haines_mid   INT,
                haines_low   INT,
                hdw          INT,
                conv_t_def_c REAL,
                cape_ratio   REAL,
                PRIMARY KEY (site, valid_time, model, year_lcl, month_lcl, day_lcl, hour_lcl)
            )",
            NO_PARAMS,
        )?;

        Ok(ClimoDB { conn })
    }
}

/// The struct creates and caches several prepared statements.
pub struct ClimoDBInterface<'a> {
    add_location_query: Statement<'a>,
    add_fire_data_query: Statement<'a>,
    check_exists_query: Statement<'a>,
}

impl<'a> ClimoDBInterface<'a> {
    pub fn initialize(climo_db: &'a ClimoDB) -> Result<Self, Box<dyn Error>> {
        let conn = &climo_db.conn;
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
                fire (site, model, valid_time, year_lcl, month_lcl, day_lcl, hour_lcl, 
                    haines_high, haines_mid, haines_low, hdw, conv_t_def_c, cape_ratio)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ",
        )?;

        let check_exists_query = conn.prepare(
            "
                SELECT COUNT(*) FROM 
                fire 
                WHERE site = ?1 AND MODEL = ?2 AND valid_time = ?3
            ",
        )?;

        Ok(ClimoDBInterface {
            add_location_query,
            add_fire_data_query,
            check_exists_query,
        })
    }

    #[inline]
    pub fn exists(
        &mut self,
        site: &Site,
        model: Model,
        valid_time: NaiveDateTime,
    ) -> Result<bool, Box<dyn Error>> {
        let model_str = model.as_static();

        let num: i32 = self.check_exists_query.query_row(
            &[&site.id as &ToSql, &model_str, &valid_time as &ToSql],
            |row| row.get_checked(0),
        )??;

        Ok(num > 0)
    }

    #[inline]
    pub fn add_location(
        &mut self,
        site: &Site,
        model: Model,
        valid_time: NaiveDateTime,
        lat: f64,
        lon: f64,
        elev_m: f64,
    ) -> Result<(), Box<dyn Error>> {
        self.add_location_query.execute(&[
            &site.id as &ToSql,
            &model.as_static(),
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
        site: &Site,
        model: Model,
        valid_time: NaiveDateTime,
        hns_high_mid_low: (i32, i32, i32),
        hdw: i32,
        conv_t_def_c: Option<f64>,
        cape_ratio: Option<f64>,
    ) -> Result<(), Box<dyn Error>> {
        let lcl_time = site
            .time_zone
            .unwrap_or_else(|| FixedOffset::west(0))
            .from_utc_datetime(&valid_time);
        let year_lcl = lcl_time.year();
        let month_lcl = lcl_time.month();
        let day_lcl = lcl_time.day();
        let hour_lcl = lcl_time.hour();

        self.add_fire_data_query.execute(&[
            &site.id as &ToSql,
            &model.as_static(),
            &valid_time as &ToSql,
            &year_lcl as &ToSql,
            &month_lcl as &ToSql,
            &day_lcl as &ToSql,
            &hour_lcl as &ToSql,
            &hns_high_mid_low.0 as &ToSql,
            &hns_high_mid_low.1 as &ToSql,
            &hns_high_mid_low.2 as &ToSql,
            &hdw as &ToSql,
            &conv_t_def_c as &ToSql,
            &cape_ratio as &ToSql,
        ])?;

        Ok(())
    }
}
