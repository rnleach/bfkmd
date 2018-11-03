use bufkit_data::{Model, Site};
use chrono::{Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Timelike};
use failure::Error;
use rusqlite::types::ToSql;
use rusqlite::{Connection, OpenFlags, Statement, NO_PARAMS};
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

    pub fn connect_or_create(arch_root: &Path) -> Result<Self, Error> {
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
                site          TEXT NOT NULL,
                model         TEXT NOT NULL,
                valid_time    TEXT NOT NULL,
                year_lcl      INT  NOT NULL,
                month_lcl     INT  NOT NULL,
                day_lcl       INT  NOT NULL,
                hour_lcl      INT  NOT NULL,
                haines_high   INT,
                haines_mid    INT,
                haines_low    INT,
                hdw           INT,
                PRIMARY KEY (site, valid_time, model, year_lcl, month_lcl, day_lcl, hour_lcl)
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
    check_exists_query: Statement<'b>,
}

impl<'a, 'b: 'a> ClimoDBInterface<'a, 'b> {
    pub fn initialize(climo_db: &'b ClimoDB) -> Result<Self, Error> {
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
                    haines_high, haines_mid, haines_low, hdw)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
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
            conn,
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
    ) -> Result<bool, Error> {
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
    ) -> Result<(), Error> {
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
    ) -> Result<(), Error> {
        let lcl_time = site
            .time_zone
            .unwrap_or_else(|| FixedOffset::west(0))
            .from_utc_datetime(&valid_time);
        let year_lcl = lcl_time.year();
        let month_lcl = lcl_time.month();
        let day_lcl = lcl_time.day();
        let hour_lcl = lcl_time.hour();

        print!("adding fire...");

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
        ])?;
        println!("added {} {} {}", site.id, model, valid_time);

        Ok(())
    }

    pub fn calc_fire_summary(
        &self,
        site: &Site,
        model: Model,
    ) -> Result<Vec<FireSummaryRow>, Error> {
        let mut to_return = Vec::with_capacity(366);

        // Get the daily max HDW
        let mut hdw_stmt = self.conn.prepare(
            "
                SELECT month_lcl,day_lcl,MAX(hdw) 
                FROM fire 
                WHERE site = $1 and model = $2 
                GROUP BY year_lcl,month_lcl,day_lcl
                ORDER BY hdw ASC
            ",
        )?;

        let daily_max_hdw: Result<Vec<(i32, i32, i32)>, _> = hdw_stmt
            .query_map(&[&site.id as &ToSql, &model.as_static()], |row| {
                (row.get(0), row.get(1), row.get(2))
            })?.collect();
        let daily_max_hdw = daily_max_hdw?;

        if daily_max_hdw.is_empty() {
            return Err(format_err!("Not enough data"));
        }

        let mut haines_stmt = self.conn.prepare(
            "
                SELECT month_lcl,day_lcl,haines_high,haines_mid,haines_low 
                FROM fire 
                WHERE site =$1 AND model = $2 AND hour_lcl = 16
            ",
        )?;
        let evening_haines: Result<Vec<(i32, i32, i32, i32, i32)>, _> = haines_stmt
            .query_map(&[&site.id as &ToSql, &model.as_static()], |row| {
                (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4))
            })?.collect();
        let evening_haines = evening_haines?;

        static DAYS_PER_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

        for month in 1u32..=12 {
            for day in 1..=DAYS_PER_MONTH[month as usize - 1] {
                // Use 2019 cause not a leap year
                let center_date = NaiveDate::from_ymd(2019, month, day);
                let first_date = center_date - Duration::days(7);
                let last_date = center_date + Duration::days(7);

                let first_month = first_date.month() as i32;
                let first_day = first_date.day() as i32;
                let last_month = last_date.month() as i32;
                let last_day = last_date.day() as i32;

                let hdw_vals: Vec<i32> = daily_max_hdw
                    .iter()
                    .filter_map(|&(month, day, hdw)| {
                        if (first_month == last_month
                            && month == first_month
                            && day >= first_day
                            && day <= last_day)
                            || (first_month != last_month
                                && month == first_month
                                && day >= first_day)
                            || (first_month != last_month && month == last_month && day <= last_day)
                        {
                            Some(hdw)
                        } else {
                            None
                        }
                    }).collect();

                if hdw_vals.is_empty() {
                    return Err(format_err!("Not enough data"));
                }

                let pct_idx = |pctl: usize, len: usize| -> usize {
                    ((len - 1) as f32 / 100.0 * pctl as f32).round() as usize
                };

                let num_samples = hdw_vals.len();

                let hdw_pcts: [i32; 11] = [
                    hdw_vals[0],
                    hdw_vals[pct_idx(10, hdw_vals.len())],
                    hdw_vals[pct_idx(20, hdw_vals.len())],
                    hdw_vals[pct_idx(30, hdw_vals.len())],
                    hdw_vals[pct_idx(40, hdw_vals.len())],
                    hdw_vals[pct_idx(50, hdw_vals.len())],
                    hdw_vals[pct_idx(60, hdw_vals.len())],
                    hdw_vals[pct_idx(70, hdw_vals.len())],
                    hdw_vals[pct_idx(80, hdw_vals.len())],
                    hdw_vals[pct_idx(90, hdw_vals.len())],
                    hdw_vals[hdw_vals.len() - 1],
                ];

                let mut haines_total = 0;
                let mut haines_high = (0, 0, 0, 0, 0, 0);
                let mut haines_mid = (0, 0, 0, 0, 0, 0);
                let mut haines_low = (0, 0, 0, 0, 0, 0);

                evening_haines
                    .iter()
                    .filter_map(|&(month, day, high, mid, low)| {
                        if (first_month == last_month
                            && month == first_month
                            && day >= first_day
                            && day <= last_day)
                            || (first_month != last_month
                                && month == first_month
                                && day >= first_day)
                            || (first_month != last_month && month == last_month && day <= last_day)
                        {
                            Some((high, mid, low))
                        } else {
                            None
                        }
                    }).for_each(|(high, mid, low)| {
                        haines_total += 1;
                        match high {
                            0 => haines_high.0 += 1,
                            2 => haines_high.1 += 1,
                            3 => haines_high.2 += 1,
                            4 => haines_high.3 += 1,
                            5 => haines_high.4 += 1,
                            6 => haines_high.5 += 1,
                            _ => panic!("Invalid Haines value"),
                        }
                        match mid {
                            0 => haines_mid.0 += 1,
                            2 => haines_mid.1 += 1,
                            3 => haines_mid.2 += 1,
                            4 => haines_mid.3 += 1,
                            5 => haines_mid.4 += 1,
                            6 => haines_mid.5 += 1,
                            _ => panic!("Invalid Haines value"),
                        }
                        match low {
                            0 => haines_low.0 += 1,
                            2 => haines_low.1 += 1,
                            3 => haines_low.2 += 1,
                            4 => haines_low.3 += 1,
                            5 => haines_low.4 += 1,
                            6 => haines_low.5 += 1,
                            _ => panic!("Invalid Haines value"),
                        }
                    });
                let haines_total = haines_total as f64;
                let haines_high_pcts = [
                    haines_high.0 as f64 / haines_total,
                    haines_high.1 as f64 / haines_total,
                    haines_high.2 as f64 / haines_total,
                    haines_high.3 as f64 / haines_total,
                    haines_high.4 as f64 / haines_total,
                    haines_high.5 as f64 / haines_total,
                ];
                let haines_mid_pcts = [
                    haines_mid.0 as f64 / haines_total,
                    haines_mid.1 as f64 / haines_total,
                    haines_mid.2 as f64 / haines_total,
                    haines_mid.3 as f64 / haines_total,
                    haines_mid.4 as f64 / haines_total,
                    haines_mid.5 as f64 / haines_total,
                ];
                let haines_low_pcts = [
                    haines_low.0 as f64 / haines_total,
                    haines_low.1 as f64 / haines_total,
                    haines_low.2 as f64 / haines_total,
                    haines_low.3 as f64 / haines_total,
                    haines_low.4 as f64 / haines_total,
                    haines_low.5 as f64 / haines_total,
                ];

                to_return.push(FireSummaryRow {
                    month,
                    day,
                    hdw_pcts,
                    haines_low_pcts,
                    haines_mid_pcts,
                    haines_high_pcts,
                    num_samples,
                });
            }
        }

        Ok(to_return)
    }
}

pub struct FireSummaryRow {
    // All values should use a 15 day sliding window centered on the month/day
    month: u32,
    day: u32,
    hdw_pcts: [i32; 11], // [min, 10, 20, 30, ..., 80, 60, max] min->deciles->max
    haines_low_pcts: [f64; 6], // [0,2,3,4,5,6] relative frequency.
    haines_mid_pcts: [f64; 6], //            "
    haines_high_pcts: [f64; 6], //           "
    num_samples: usize,
}

impl FireSummaryRow {
    pub fn as_strings(&self) -> Vec<String> {
        let mut to_return = vec![];
        to_return.push(self.month.to_string());
        to_return.push(self.day.to_string());
        for percentile in &self.hdw_pcts {
            to_return.push(percentile.to_string());
        }
        for percent in &self.haines_low_pcts {
            to_return.push(percent.to_string());
        }
        for percent in &self.haines_mid_pcts {
            to_return.push(percent.to_string());
        }
        for percent in &self.haines_high_pcts {
            to_return.push(percent.to_string());
        }
        to_return.push(self.num_samples.to_string());

        to_return
    }
}
