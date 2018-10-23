use bufkit_data::{Archive, Model};
use chrono::{NaiveDate, NaiveDateTime, Utc, Datelike, Timelike};
use failure::Error;
use rusqlite::{Connection, OpenFlags, types::ToSql, NO_PARAMS};
use sounding_analysis::{hot_dry_windy, haines_high, haines_mid, haines_low};
use sounding_bufkit::BufkitData;
use std::fs::create_dir;
use std::path::Path;
use strum::AsStaticRef;

pub fn build_climo(arch: &Archive, site: &str, model: Model) -> Result<(), Error> {
    let climo_db = &create_or_overwrite(arch.get_root())?;
    let site = &site.to_uppercase();
    let model_str = model.as_static();

    let mut location_stmt = climo_db.prepare(
        "INSERT OR IGNORE INTO 
             locations (site, model, start_date, latitude, longitude, elevation_m)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)
            ",
    )?;

    let mut fire_stmt = climo_db.prepare(
        "
            INSERT OR REPLACE INTO
            fire (site, model, year, month, day, hour, haines_high, haines_mid, haines_low, hdw)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ",
    )?;

    let (start_date, end_date) = get_current_climo_date_range(climo_db, site, model_str)?;
    
    let (mut new_start_date, mut new_end_date) = if start_date == end_date {
        (
            NaiveDate::from_ymd(3000, 12, 31).and_hms(0, 0, 0),
            NaiveDate::from_ymd(1900, 1, 1).and_hms(0, 0, 0),
        )
    } else {
        (start_date, end_date)
    };

    let (mut hdw_max, mut hdw_max_date) = get_current_maximums(climo_db, site, model_str)?;

    for anal in arch
        // Get all the available init times
        .get_init_times(site, model)?
        .into_iter()
        // Filter out dates we already have in the climo database.
        .filter(|&init_time| init_time < start_date || init_time > end_date)
        // Retrieve the sounding file from the archive
        .filter_map(|init_time| arch.get_file(site, model, &init_time).ok())
        // Parse the string data as a sounding analysis series and take enough forecasts to fill in
        // between forecasts.
        .filter_map(|string_data| {
            let analysis: Vec<_> = BufkitData::new(&string_data)
                .ok()?
                .into_iter()
                .take(model.hours_between_runs() as usize)
                .collect();

            Some(analysis)
        }).
        // Flatten them into an hourly time series.
        flat_map(|anal_vec| anal_vec.into_iter())
    {
        let snd = anal.sounding();
        let valid_time = match snd.get_valid_time(){
            Some(valid_time) => valid_time,
            None => continue,
        };
        let year = valid_time.year();
        let month = valid_time.month();
        let day = valid_time.day();
        let hour = valid_time.hour();
        
        //
        //  Update inventory end time
        //
        if valid_time > new_end_date {
            new_end_date = valid_time;
        }

        if valid_time < new_start_date {
            new_start_date = valid_time;
        }

        //
        // Get the HDW
        //
        let hdw = hot_dry_windy(snd)? as i32;
        if hdw >= hdw_max {
            hdw_max = hdw;
            hdw_max_date = valid_time;
        }

        //
        // Get the hdw various Haines index values.
        //
        let hns_low = haines_low(snd).unwrap_or(0.0) as i32;
        let hns_mid = haines_mid(snd).unwrap_or(0.0) as i32;
        let hns_high = haines_high(snd).unwrap_or(0.0) as i32;
        fire_stmt.execute(&[site as &ToSql, &model_str as &ToSql, &year as &ToSql, &month as &ToSql, &day as &ToSql, &hour as &ToSql, &hns_high as &ToSql, &hns_mid as &ToSql, &hns_low as &ToSql, &hdw as &ToSql])?;

        //
        // Things only to do for 0 lead time, don' need every sounding
        //
        if anal
            .sounding()
            .get_lead_time()
            .into_option()
            .map(|lt| lt != 0)
            .unwrap_or(false)
        {
            continue;
        }

        //
        // Location climo
        //
        let location = snd.get_station_info().location();
        let elevation = snd.get_station_info().elevation().into_option();

        if let (Some(elev_m), Some((lat, lon))) = (elevation, location)
        {
            location_stmt.execute(&[
                site as &ToSql,
                &model_str  as &ToSql,
                &valid_time as &ToSql,
                &lat as &ToSql,
                &lon as &ToSql,
                &elev_m as &ToSql,
            ])?;
        }
    }

    //
    // With new dates for inventory and max value for HDW, update the db
    //
    climo_db.execute(
        "
            INSERT OR REPLACE INTO inventory (site, model, start_date, end_date)
            VALUES ($1, $2, $3, $4)
        ",
        &[site as &ToSql, &model_str as &ToSql, &new_start_date as &ToSql, &new_end_date as &ToSql],
    )?;

    climo_db.execute(
        "
            INSERT OR REPLACE INTO max (site, model, hdw, hdw_date)
            VALUES ($1, $2, $3, $4)
        ",
        &[site as &ToSql, &model_str as &ToSql, &hdw_max as &ToSql, &hdw_max_date as &ToSql],
    )?;

    Ok(())
}

fn create_or_overwrite(arch_root: &Path) -> Result<Connection, Error> {
    const CLIMO_DIR: &str = "climo";
    const CLIMO_DB: &str = "climo.db";

    let climo_path = arch_root.join(CLIMO_DIR);
    if !climo_path.is_dir() {
        create_dir(&climo_path)?;
    }

    let db_file = climo_path.join(CLIMO_DB);

    // Create and set up the database
    let db_conn = Connection::open_with_flags(
        db_file,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )?;

    //
    //  Create the inventory
    //
    db_conn.execute(
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
    db_conn.execute(
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
    db_conn.execute(
        "CREATE INDEX IF NOT EXISTS locations_idx ON locations (site, model)",
        NO_PARAMS,
    )?;

    //
    // Create the the fire climate table
    //
    db_conn.execute(
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
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS max (
            site     TEXT NOT NULL,
            model    TEXT NOT NULL,
            hdw      INT,
            hdw_date TEXT,
            PRIMARY KEY (site, model)
        )",
        NO_PARAMS,
    )?;

    Ok(db_conn)
}

fn get_current_climo_date_range(
    climo_db: &Connection,
    site: &str,
    model_str: &str,
) -> Result<(NaiveDateTime, NaiveDateTime), Error> {
    let res: Result<(Result<NaiveDateTime, _>, Result<NaiveDateTime, _>), _> = climo_db.query_row(
        "
            SELECT start_date, end_date FROM inventory WHERE site = ?1 and model = ?2
        ",
        &[&site.to_uppercase() as &ToSql, &model_str as &ToSql],
        |row| (row.get_checked(0), row.get_checked(1)),
    );
    if let Ok(res) = res {
        Ok((res.0?, res.1?))
    } else {
        let now = Utc::now().naive_utc();
        Ok((now, now))
    }
}

fn get_current_maximums(climo_db: &Connection,
    site: &str,
    model_str: &str) -> Result<(i32, NaiveDateTime), Error> {
    let res: Result<(Result<f64, _>, Result<NaiveDateTime, _>), _> = climo_db.query_row(
        "
            SELECT hdw, hdw_date FROM max WHERE site = ?1 and model = ?2
        ",
        &[&site, &model_str],
        |row| (row.get_checked(0), row.get_checked(1)),
    );
    if let Ok(res) = res {
        Ok((res.0? as i32, res.1?))
    } else {
        let now = Utc::now().naive_utc();
        Ok((0, now))
    }
}
