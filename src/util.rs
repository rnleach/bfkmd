use bufkit_data::{Archive, Model, StationNumber};
use chrono::{NaiveDate, NaiveDateTime};
use std::{error::Error, fmt::Display};

pub fn bail(msg: &str) -> ! {
    println!("{}", msg);
    ::std::process::exit(1);
}

pub fn parse_date_string(dt_str: &str) -> NaiveDateTime {
    let hour: u32 = match dt_str[11..].parse() {
        Ok(hour) => hour,
        Err(_) => bail(&format!("Could not parse date: {}", dt_str)),
    };

    let date = match NaiveDate::parse_from_str(&dt_str[..10], "%Y-%m-%d") {
        Ok(date) => date,
        Err(_) => bail(&format!("Could not parse date: {}", dt_str)),
    };

    date.and_hms(hour, 0, 0)
}

pub fn site_id_to_station_num(arch: &Archive, id: &str) -> Result<StationNumber, StrErr> {
    let mut value = 0u32;
    for &model in &[Model::GFS, Model::NAM, Model::NAM4KM] {
        let val: u32 = match arch.station_num_for_id(id, model) {
            Ok(stn_num) => Into::<u32>::into(stn_num),
            Err(_) => continue,
        };

        if value == 0 || value == val {
            value = val;
        } else {
            return Err(StrErr {
                msg: "ambigous id, multiple matches found",
            });
        }
    }

    if value == 0 {
        return Err(StrErr {
            msg: "no matching station found for id",
        });
    }

    Ok(bufkit_data::StationNumber::from(value))
}

#[derive(Debug)]
pub struct StrErr {
    pub msg: &'static str,
}

impl Display for StrErr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.msg)
    }
}

impl Error for StrErr {}
