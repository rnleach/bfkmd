use chrono::{NaiveDate, NaiveDateTime};

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