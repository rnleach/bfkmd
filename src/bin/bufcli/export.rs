use bufkit_data::{Archive, Model};
use climo_db::{ClimoDB, ClimoDBInterface};
use csv;
use failure::Error;
use std::fs::File;
use std::path::Path;

pub fn export_climo(
    root: &Path,
    destination: &Path,
    site: &str,
    model: Model,
) -> Result<(), Error> {
    let climo_db = ClimoDB::connect_or_create(root)?;
    let climo_db = ClimoDBInterface::initialize(&climo_db)?;

    let site = &Archive::connect(root)?.site_info(site)?;

    let fire_climo = climo_db.calc_fire_summary(site, model)?;

    let fire_climo_file_name = format!("{}_{}_fire_climo.csv", site.id, model);
    let fire_climo_path = destination.join(fire_climo_file_name);
    let file = File::create(fire_climo_path)?;
    let mut wtr = csv::Writer::from_writer(file);

    let mut headers = vec![];
    headers.push("month".to_string());
    headers.push("day".to_string());
    headers.push("min hdw".to_string());
    for i in 1..=9 {
        headers.push(format!("hdw {}th percentile", i * 10));
    }
    headers.push("max hdw".to_string());
    for i in 0..=6 {
        if i == 1 {
            continue;
        }
        headers.push(format!("percent low haines {}", i));
    }
    for i in 0..=6 {
        if i == 1 {
            continue;
        }
        headers.push(format!("percent mid haines {}", i));
    }
    for i in 0..=6 {
        if i == 1 {
            continue;
        }
        headers.push(format!("percent high haines {}", i));
    }
    headers.push("number of samples".to_string());
    wtr.write_record(&headers)?;

    for record in fire_climo {
        wtr.write_record(&record.as_strings())?;
    }

    Ok(())
}
