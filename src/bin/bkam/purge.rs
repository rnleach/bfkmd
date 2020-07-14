use bfkmd::parse_date_string;
use bufkit_data::{Archive, Model, StationNumber};
use chrono::{NaiveDate, Utc};
use clap::ArgMatches;
use std::str::FromStr;
use std::{error::Error, path::PathBuf};
use strum::IntoEnumIterator;

pub fn purge(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(root)?;

    let sites: Vec<String> = sub_args
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(ToOwned::to_owned))
        .collect();

    let mut models: Vec<Model> = sub_args
        .values_of("models")
        .into_iter()
        .flat_map(|model_iter| model_iter.map(Model::from_str))
        .filter_map(Result::ok)
        .collect();

    let after = sub_args
        .value_of("after")
        .map(|after_str| parse_date_string(after_str))
        .unwrap_or_else(|| NaiveDate::from_ymd(1900, 1, 1).and_hms(0, 0, 0));

    let before = sub_args
        .value_of("before")
        .map(|before_str| parse_date_string(before_str))
        .unwrap_or_else(|| Utc::now().naive_utc());

    if models.is_empty() {
        models = Model::iter().collect();
    }

    for &model in &models {
        let sites: Vec<StationNumber> = if sites.is_empty() {
            arch.sites()?
                .into_iter()
                .map(|info| info.station_num)
                .filter(|&stn_num| {
                    arch.models(stn_num)
                        .map(|mdls| mdls.contains(&model))
                        .unwrap_or(false)
                })
                .collect()
        } else {
            sites
                .iter()
                .filter_map(|id| arch.station_num_for_id(id, model).ok())
                .collect()
        };

        for site in sites {
            let all_runs = model.all_runs(&after, &before);

            for run in all_runs {
                println!("  Removing {} {} {}.", site, model.as_static_str(), run);

                match arch.remove(site, model, run) {
                    Ok(()) => {}
                    Err(err) => println!("    Error removing: {}", err),
                }
            }
        }
    }

    Ok(())
}
