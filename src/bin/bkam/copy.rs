use bfkmd::parse_date_string;
use bufkit_data::{Archive, Model, StationNumber};
use chrono::{NaiveDate, Utc};
use clap::ArgMatches;
use std::str::FromStr;
use std::{error::Error, path::PathBuf};
use strum::IntoEnumIterator;

pub fn copy(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(root)?;

    let models = {
        let mut models: Vec<Model> = sub_args
            .values_of("models")
            .into_iter()
            .flat_map(|model_iter| model_iter.map(Model::from_str))
            .filter_map(Result::ok)
            .collect();

        if models.is_empty() {
            models = Model::iter().collect();
        }

        models
    };

    let start = sub_args
        .value_of("start")
        .map(|after_str| parse_date_string(after_str))
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1900, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap());

    let end = sub_args
        .value_of("end")
        .map(|before_str| parse_date_string(before_str))
        .unwrap_or_else(|| Utc::now().naive_utc());

    let sites: Vec<String> = sub_args
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(ToOwned::to_owned))
        .collect();

    let mut stations: Vec<StationNumber> = vec![];
    for &model in &models {
        if sites.is_empty() {
            arch.sites()?
                .into_iter()
                .map(|info| info.station_num)
                .filter(|&stn_num| {
                    arch.models(stn_num)
                        .map(|mdls| mdls.contains(&model))
                        .unwrap_or(false)
                })
                .for_each(|stn_num| stations.push(stn_num));
        } else {
            sites
                .iter()
                .filter_map(|id| arch.station_num_for_id(id, model).ok())
                .for_each(|stn_num| stations.push(stn_num));
        };
    }
    stations.sort();
    stations.dedup();
    let stations = stations;

    let dest_path = PathBuf::from(sub_args.value_of("dest").unwrap());

    arch.export(&stations, &models, start, end, &dest_path)?;

    Ok(())
}
