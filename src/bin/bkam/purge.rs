use bfkmd::parse_date_string;
use bufkit_data::{Archive, Model, Site};
use chrono::{NaiveDate, Utc};
use clap::ArgMatches;
use std::str::FromStr;
use std::{error::Error, path::PathBuf};
use strum::IntoEnumIterator;

pub fn purge(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(root)?;

    let mut sites: Vec<Site> = sub_args
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(ToOwned::to_owned))
        .filter_map(|site_id| arch.site_for_id(&site_id))
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

    if sites.is_empty() {
        sites = arch.sites()?;
    }

    if models.is_empty() {
        models = Model::iter().collect();
    }

    for site in &sites {
        let available_models = arch.models(site)?;
        for &model in &models {
            if !available_models.contains(&model) {
                continue;
            }

            let all_runs = model.all_runs(&after, &before);

            for run in all_runs {
                println!(
                    "  Removing {} {} {} {}.",
                    site.station_num,
                    site.id.as_deref().unwrap_or(""),
                    model.as_static_str(),
                    run
                );
                if let Ok(()) = arch.remove(site, model, &run) {}
            }
        }
    }

    Ok(())
}
