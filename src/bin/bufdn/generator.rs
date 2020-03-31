use super::{ReqInfo, StepResult, DEFAULT_DAYS_BACK, HOST_URL};
use crate::missing_url::MissingUrlDb;
use bfkmd::parse_date_string;
use bufkit_data::{Archive, BufkitDataErr, Model};
use chrono::{Datelike, Duration, NaiveDateTime, Timelike, Utc};
use clap::ArgMatches;
use crossbeam_channel as channel;
use itertools::iproduct;
use std::{error::Error, ops::Deref, path::PathBuf, str::FromStr, thread::spawn};
use strum::IntoEnumIterator;

pub fn start_generator_thread<'a>(
    root: PathBuf,
    arg_matches: &'a ArgMatches,
    generator_tx: channel::Sender<StepResult>,
) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(&root)?;

    let sites: Vec<String> = if arg_matches.is_present("sites") {
        arg_matches
            .values_of("sites")
            .into_iter()
            .flat_map(|site_iter| site_iter.map(ToOwned::to_owned))
            .collect()
    } else if arg_matches.is_present("all-sites") {
        arch.sites()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|site| site.id)
            .collect()
    } else {
        arch.sites()
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|s| s.auto_download)
            .map(|site| site.id)
            .collect()
    };

    let models: Vec<Model> = if arg_matches.is_present("models") {
        arg_matches
            .values_of("models")
            .into_iter()
            .flat_map(|model_iter| model_iter.map(Model::from_str))
            .filter_map(Result::ok)
            .collect()
    } else {
        Model::iter().collect()
    };

    let days_back = arg_matches
        .value_of("days-back")
        .and_then(|val| val.parse::<i64>().ok())
        .unwrap_or(DEFAULT_DAYS_BACK);

    let mut end = Utc::now().naive_utc() - Duration::hours(2);
    let mut start = Utc::now().naive_utc() - Duration::days(days_back);

    if let Some(start_date) = arg_matches.value_of("start") {
        start = parse_date_string(start_date);
    }

    if let Some(end_date) = arg_matches.value_of("end") {
        end = parse_date_string(end_date);
    }

    spawn(move || {
        let missing_urls = match MissingUrlDb::open_or_create_404_db(&root) {
            Ok(missing_urls) => missing_urls,
            Err(err) => {
                generator_tx
                    .send(StepResult::IntializationError(err.to_string()))
                    .expect("generator_tx send error.");
                return;
            }
        };

        let iter = match build_download_list(&sites, &models, start, end) {
            Ok(iter) => iter,
            Err(err) => {
                generator_tx
                    .send(StepResult::IntializationError(err.to_string()))
                    .expect("generator_tx send error.");
                return;
            }
        };

        iter
            // Filter out known bad combinations
            .filter(|(site, model, _)| !invalid_combination(site, *model))
            // Filter out data already in the databse
            .filter(|(site, model, init_time)| {
                !arch.file_exists(site, *model, init_time).unwrap_or(false)
            })
            // Add the url
            .filter_map(
                |(site, model, init_time)| match build_url(&site, model, &init_time) {
                    Ok(url) => Some((site, model, init_time, url)),
                    Err(_) => None,
                },
            )
            // Filter out known missing values
            .filter(|(_, _, _, url)| !missing_urls.is_missing(url).unwrap_or(false))
            // Pass it off to another thread for downloading.
            .map(move |(site, model, init_time, url)| {
                StepResult::Request(ReqInfo {
                    site,
                    model,
                    init_time,
                    url,
                })
            })
            .for_each(move |request| {
                if generator_tx.send(request).is_err() {
                    return;
                }
            });
    });

    Ok(())
}

fn build_download_list<'a>(
    sites: &'a [String],
    models: &'a [Model],
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Result<impl Iterator<Item = (String, Model, NaiveDateTime)> + 'a, BufkitDataErr> {
    Ok(iproduct!(sites, models).flat_map(move |(site, model)| {
        model
            .all_runs(&end, &(start - Duration::hours(model.hours_between_runs())))
            .map(move |init_time| (site.to_uppercase(), *model, init_time))
    }))
}

fn invalid_combination(site: &str, model: Model) -> bool {
    let site: String = site.to_lowercase();

    match site.deref() {
        "bam" | "c17" | "lrr" | "s06" | "ssy" | "xkza" | "xxpn" => {
            model == Model::NAM || model == Model::NAM4KM
        }
        "bon" | "hmm" | "mrp" | "smb" | "win" => model == Model::GFS,
        "wntr" => model == Model::GFS || model == Model::NAM4KM,
        "kfca" => model == Model::NAM || model == Model::NAM4KM,
        _ => false, // All other combinations are OK
    }
}

fn build_url(
    site: &str,
    model: Model,
    init_time: &NaiveDateTime,
) -> Result<String, Box<dyn Error>> {
    let site = site.to_lowercase();

    let year = init_time.year();
    let month = init_time.month();
    let day = init_time.day();
    let hour = init_time.hour();
    let remote_model = match (model, hour) {
        (Model::GFS, _) => "gfs3",
        (Model::NAM, 6) | (Model::NAM, 18) => "namm",
        (Model::NAM, _) => "nam",
        (Model::NAM4KM, _) => "nam4km",
    };

    let remote_file_name = remote_model.to_string() + "_" + &site + ".buf";

    Ok(format!(
        "{}{}/{:02}/{:02}/bufkit/{:02}/{}/{}",
        HOST_URL,
        year,
        month,
        day,
        hour,
        model.to_string().to_lowercase(),
        remote_file_name
    ))
}
