use super::{ReqInfo, StepResult, DEFAULT_DAYS_BACK, HOST_URL};
use crate::missing_url::MissingUrlDb;
use bfkmd::parse_date_string;
use bufkit_data::{Archive, BufkitDataErr, Model, StationNumber};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use clap::ArgMatches;
use crossbeam_channel as channel;
use std::{error::Error, ops::Deref, path::PathBuf, str::FromStr, thread::spawn};
use strum::IntoEnumIterator;

pub fn start_generator_thread<'a>(
    root: PathBuf,
    arg_matches: &'a ArgMatches,
    generator_tx: channel::Sender<StepResult>,
) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(&root)?;

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

    let sites: Vec<String> = arg_matches
        .values_of("sites")
        .map(|site_iter| site_iter.map(|s| s.to_string()).collect())
        .unwrap_or_else(Vec::new);

    spawn(move || {
        let missing_urls = match MissingUrlDb::open_or_create_404_db(&root) {
            Ok(missing_urls) => missing_urls,
            Err(err) => {
                generator_tx
                    .send(StepResult::InitializationError(err.to_string()))
                    .expect("generator_tx send error.");
                return;
            }
        };

        let download_list = match build_download_list(&arch, sites, &models, start, end) {
            Ok(a_vec) => a_vec,
            Err(err) => {
                generator_tx
                    .send(StepResult::InitializationError(err.to_string()))
                    .expect("generator_tx send error.");
                return;
            }
        };

        download_list
            .into_iter()
            // Filter out known bad combinations
            .filter(|(site_id, _stn_num, model, init_time)| {
                !invalid_combination(site_id, *model, *init_time)
            })
            // Filter out data already in the databse
            .filter(|(_site_id, site, model, init_time)| {
                !site
                    .and_then(|s| arch.file_exists(s, *model, *init_time).ok())
                    .unwrap_or(false)
            })
            // Add the url
            .filter_map(|(site_id, site, model, init_time)| {
                match build_url(&site_id, model, &init_time) {
                    Ok(url) => Some((site_id, site, model, init_time, url)),
                    Err(_) => None,
                }
            })
            // Filter out known missing values
            .filter(|(_, _, _, _, url)| !missing_urls.is_missing(url).unwrap_or(false))
            // Limit the number of downloads.
            .take(1_000)
            // Pass it off to another thread for downloading.
            .map(move |(site_id, site, model, init_time, url)| {
                StepResult::Request(ReqInfo {
                    site_id,
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

fn build_download_list(
    arch: &Archive,
    sites: Vec<String>,
    models: &[Model],
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Result<Vec<(String, Option<StationNumber>, Model, NaiveDateTime)>, BufkitDataErr> {
    let mut to_ret: Vec<(String, Option<StationNumber>, Model, NaiveDateTime)> = vec![];
    for &model in models {
        let sites: Vec<(String, Option<StationNumber>)> = if !sites.is_empty() {
            sites
                .iter()
                .map(|s| s.to_lowercase())
                .map(|s| {
                    let stn_num = arch.station_num_for_id(&s, model).ok();
                    (s, stn_num)
                })
                .collect()
        } else {
            list_of_auto_download_sites_for_model(arch, model)?
        };

        for init_time in
            model.all_runs(&end, &(start - Duration::hours(model.hours_between_runs())))
        {
            for (site_id, stn_num) in &sites {
                to_ret.push((site_id.clone(), *stn_num, model, init_time));
            }
        }
    }

    to_ret.sort_by_key(|val| std::cmp::Reverse(val.3));

    Ok(to_ret)
}

fn list_of_auto_download_sites_for_model(
    arch: &Archive,
    model: Model,
) -> Result<Vec<(String, Option<StationNumber>)>, BufkitDataErr> {
    let iter = arch
        .sites()?
        .into_iter()
        .filter(|s| s.auto_download)
        .map(|s| s.station_num);

    let mut to_ret = vec![];
    for stn_num in iter {
        if let Some(site_id) = arch.most_recent_id(stn_num, model)? {
            to_ret.push((site_id.to_lowercase(), Some(stn_num)));
        }
    }

    Ok(to_ret)
}

fn invalid_combination(site: &str, model: Model, init_time: NaiveDateTime) -> bool {
    let site: String = site.to_lowercase();

    let model_site_mismatch = match site.deref() {
        "bam" | "c17" | "lrr" | "s06" | "ssy" | "xkza" | "xxpn" => {
            model == Model::NAM || model == Model::NAM4KM
        }
        "bon" | "hmm" | "mrp" | "smb" | "win" => model == Model::GFS,
        "wntr" => model == Model::GFS || model == Model::NAM4KM,
        "kfca" => model == Model::NAM || model == Model::NAM4KM,
        "paeg" | "pabt" | "pafa" | "pafm" | "pamc" | "pfyu" => model == Model::NAM4KM,
        _ => false, // All other combinations are OK
    };

    let model_init_time_mismatch = match model {
        Model::NAM4KM => init_time < NaiveDate::from_ymd(2013, 3, 25).and_hms(0, 0, 0),
        _ => init_time < NaiveDate::from_ymd(2011, 1, 1).and_hms(0, 0, 0),
    };

    model_site_mismatch || model_init_time_mismatch
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
