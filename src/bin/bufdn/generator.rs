use super::sources::{IowaState, Source};
use super::{ReqInfo, StepResult, DEFAULT_DAYS_BACK};
use crate::missing_url::MissingUrlDb;
use bfkmd::{parse_date_string, AutoDownloadListDb};
use bufkit_data::{Archive, BufkitDataErr, Model, StationNumber};
use chrono::{Duration, NaiveDateTime, Utc};
use clap::ArgMatches;
use crossbeam_channel as channel;
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    thread::spawn,
};
use strum::IntoEnumIterator;

pub fn start_generator_thread(
    root: PathBuf,
    arg_matches: &ArgMatches,
    generator_tx: channel::Sender<StepResult>,
) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(&root)?;

    if arg_matches.occurrences_of("local") > 0 {
        let local_dir: &Path = arg_matches.value_of("local").map(Path::new).unwrap();

        let entries: Vec<_> = fs::read_dir(local_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.metadata().ok().map(|p| p.is_file()).unwrap_or(false))
            .map(|e| e.path())
            .filter_map(|e| e.canonicalize().ok())
            .filter(|e| e.extension().map(|ext| ext == "buf").unwrap_or(false))
            .collect();

        spawn(move || {
            entries
                .iter()
                // Get the file name as a string to work with
                .filter_map(|e| {
                    if let Some(e_str) = e.file_name().and_then(|f| f.to_str()) {
                        let e_str = String::from(e_str);
                        Some((e, e_str))
                    } else {
                        None
                    }
                })
                // Parse the model from the file name
                .map(|(e, e_str)| {
                    let mut model: Option<Model> = None;

                    for m in Model::iter() {
                        if e_str.contains(m.as_static_str()) {
                            model = Some(m);
                            // NO BREAK HERE because nam4km will be miscoded as nam
                        }
                    }

                    (e, model, e_str)
                })
                // Parse the site id from the file name
                .map(|(e, model, e_str)| {
                    let site_id = e_str
                        .split('.')
                        .nth(0)
                        .and_then(|s| s.split('_').nth(1))
                        .map(String::from);

                    (e, model, site_id)
                })
                // Build URL from the path
                .map(|(e, model, site_id)| {
                    let url = reqwest::Url::from_file_path(&e);
                    (e, model, url, site_id)
                })
                .map(
                    |(entry, model_opt, url_res, site_id)| match (site_id, model_opt, url_res) {
                        (None, _, _) => StepResult::FileNameParseError(format!(
                            "Error parsing site id from file name: {}",
                            entry.display()
                        )),
                        (_, None, _) => StepResult::FileNameParseError(format!(
                            "Error parsing model from file name: {}",
                            entry.display()
                        )),
                        (_, _, Err(e)) => StepResult::FileNameParseError(format!(
                            "Error creating file url from file name: {}, {:?}",
                            entry.display(),
                            e
                        )),
                        (Some(site_id), Some(model), Ok(url)) => {
                            let req = ReqInfo {
                                site_id,
                                site: None,
                                model,
                                init_time: None,
                                url: url.to_string(),
                            };
                            StepResult::Local(req)
                        }
                    },
                )
                .for_each(move |request| {
                    if generator_tx.send(request).is_err() {
                        return;
                    }
                });
        });
    } else {
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

            let sources: &[&dyn Source] = &[&IowaState {}];

            download_list
                .into_iter()
                // Filter out data already in the databse
                .filter(|(_site_id, stn_num, model, init_time)| {
                    !stn_num
                        .and_then(|s| arch.file_exists(s, *model, *init_time).ok())
                        .unwrap_or(false)
                })
                // Make a request
                .filter_map(move |(site_id, site, model, init_time)| {
                    sources
                        .iter()
                        .filter_map(|src| src.build_req_info(site_id.clone(), site, model, init_time))
                        .find(|ReqInfo { ref url, .. }| !missing_urls.is_missing(url).unwrap_or(false))
                })
                // Limit the number of downloads.
                .take(1_500)
                // Pass it off to another thread for downloading.
                .map(StepResult::Request)
                .for_each(move |request| {
                    if generator_tx.send(request).is_err() {
                        return;
                    }
                });
        });
    }

    Ok(())
}

fn build_download_list(
    arch: &Archive,
    sites: Vec<String>,
    models: &[Model],
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Result<Vec<(String, Option<StationNumber>, Model, NaiveDateTime)>, BufkitDataErr> {
    use std::time::Instant;

    let start_long_request = Instant::now();
    let site_model: Vec<(String, Option<StationNumber>, Model)> = if !sites.is_empty() {
        println!("Using provided sites...");
        models
            .iter()
            .flat_map(|&model| {
                sites.iter().map(|s| s.to_lowercase()).map(move |s| {
                    let stn_num = arch.station_num_for_id(&s, model).ok();
                    (s, stn_num, model)
                })
            })
            .collect()
    } else {
        println!("Make download list of sites...");
        list_of_auto_download(arch)?
    };
    let duration = start_long_request.elapsed();
    println!("....done! with sites it took: {:?}", duration);

    let mut to_ret: Vec<(String, Option<StationNumber>, Model, NaiveDateTime)> = site_model
        .iter()
        .flat_map(|(id, stn, model)| {
            model
                .all_runs(
                    &end,
                    &(start - chrono::Duration::hours(model.hours_between_runs())),
                )
                .map(move |vt| (id.clone(), *stn, *model, vt))
        })
        .collect();

    to_ret.sort_by_key(|val| std::cmp::Reverse(val.3));

    Ok(to_ret)
}

fn list_of_auto_download(
    arch: &Archive,
) -> Result<Vec<(String, Option<StationNumber>, Model)>, BufkitDataErr> {
    let dl_db = AutoDownloadListDb::open_or_create(&arch.root())?;
    let mut dl_stations = dl_db.get_list()?;
    dl_stations.sort_unstable();

    let mut dl_id_stations: Vec<(String, Option<StationNumber>, Model)> = Vec::new();

    for model in Model::iter() {
        arch.sites_and_ids_for(model)?
            .into_iter()
            .filter(|(site, _id)| dl_stations.binary_search(&site.station_num).is_ok())
            .for_each(|(site, id)| {
                dl_id_stations.push((id, Some(site.station_num), model));
            })
    }

    Ok(dl_id_stations)
}
