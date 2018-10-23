//! Bufkit Downloader.
//!
//! Downloads Bufkit files and stores them in your archive.

extern crate bfkmd;
extern crate bufkit_data;
extern crate chrono;
#[macro_use]
extern crate clap;
extern crate crossbeam_channel;
extern crate dirs;
#[macro_use]
extern crate itertools;
#[macro_use]
extern crate failure;
extern crate reqwest;
extern crate rusqlite;
extern crate strum;

use bfkmd::parse_date_string;
use bufkit_data::{Archive, BufkitDataErr, Model};
use chrono::{Datelike, Duration, NaiveDateTime, Timelike, Utc};
use clap::{App, Arg, ArgMatches};
use crossbeam_channel as channel;
use dirs::home_dir;
use failure::{Error, Fail};
use reqwest::{Client, StatusCode};
use rusqlite::{Connection, OpenFlags, NO_PARAMS};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread::{spawn, JoinHandle};
use strum::IntoEnumIterator;

static HOST_URL: &str = "http://mtarchive.geol.iastate.edu/";
const DEFAULT_DAYS_BACK: i64 = 2;

fn main() {
    if let Err(ref e) = run() {
        println!("error: {}", e);

        let mut fail: &Fail = e.as_fail();

        while let Some(cause) = fail.cause() {
            println!("caused by: {}", cause);

            if let Some(backtrace) = cause.backtrace() {
                println!("backtrace: {}\n\n\n", backtrace);
            }

            fail = cause;
        }

        ::std::process::exit(1);
    }
}

// Result from a single step in the processing chain
enum StepResult {
    BufkitFileAsString(String),         // Data
    URLNotFound(String),                // URL
    OtherURLStatus(String, StatusCode), // URL, status code
    OtherDownloadError(Error),          // Any other error downloading
    ArhciveError(Error),                // Error adding it to the archive
    Success,                            // File added to the archive
}

fn run() -> Result<(), Error> {
    let app = App::new("bufdn")
        .author("Ryan Leach <clumsycodemonkey@gmail.com>")
        .version(crate_version!())
        .about("Download data into your archive.")
        .arg(
            Arg::with_name("sites")
                .multiple(true)
                .short("s")
                .long("sites")
                .takes_value(true)
                .help("Site identifiers (e.g. kord, katl, smn).")
                .long_help(concat!(
                    "Site identifiers (e.g. kord, katl, smn). ",
                    "If not specified, it will look in the database for sites configured for auto ",
                    "download and use all of them."
                )),
        ).arg(
            Arg::with_name("models")
                .multiple(true)
                .short("m")
                .long("models")
                .takes_value(true)
                .help("Allowable models for this operation/program.")
                .long_help("Allowable models for this operation/program. Case insensitive."),
        ).arg(
            Arg::with_name("days-back")
                .short("d")
                .long("days-back")
                .takes_value(true)
                .conflicts_with_all(&["start", "end"])
                .help("Number of days back to consider.")
                .long_help(concat!(
                    "The number of days back to consider. Cannot use --start or --end with this."
                )),
        ).arg(
            Arg::with_name("start")
                .long("start")
                .takes_value(true)
                .help("The starting model inititialization time. YYYY-MM-DD-HH")
                .long_help(concat!(
                    "The initialization time of the first model run to download.",
                    " Format is YYYY-MM-DD-HH. If the --end argument is not specified",
                    " then the end time is assumed to be now."
                )),
        ).arg(
            Arg::with_name("end")
                .long("end")
                .takes_value(true)
                .requires("start")
                .help("The last model inititialization time. YYYY-MM-DD-HH")
                .long_help(concat!(
                    "The initialization time of the last model run to download.",
                    " Format is YYYY-MM-DD-HH. This requires the --start option too."
                )),
        ).arg(
            Arg::with_name("root")
                .short("r")
                .long("root")
                .takes_value(true)
                .help("Set the root of the archive.")
                .long_help(
                    "Set the root directory of the archive you are invoking this command for.",
                ).conflicts_with("create")
                .global(true),
        );

    let matches = app.get_matches();

    let root = matches
        .value_of("root")
        .map(PathBuf::from)
        .or_else(|| home_dir().and_then(|hd| Some(hd.join("bufkit"))))
        .expect("Invalid root.");
    let root_clone = root.clone();
    let root_clone2 = root.clone();

    let arch = Archive::connect(&root)?;

    let main_tx: channel::Sender<(String, Model, NaiveDateTime, String)>;
    let dl_rx: channel::Receiver<(String, Model, NaiveDateTime, String)>;
    let tx_rx = channel::bounded(10);
    main_tx = tx_rx.0;
    dl_rx = tx_rx.1;

    let dl_tx: channel::Sender<(String, Model, NaiveDateTime, StepResult)>;
    let save_rx: channel::Receiver<(String, Model, NaiveDateTime, StepResult)>;
    let tx_rx = channel::bounded(10);
    dl_tx = tx_rx.0;
    save_rx = tx_rx.1;

    let save_tx: channel::Sender<(String, Model, NaiveDateTime, StepResult)>;
    let print_rx: channel::Receiver<(String, Model, NaiveDateTime, StepResult)>;
    let tx_rx = channel::bounded(10);
    save_tx = tx_rx.0;
    print_rx = tx_rx.1;

    // The file download thread
    let download_handle: JoinHandle<Result<(), Error>> = spawn(move || {
        let client = Client::new();

        for vals in dl_rx {
            let (site, model, init_time, url) = vals;

            let download_result = match client.get(&url).send() {
                Ok(ref mut response) => match response.status() {
                    StatusCode::OK => {
                        let mut buffer = String::new();
                        match response.read_to_string(&mut buffer) {
                            Ok(_) => StepResult::BufkitFileAsString(buffer),
                            Err(err) => StepResult::OtherDownloadError(err.into()),
                        }
                    }
                    StatusCode::NOT_FOUND => StepResult::URLNotFound(url),
                    code => StepResult::OtherURLStatus(url, code),
                },
                Err(err) => StepResult::OtherDownloadError(err.into()),
            };

            dl_tx.send((site, model, init_time, download_result));
        }

        Ok(())
    });

    // The db writer thread
    let writer_handle: JoinHandle<Result<(), Error>> = spawn(move || {
        let arch = Archive::connect(root_clone)?;

        for (site, model, init_time, download_res) in save_rx {
            let save_res = match download_res {
                StepResult::BufkitFileAsString(data) => {
                    match arch.add_file(&site, model, &init_time, &data) {
                        Ok(_) => StepResult::Success,
                        Err(err) => StepResult::ArhciveError(err.into()),
                    }
                }
                _ => download_res,
            };

            save_tx.send((site, model, init_time, save_res));
        }

        Ok(())
    });

    // The finalize thread
    let finalize_handle: JoinHandle<Result<(), Error>> = spawn(move || {
        let stdout = ::std::io::stdout();
        let mut lock = stdout.lock();

        let too_old_to_be_missing = Utc::now().naive_utc() - Duration::hours(27);

        let missing_urls = MissingUrlDb::open_or_create_404_db(&root_clone2)?;

        for (site, model, init_time, save_res) in print_rx {
            use StepResult::*;

            match save_res {
                URLNotFound(ref url) => {
                    if init_time < too_old_to_be_missing {
                        missing_urls.add_url(url)?;
                    }
                    writeln!(lock, "URL does not exist: {}", url)?
                }
                OtherURLStatus(url, code) => writeln!(lock, "  HTTP error ({}): {}.", code, url)?,
                OtherDownloadError(err) | ArhciveError(err) => {
                    writeln!(lock, "  {}", err)?;

                    let mut fail: &Fail = err.as_fail();

                    while let Some(cause) = fail.cause() {
                        writeln!(lock, "  caused by: {}", cause)?;
                        fail = cause;
                    }
                }
                Success => writeln!(lock, "Success for {:>4} {:^6} {}.", site, model, init_time)?,
                _ => {}
            }
        }

        Ok(())
    });

    let missing_urls = MissingUrlDb::open_or_create_404_db(&root)?;

    // Start processing
    build_download_list(&matches, &arch)?
        // Filter out known bad combinations
        .filter(|(site, model, _)| !invalid_combination(site, *model))
        // Filter out data already in the databse
        .filter(|(site, model, init_time)| !arch.exists(site, *model, init_time).unwrap_or(false))
        // Add the url
        .filter_map(|(site, model, init_time)| {
            match build_url(&site, model, &init_time){
                Ok(url) => Some((site, model, init_time, url)),
                Err(_) => None,
            }
        })
        // Filter out known missing values
        .filter(|(_, _, _, url)|{
            !missing_urls.is_missing(url).unwrap_or(false)
        })
        // Pass it off to another thread for downloading.
        .for_each(move |list_val: (String, Model, NaiveDateTime, String)|{
            main_tx.send(list_val);
        });

    download_handle.join().unwrap()?;
    writer_handle.join().unwrap()?;
    finalize_handle.join().unwrap()?;

    Ok(())
}

fn build_download_list<'a>(
    arg_matches: &'a ArgMatches,
    arch: &'a Archive,
) -> Result<impl Iterator<Item = (String, Model, NaiveDateTime)> + 'a, BufkitDataErr> {
    let mut sites: Vec<String> = arg_matches
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(|arg_val| arg_val.to_owned()))
        .collect();

    let mut models: Vec<Model> = arg_matches
        .values_of("models")
        .into_iter()
        .flat_map(|model_iter| model_iter.map(Model::from_str))
        .filter_map(|res| res.ok())
        .collect();

    let days_back = arg_matches
        .value_of("days-back")
        .and_then(|val| val.parse::<i64>().ok())
        .unwrap_or(DEFAULT_DAYS_BACK);

    let mut auto_sites = false;
    let mut auto_models = false;

    if sites.is_empty() {
        sites = arch
            .get_sites()?
            .into_iter()
            .filter(|s| s.auto_download)
            .map(|site| site.id)
            .collect();
        auto_sites = true;
    }

    if models.is_empty() {
        models = Model::iter().collect();
        auto_models = true;
    }

    let mut end = Utc::now().naive_utc() - Duration::hours(2);
    let mut start = Utc::now().naive_utc() - Duration::days(days_back);

    if let Some(start_date) = arg_matches.value_of("start") {
        start = parse_date_string(start_date);
    }

    if let Some(end_date) = arg_matches.value_of("end") {
        end = parse_date_string(end_date);
    }

    Ok(iproduct!(sites, models)
        .filter(move |(site, model)| {
            if auto_models || auto_sites {
                arch.models_for_site(site)
                    .map(|vec| vec.contains(model))
                    .unwrap_or(false)
            } else {
                true
            }
        }).flat_map(move |(site, model)| {
            model
                .all_runs(&(start - Duration::hours(model.hours_between_runs())), &end)
                .map(move |init_time| (site.to_uppercase(), model, init_time))
        }))
}

fn invalid_combination(site: &str, model: Model) -> bool {
    match site {
        "lrr" | "c17" | "s06" => model == Model::NAM || model == Model::NAM4KM,
        "mrp" | "hmm" | "bon" => model == Model::GFS,
        "kfca" => model == Model::NAM || model == Model::NAM4KM,
        _ => false, // All other combinations are OK
    }
}

fn build_url(site: &str, model: Model, init_time: &NaiveDateTime) -> Result<String, Error> {
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
        _ => return Err(format_err!("Invalid model for download")),
    };

    let remote_site = translate_sites(&site, model);

    let remote_file_name = remote_model.to_string() + "_" + remote_site + ".buf";

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

fn translate_sites(site: &str, model: Model) -> &str {
    match (site, model) {
        ("kgpi", Model::GFS) => "kfca",
        _ => site,
    }
}

struct MissingUrlDb {
    db_conn: Connection,
}

impl MissingUrlDb {
    fn open_or_create_404_db(root: &Path) -> Result<Self, BufkitDataErr> {
        let db_file = &root.join("404.db");

        let db404 = Connection::open_with_flags(
            db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;

        db404.execute(
            "CREATE TABLE IF NOT EXISTS missing (
                url TEXT PRIMARY KEY
            )",
            NO_PARAMS,
        )?;

        Ok(MissingUrlDb { db_conn: db404 })
    }

    fn is_missing(&self, url: &String) -> Result<bool, BufkitDataErr> {
        let num_missing: i32 = self.db_conn.query_row(
            "SELECT COUNT(*) FROM missing WHERE url = ?1",
            &[url],
            |row| row.get(0),
        )?;

        Ok(num_missing > 0)
    }

    fn add_url(&self, url: &String) -> Result<(), BufkitDataErr> {
        self.db_conn
            .execute("INSERT INTO missing (url) VALUES (?1)", &[url])?;

        Ok(())
    }
}
