//! Bufkit Downloader.
//!
//! Downloads Bufkit files and stores them in your archive.
use crate::missing_url::MissingUrlDb;
use bufkit_data::{Model, StationNumber};
use chrono::{Duration, NaiveDateTime, Utc};
use clap::{crate_version, App, Arg, ArgMatches};
use crossbeam_channel as channel;
use dirs::home_dir;
use reqwest::StatusCode;
use std::{error::Error, path::PathBuf};

mod db_writer;
mod download;
mod generator;
mod missing_url;
mod sources;

const DEFAULT_DAYS_BACK: i64 = 2;

fn main() {
    if let Err(e) = run() {
        println!("error: {}", e);

        let mut err = &*e;

        while let Some(cause) = err.source() {
            println!("caused by: {}", cause);
            err = cause;
        }

        ::std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    const CAPACITY: usize = 16;

    let matches = parse_args();

    let root = matches
        .value_of("root")
        .map(PathBuf::from)
        .or_else(|| home_dir().map(|hd| hd.join("bufkit")))
        .expect("Invalid root.");

    let (generator_tx, dl_rx) = channel::bounded::<StepResult>(CAPACITY);
    let (dl_tx, save_rx) = channel::bounded::<StepResult>(CAPACITY);
    let (save_tx, print_rx) = channel::bounded::<StepResult>(CAPACITY);

    generator::start_generator_thread(root.clone(), &matches, generator_tx)?;
    download::start_download_threads(dl_rx, dl_tx);
    db_writer::start_writer_thread(root.clone(), save_rx, save_tx);

    let too_old_to_be_missing = Utc::now().naive_utc() - Duration::hours(27);
    let missing_urls = MissingUrlDb::open_or_create_404_db(&root)?;

    for step_result in print_rx {
        use crate::StepResult::*;

        match step_result {
            URLNotFound(_) | ParseError(_, _) | OtherDownloadError(_, _) | ArchiveError(_, _) => {
                let msg = handle_error_as_missing_data(
                    &step_result,
                    too_old_to_be_missing,
                    &missing_urls,
                )?;
                println!("{}", msg);
            }
            OtherURLStatus(ReqInfo { url, .. }, code) => {
                println!("  HTTP error ({}): {}.", code, url)
            }
            Success(req) => {
                println!(
                    "Success for {:>4} {:^6} {}.",
                    req.site_id, req.model, req.init_time
                );
            }
            InitializationError(msg) => println!("Error initializing threads: {}", msg),
            _ => unreachable!(),
        }
    }

    Ok(())
}

fn handle_error_as_missing_data(
    res: &StepResult,
    too_old_to_be_missing: chrono::NaiveDateTime,
    missing_urls: &MissingUrlDb,
) -> Result<String, Box<dyn Error>> {
    use StepResult::*;

    let (url, init_time, msg) = match res {
        ArchiveError(ReqInfo { init_time, url, .. }, err) => {
            (url, *init_time, format!("  {}", err))
        }
        OtherDownloadError(ReqInfo { init_time, url, .. }, err) => {
            (url, *init_time, format!("  {}", err))
        }
        ParseError(ReqInfo { init_time, url, .. }, msg) => (
            url,
            *init_time,
            format!("Corrupt file at URL ({}): {}", msg, url),
        ),
        URLNotFound(ReqInfo { init_time, url, .. }) => {
            let msg = if *init_time < too_old_to_be_missing {
                format!("URL missing, will not try again: {}", url)
            } else {
                format!("Try again later: {}", url)
            };
            (url, *init_time, msg)
        }
        _ => unreachable!(),
    };

    if init_time < too_old_to_be_missing {
        missing_urls.add_url(url).map_err(|err| err.to_string())?;
    }

    Ok(msg)
}

fn parse_args() -> ArgMatches<'static> {
    App::new("bufdn")
        .author("Ryan <rnleach@users.noreply.github.com>")
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
                    "If not specified, it will look in the index for sites configured for auto ",
                    "download and use all of them. If this is the first time downloading for this ",
                    "site, then it won't be in the index yet and you will need to also specify ",
                    "which models to download for the site."
                )),
        )
        .arg(
            Arg::with_name("models")
                .multiple(true)
                .short("m")
                .long("models")
                .takes_value(true)
                .help("Allowable models for this operation/program.")
                .long_help(concat!(
                    "Allowable models for this operation/program. Case insensitive.",
                    "If not specified, it will use all models available."
                )),
        )
        .arg(
            Arg::with_name("days-back")
                .short("d")
                .long("days-back")
                .takes_value(true)
                .conflicts_with_all(&["start", "end"])
                .help("Number of days back to consider.")
                .long_help(concat!(
                    "The number of days back to consider. Cannot use --start or --end with this."
                )),
        )
        .arg(
            Arg::with_name("start")
                .long("start")
                .takes_value(true)
                .help("The starting model inititialization time. YYYY-MM-DD-HH")
                .long_help(concat!(
                    "The initialization time of the first model run to download.",
                    " Format is YYYY-MM-DD-HH. If the --end argument is not specified",
                    " then the end time is assumed to be now."
                )),
        )
        .arg(
            Arg::with_name("end")
                .long("end")
                .takes_value(true)
                .requires("start")
                .help("The last model inititialization time. YYYY-MM-DD-HH")
                .long_help(concat!(
                    "The initialization time of the last model run to download.",
                    " Format is YYYY-MM-DD-HH. This requires the --start option too."
                )),
        )
        .arg(
            Arg::with_name("root")
                .short("r")
                .long("root")
                .takes_value(true)
                .help("Set the root of the archive.")
                .long_help(
                    "Set the root directory of the archive you are invoking this command for.",
                )
                .conflicts_with("create")
                .global(true),
        )
        .after_help(concat!(
            "To download data for a new site for the first time you must also specify the model."
        ))
        .get_matches()
}

// Result from a single step in the processing chain
#[derive(Debug, Clone)]
pub enum StepResult {
    Request(ReqInfo),
    BufkitFileAsString(ReqInfo, String), // Data, sounding loaded as text data, not parsed
    Success(ReqInfo),
    StationIdMoved {
        info: ReqInfo,
        old: StationNumber,
        new: StationNumber,
    },

    // Errors
    URLNotFound(ReqInfo),
    OtherURLStatus(ReqInfo, StatusCode), // status code returned by reqwest
    OtherDownloadError(ReqInfo, String), // Any other error downloading, error converted to string.
    ParseError(ReqInfo, String),         // An error during parsing
    ArchiveError(ReqInfo, String),       // Error adding it to the archive
    StationMovedError(ReqInfo),          // The station number wasn't what was expected.
    MissingUrlDbError(ReqInfo, String),  // Error dealing with the MissingUrlDb
    InitializationError(String),         // Error setting up threads.
}

#[derive(Debug, Clone)]
pub struct ReqInfo {
    site_id: String,
    site: Option<StationNumber>,
    model: Model,
    init_time: NaiveDateTime,
    url: String,
}
