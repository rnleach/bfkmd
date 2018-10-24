//! bufcli
//!
//! Generate ad hoc model climatologies from Bufkit soundings and store the intermediate data in the
//! archive. These can be queried later by other tools to provide context to any given analysis.
extern crate bfkmd;
extern crate bufkit_data;
extern crate chrono;
#[macro_use]
extern crate clap;
extern crate dirs;
extern crate failure;
#[macro_use]
extern crate itertools;
extern crate rusqlite;
extern crate sounding_analysis;
extern crate sounding_bufkit;
extern crate strum;

mod builder;
mod climo_db;

use self::builder::build_climo;
use bfkmd::bail;
use bufkit_data::{Archive, Model};
use clap::{App, Arg};
use dirs::home_dir;
use failure::{Error, Fail};
use std::path::PathBuf;
use std::str::FromStr;
use strum::{AsStaticRef, IntoEnumIterator};

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

fn run() -> Result<(), Error> {
    let args = &parse_args()?;

    match args.operation.as_ref() {
        "build" => build(args),
        "show" => show(args),
        "reset" => reset(args),
        _ => bail("Unknown operation."),
    }
}

#[derive(Debug)]
struct CmdLineArgs {
    root: PathBuf,
    sites: Vec<String>,
    models: Vec<Model>,
    save_dir: Option<PathBuf>,
    operation: String,
}

fn parse_args() -> Result<CmdLineArgs, Error> {
    let app = App::new("bufcli")
        .author("Ryan Leach <clumsycodemonkey@gmail.com>")
        .version(crate_version!())
        .about("Model sounding climatology.")
        .arg(
            Arg::with_name("sites")
                .multiple(true)
                .short("s")
                .long("sites")
                .takes_value(true)
                .help("Site identifiers (e.g. kord, katl, smn)."),
        ).arg(
            Arg::with_name("models")
                .multiple(true)
                .short("m")
                .long("models")
                .takes_value(true)
                .possible_values(
                    &Model::iter()
                        .map(|val| val.as_static())
                        .collect::<Vec<&str>>(),
                ).help("Allowable models for this operation/program.")
                .long_help(concat!(
                    "Allowable models for this operation/program.",
                    " Default is to use all possible values."
                )),
        ).arg(
            Arg::with_name("save-dir")
                .long("save-dir")
                .takes_value(true)
                .help("Directory to save .csv files to.")
                .long_help(concat!(
                    "Directory to save .csv files to. If this is specified then a file",
                    " 'site_model.csv' is created for each site and model in that directory with",
                    " data for each graph statistic specified. The exported data is set by the -g",
                    " option."
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
        ).arg(
            Arg::with_name("operation")
                .index(1)
                .takes_value(true)
                .required(true)
                .possible_values(&["build", "show", "reset"])
                .help("Either build or show the climatology. reset deletes the while database.")
                .long_help(concat!(
                    "Either build, show, or reset the climate database. reset deletes the whole",
                    " climate database and starts over fresh. Build will only add data for dates",
                    " not already in the database.",
                )),
        );

    let matches = app.get_matches();

    let root = matches
        .value_of("root")
        .map(PathBuf::from)
        .or_else(|| home_dir().and_then(|hd| Some(hd.join("bufkit"))))
        .expect("Invalid root.");
    let root_clone = root.clone();

    let arch = match Archive::connect(root) {
        arch @ Ok(_) => arch,
        err @ Err(_) => {
            println!("Unable to connect to db, printing error and exiting.");
            err
        }
    }?;

    let mut sites: Vec<String> = matches
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(|arg_val| arg_val.to_owned()))
        .collect();

    if sites.is_empty() {
        sites = arch.get_sites()?.into_iter().map(|site| site.id).collect();
    }

    for site in &sites {
        if !arch.site_exists(site)? {
            println!("Site {} not in the archive, skipping.", site);
        }
    }

    let mut models: Vec<Model> = matches
        .values_of("models")
        .into_iter()
        .flat_map(|model_iter| model_iter.map(Model::from_str))
        .filter_map(|res| res.ok())
        .collect();

    if models.is_empty() {
        models = vec![Model::GFS, Model::NAM, Model::NAM4KM];
    }

    let save_dir: Option<PathBuf> = matches
        .value_of("save-dir")
        .map(str::to_owned)
        .map(PathBuf::from);

    save_dir.as_ref().and_then(|path| {
        if !path.is_dir() {
            bail(&format!("save-dir path {} does not exist.", path.display()));
        } else {
            Some(())
        }
    });

    let operation: String = matches.value_of("operation").map(str::to_owned).unwrap();

    Ok(CmdLineArgs {
        root: root_clone,
        sites,
        models,
        save_dir,
        operation,
    })
}

fn build(args: &CmdLineArgs) -> Result<(), Error> {
    let arch = &Archive::connect(&args.root)?;

    for (site, &model) in iproduct!(&args.sites, &args.models) {
        build_climo(arch, site, model)?;
    }

    Ok(())
}

fn show(args: &CmdLineArgs) -> Result<(), Error> {
    unimplemented!()
}

fn reset(args: &CmdLineArgs) -> Result<(), Error> {
    unimplemented!()
}
