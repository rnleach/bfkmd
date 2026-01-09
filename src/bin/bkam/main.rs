//! BufKit Archive Manager
use clap::{Arg, Command, crate_version};
use dirs::home_dir;
use std::{error::Error, path::PathBuf};

mod copy;
mod create;
mod export;
mod fix;
mod import;
mod purge;
mod sites;

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
    let app = Command::new("bkam")
        .author("Ryan <rnleach@users.noreply.github.com>")
        .version(crate_version!())
        .about("Manage a Bufkit file archive.")
        .arg(
            Arg::new("root")
                .short('r')
                .long("root")
                .takes_value(true)
                .help("Set the root of the archive.")
                .long_help("Set the root directory of the archive you are invoking this command for.")
                .global(true),
        ).subcommand(
            Command::new("create")
                .about("Create a new archive.")
                .arg(
                    Arg::new("force")
                        .long("force")
                        .help("Overwrite any existing archive at `root`."),
                ).arg(
                    Arg::new("archive_root")
                        .index(1)
                        .help("The path to create this archive at."),
                ).after_help("The -r, --root option is ignored with this command."),
        ).subcommand(
            Command::new("sites")
                .about("View and manipulate site data.")
                .subcommand(
                    Command::new("list")
                        .about("List sites in the data base.")
                        .arg(
                            Arg::new("missing-data")
                                .short('m')
                                .long("missing-data")
                                .help("Sites with any missing info."),
                        ).arg(
                            Arg::new("missing-state")
                                .long("missing-state")
                                .help("Only sites missing state/providence."),
                        ).arg(
                            Arg::new("state")
                                .long("state")
                                .help("Only sites in the given state.")
                                .takes_value(true),
                        ).arg(
                            Arg::new("auto-download")
                                .long("auto-download")
                                .short('a')
                                .help(
                                    "Only list sites that are automatically downloaded by bufdn.",
                                ),
                        ).arg(
                            Arg::new("no-auto-download")
                                .long("no-auto-download")
                                .short('n')
                                .help(
                                    "Only list sites that are automatically downloaded by bufdn.",
                                ),
                        ).arg(
                            Arg::new("latitude")
                                .long("latitude")
                                .help("Only list sites near this location.")
                                .takes_value(true)
                                .requires("longitude"),
                        ).arg(
                            Arg::new("longitude")
                                .long("longitude")
                                .help("Only list sites near this location.")
                                .takes_value(true)
                                .requires("latitude"),
                        ),
                ).subcommand(
                    Command::new("modify")
                        .about("Modify the entry for a site.")
                        .arg(
                            Arg::new("stn")
                                .index(1)
                                .required(true)
                                .takes_value(true)
                                .help("The station number or identifier of the site to modify."),
                        ).arg(
                            Arg::new("state")
                                .long("state")
                                .takes_value(true)
                                .help("Set the state field to this value."),
                        ).arg(
                            Arg::new("name")
                                .long("name")
                                .takes_value(true)
                                .help("Set the name field to this value."),
                        ).arg(
                            Arg::new("notes")
                                .long("notes")
                                .takes_value(true)
                                .help("Set the name field to this value."),
                        ).arg(
                            Arg::new("auto-download")
                                .long("auto-download")
                                .help("Set whether or not to automatically download this site.")
                                .possible_values(&["Yes", "yes", "no", "No"])
                                .takes_value(true),
                        ).arg(
                            Arg::new("utc-offset")
                                .long("utc-offset")
                                .help("Set the UTC offset in hours. e.g. '--utc-offset -7' for MST.")
                                .require_equals(true)
                                .takes_value(true),
                        ),
                ).subcommand(
                    Command::new("inv")
                        .about("Get the inventory of soundings for a site.")
                        .arg(
                            Arg::new("stn")
                                .index(1)
                                .required(true)
                                .takes_value(true)
                                .help("The station number or identifier of the site to get the inventory for."),
                        )
                        .arg(
                            Arg::new("model")
                                .index(2)
                                .required(true)
                                .takes_value(true)
                                .help("The model to get the inventory for, e.g. gfs or nam or nam4km"),
                        ),
                ),
        ).subcommand(
            Command::new("export")
                .about("Export a sounding from the database")
                .arg(
                    Arg::new("start")
                        .long("start")
                        .takes_value(true)
                        .help("The starting model inititialization time. YYYY-MM-DD-HH")
                        .long_help(concat!(
                            "The initialization time of the first model run to export.",
                            " Format is YYYY-MM-DD-HH. If the --end argument is not specified",
                            " then the end time is assumed to be the last available run in the",
                            " archive."
                        )),
                ).arg(
                    Arg::new("end")
                        .long("end")
                        .takes_value(true)
                        .requires("start")
                        .help("The last model inititialization time. YYYY-MM-DD-HH")
                        .long_help(concat!(
                            "The initialization time of the last model run to export.",
                            " Format is YYYY-MM-DD-HH."
                        )),
                ).arg(
                    Arg::new("no-prefix-date")
                        .long("no-prefix-date")
                        .takes_value(false)
                        .help("Do not prefix the date in YYYYMMDDHHZ format to the file name.")
                ).arg(
                    Arg::new("site")
                        .index(1)
                        .required(true)
                        .help("The site to export data for."),
                ).arg(
                    Arg::new("model")
                        .index(2)
                        .required(true)
                        .help("The model to export data for, e.g. gfs, GFS, NAM4KM, nam."),
                ).arg(
                    Arg::new("target")
                        .index(3)
                        .required(true)
                        .help("Target directory to save the export file into."),
                ),
        ).subcommand(
            Command::new("import")
                .about("Import a text file into the database.")
                .arg(
                    Arg::new("site")
                        .index(1)
                        .required(true)
                        .help("The site to export data for."),
                ).arg(
                    Arg::new("model")
                        .index(2)
                        .required(true)
                        .help("The model to export data for, e.g. gfs, GFS, NAM4KM, nam."),
                ).arg(
                    Arg::new("file")
                        .index(3)
                        .required(true)
                        .multiple_values(true)
                        .help("Source file to import."),
                ),
        ).subcommand(
            Command::new("purge")
                .about("Delete some data from the archive.")
                .arg(
                    Arg::new("sites")
                        .short('s')
                        .long("sites")
                        .takes_value(true)
                        .multiple_values(true)
                        .help("The site(s) to purge data for.")
                        .long_help(concat!(
                            "Purge data for these sites. This can be combined  with 'model' and",
                            " 'before' or 'after' arguments to narrow the specification.")),
                ).arg(
                    Arg::new("models")
                        .short('m')
                        .long("models")
                        .takes_value(true)
                        .multiple_values(true)
                        .help("The model(s) to purge data for, e.g. gfs, GFS, NAM4KM, nam.")
                        .long_help(concat!(
                            "Purge data for these models only. This can be combined with 'site' and",
                            " 'before' or 'after' arguments to narrow the specification.")),
                ).arg(
                    Arg::new("before")
                        .long("before")
                        .takes_value(true)
                        .help("Purge data before this time. YYYY-MM-DD-HH")
                        .long_help(concat!(
                            "Purge all data before this date. If this AND 'after' are not",
                            " specified, then data for all times is purged. This can be combined",
                            " with 'model' and 'site' arguments.")),
                ).arg(
                    Arg::new("after")
                        .long("after")
                        .takes_value(true)
                        .conflicts_with("before")
                        .help("Purge data after this time. YYYY-MM-DD-HH")
                        .long_help(concat!(
                            "Purge all data after this date. If this AND 'before' are not",
                            " specified, then data for all times is purged. This can be combined",
                            " with 'model' and 'site' arguments.")),
                )
        ).subcommand(
            Command::new("copy")
                .about("Copy some of the archive to a new archive.")
                .arg(
                    Arg::new("sites")
                        .short('s')
                        .long("sites")
                        .required(true)
                        .takes_value(true)
                        .multiple_values(true)
                        .help("The site(s) to copy.")
                        .long_help(concat!(
                            "Copy data for these sites. This must be combined  with 'model' and",
                            " 'before' or 'after' arguments to narrow the specification.")),
                ).arg(
                    Arg::new("models")
                        .short('m')
                        .long("models")
                        .takes_value(true)
                        .required(true)
                        .multiple_values(true)
                        .help("The model(s) to copy data for, e.g. gfs, GFS, NAM4KM, nam.")
                        .long_help(concat!(
                            "Copy data for these models only. This must be combined with 'site' and",
                            " 'before' or 'after' arguments to narrow the specification.")),
                ).arg(
                    Arg::new("start")
                        .long("start")
                        .takes_value(true)
                        .help("Copy data after (and including) this time. YYYY-MM-DD-HH")
                        .long_help("Copy all data after (and including) this date.")
                ).arg(
                    Arg::new("end")
                        .long("end")
                        .takes_value(true)
                        .help("Copy data up to and including this time. YYYY-MM-DD-HH")
                        .long_help("Copy all data up to (and including) this date."),
                ).arg(
                    Arg::new("dest")
                        .long("destination")
                        .short('d')
                        .index(1)
                        .required(true)
                        .takes_value(true)
                        .help("Destination directory of copy.")
                )
        ).subcommand(
            Command::new("fix")
                .about("Find and fix inconsistencies in the archive.")
        );

    let matches = app.get_matches();

    let root = &matches
        .value_of("root")
        .map(PathBuf::from)
        .or_else(|| home_dir().map(|hd| hd.join("bufkit")))
        .expect("Invalid root.");

    match matches.subcommand() {
        Some(("create", sub_args)) => create::create(root, sub_args)?,
        Some(("sites", sub_args)) => sites::sites(root, sub_args)?,
        Some(("export", sub_args)) => export::export(root, sub_args)?,
        Some(("import", sub_args)) => import::import(root, sub_args)?,
        Some(("purge", sub_args)) => purge::purge(root, sub_args)?,
        Some(("fix", sub_args)) => fix::fix(root, sub_args)?,
        Some(("copy", sub_args)) => copy::copy(root, sub_args)?,
        _ => unreachable!(),
    }

    Ok(())
}
