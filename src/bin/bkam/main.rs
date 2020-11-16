//! BufKit Archive Manager
use clap::{crate_version, App, Arg, SubCommand};
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
    let app = App::new("bkam")
        .author("Ryan <rnleach@users.noreply.github.com>")
        .version(crate_version!())
        .about("Manage a Bufkit file archive.")
        .arg(
            Arg::with_name("root")
                .short("r")
                .long("root")
                .takes_value(true)
                .help("Set the root of the archive.")
                .long_help(
                    "Set the root directory of the archive you are invoking this command for.",
                ).conflicts_with("create")
                .global(true),
        ).subcommand(
            SubCommand::with_name("create")
                .about("Create a new archive.")
                .arg(
                    Arg::with_name("force")
                        .long("force")
                        .help("Overwrite any existing archive at `root`."),
                ).arg(
                    Arg::with_name("archive_root")
                        .index(1)
                        .help("The path to create this archive at."),
                ).after_help("The -r, --root option is ignored with this command."),
        ).subcommand(
            SubCommand::with_name("sites")
                .about("View and manipulate site data.")
                .subcommand(
                    SubCommand::with_name("list")
                        .about("List sites in the data base.")
                        .arg(
                            Arg::with_name("missing-data")
                                .short("m")
                                .long("missing-data")
                                .help("Sites with any missing info."),
                        ).arg(
                            Arg::with_name("missing-state")
                                .long("missing-state")
                                .help("Only sites missing state/providence."),
                        ).arg(
                            Arg::with_name("state")
                                .long("state")
                                .help("Only sites in the given state.")
                                .takes_value(true),
                        ).arg(
                            Arg::with_name("auto-download")
                                .long("auto-download")
                                .short("a")
                                .help(
                                    "Only list sites that are automatically downloaded by bufdn.",
                                ),
                        ).arg(
                            Arg::with_name("no-auto-download")
                                .long("no-auto-download")
                                .short("n")
                                .help(
                                    "Only list sites that are automatically downloaded by bufdn.",
                                ),
                        ),
                ).subcommand(
                    SubCommand::with_name("modify")
                        .about("Modify the entry for a site.")
                        .arg(
                            Arg::with_name("stn")
                                .index(1)
                                .required(true)
                                .takes_value(true)
                                .help("The station number or identifier of the site to modify."),
                        ).arg(
                            Arg::with_name("state")
                                .long("state")
                                .takes_value(true)
                                .help("Set the state field to this value."),
                        ).arg(
                            Arg::with_name("name")
                                .long("name")
                                .takes_value(true)
                                .help("Set the name field to this value."),
                        ).arg(
                            Arg::with_name("notes")
                                .long("notes")
                                .takes_value(true)
                                .help("Set the name field to this value."),
                        ).arg(
                            Arg::with_name("auto-download")
                                .long("auto-download")
                                .help("Set whether or not to automatically download this site.")
                                .possible_values(&["Yes", "yes", "no", "No"])
                                .takes_value(true),
                        ).arg(
                            Arg::with_name("utc-offset")
                                .long("utc-offset")
                                .help("Set the UTC offset in hours. e.g. '--utc-offset -7' for MST.")
                                .require_equals(true)
                                .takes_value(true),
                        ),
                ).subcommand(
                    SubCommand::with_name("inv")
                        .about("Get the inventory of soundings for a site.")
                        .arg(
                            Arg::with_name("stn")
                                .index(1)
                                .required(true)
                                .takes_value(true)
                                .help("The station number or identifier of the site to get the inventory for."),
                        )
                        .arg(
                            Arg::with_name("model")
                                .index(2)
                                .required(true)
                                .takes_value(true)
                                .help("The model to get the inventory for, e.g. gfs or nam or nam4km"),
                        ),
                ),
        ).subcommand(
            SubCommand::with_name("export")
                .about("Export a sounding from the database")
                .arg(
                    Arg::with_name("start")
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
                    Arg::with_name("end")
                        .long("end")
                        .takes_value(true)
                        .requires("start")
                        .help("The last model inititialization time. YYYY-MM-DD-HH")
                        .long_help(concat!(
                            "The initialization time of the last model run to export.",
                            " Format is YYYY-MM-DD-HH."
                        )),
                ).arg(
                    Arg::with_name("no-prefix-date")
                        .long("no-prefix-date")
                        .takes_value(false)
                        .help("Do not prefix the date in YYYYMMDDHHZ format to the file name.")
                ).arg(
                    Arg::with_name("site")
                        .index(1)
                        .required(true)
                        .help("The site to export data for."),
                ).arg(
                    Arg::with_name("model")
                        .index(2)
                        .required(true)
                        .help("The model to export data for, e.g. gfs, GFS, NAM4KM, nam."),
                ).arg(
                    Arg::with_name("target")
                        .index(3)
                        .required(true)
                        .help("Target directory to save the export file into."),
                ),
        ).subcommand(
            SubCommand::with_name("import")
                .about("Import a text file into the database.")
                .arg(
                    Arg::with_name("site")
                        .index(1)
                        .required(true)
                        .help("The site to export data for."),
                ).arg(
                    Arg::with_name("model")
                        .index(2)
                        .required(true)
                        .help("The model to export data for, e.g. gfs, GFS, NAM4KM, nam."),
                ).arg(
                    Arg::with_name("file")
                        .index(3)
                        .required(true)
                        .multiple(true)
                        .help("Source file to import."),
                ),
        ).subcommand(
            SubCommand::with_name("purge")
                .about("Delete some data from the archive.")
                .arg(
                    Arg::with_name("sites")
                        .short("s")
                        .long("sites")
                        .takes_value(true)
                        .multiple(true)
                        .help("The site(s) to purge data for.")
                        .long_help(concat!(
                            "Purge data for these sites. This can be combined  with 'model' and",
                            " 'before' or 'after' arguments to narrow the specification.")),
                ).arg(
                    Arg::with_name("models")
                        .short("m")
                        .long("models")
                        .takes_value(true)
                        .multiple(true)
                        .help("The model(s) to purge data for, e.g. gfs, GFS, NAM4KM, nam.")
                        .long_help(concat!(
                            "Purge data for these models only. This can be combined with 'site' and",
                            " 'before' or 'after' arguments to narrow the specification.")),
                ).arg(
                    Arg::with_name("before")
                        .long("before")
                        .takes_value(true)
                        .help("Purge data before this time. YYYY-MM-DD-HH")
                        .long_help(concat!(
                            "Purge all data before this date. If this AND 'after' are not",
                            " specified, then data for all times is purged. This can be combined",
                            " with 'model' and 'site' arguments.")),
                ).arg(
                    Arg::with_name("after")
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
            SubCommand::with_name("copy")
                .about("Copy some of the archive to a new archive.")
                .arg(
                    Arg::with_name("sites")
                        .short("s")
                        .long("sites")
                        .required(true)
                        .takes_value(true)
                        .multiple(true)
                        .help("The site(s) to copy.")
                        .long_help(concat!(
                            "Copy data for these sites. This must be combined  with 'model' and",
                            " 'before' or 'after' arguments to narrow the specification.")),
                ).arg(
                    Arg::with_name("models")
                        .short("m")
                        .long("models")
                        .takes_value(true)
                        .required(true)
                        .multiple(true)
                        .help("The model(s) to copy data for, e.g. gfs, GFS, NAM4KM, nam.")
                        .long_help(concat!(
                            "Copy data for these models only. This must be combined with 'site' and",
                            " 'before' or 'after' arguments to narrow the specification.")),
                ).arg(
                    Arg::with_name("start")
                        .long("start")
                        .takes_value(true)
                        .help("Copy data after (and including) this time. YYYY-MM-DD-HH")
                        .long_help("Copy all data after (and including) this date.")
                ).arg(
                    Arg::with_name("end")
                        .long("end")
                        .takes_value(true)
                        .help("Copy data up to and including this time. YYYY-MM-DD-HH")
                        .long_help("Copy all data up to (and including) this date."),
                ).arg(
                    Arg::with_name("dest")
                        .long("destination")
                        .short("d")
                        .index(1)
                        .required(true)
                        .takes_value(true)
                        .help("Destination directory of copy.")
                )
        ).subcommand(
            SubCommand::with_name("fix")
                .about("Find and fix inconsistencies in the archive.")
        );

    let matches = app.get_matches();

    let root = &matches
        .value_of("root")
        .map(PathBuf::from)
        .or_else(|| home_dir().map(|hd| hd.join("bufkit")))
        .expect("Invalid root.");

    match matches.subcommand() {
        ("create", Some(sub_args)) => create::create(root, sub_args)?,
        ("sites", Some(sub_args)) => sites::sites(root, sub_args)?,
        ("export", Some(sub_args)) => export::export(root, sub_args)?,
        ("import", Some(sub_args)) => import::import(root, sub_args)?,
        ("purge", Some(sub_args)) => purge::purge(root, sub_args)?,
        ("fix", Some(sub_args)) => fix::fix(root, sub_args)?,
        ("copy", Some(sub_args)) => copy::copy(root, sub_args)?,
        _ => unreachable!(),
    }

    Ok(())
}
