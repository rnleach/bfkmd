//! BufKit Archive Manager
extern crate bfkmd;
extern crate bufkit_data;
extern crate chrono;
#[macro_use]
extern crate clap;
extern crate dirs;
extern crate failure;
extern crate pbr;
extern crate sounding_bufkit;
extern crate strum;

use bfkmd::{bail, parse_date_string, TablePrinter};
use bufkit_data::{Archive, BufkitDataErr, Model, Site, StateProv};
use chrono::{FixedOffset, NaiveDate, Utc};
use clap::{App, Arg, ArgMatches, SubCommand};
use dirs::home_dir;
use failure::{err_msg, Error, Fail};
use pbr::ProgressBar;
use sounding_bufkit::BufkitFile;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
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
    let app = App::new("bkam")
        .author("Ryan Leach <clumsycodemonkey@gmail.com>")
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
                            Arg::with_name("site")
                                .index(1)
                                .required(true)
                                .takes_value(true)
                                .help("The site to modify."),
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
                            Arg::with_name("site")
                                .index(1)
                                .required(true)
                                .takes_value(true)
                                .help("The site to get the inventory for."),
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
            SubCommand::with_name("fix")
                .about("Find and fix inconsistencies in the archive.")
        );

    let matches = app.get_matches();

    let root = &matches
        .value_of("root")
        .map(PathBuf::from)
        .or_else(|| home_dir().and_then(|hd| Some(hd.join("bufkit"))))
        .expect("Invalid root.");

    match matches.subcommand() {
        ("create", Some(sub_args)) => create(root, sub_args)?,
        ("sites", Some(sub_args)) => sites(root, sub_args)?,
        ("export", Some(sub_args)) => export(root, sub_args)?,
        ("import", Some(sub_args)) => import(root, sub_args)?,
        ("purge", Some(sub_args)) => purge(root, sub_args)?,
        ("fix", Some(sub_args)) => fix(root, sub_args)?,
        _ => unreachable!(),
    }

    Ok(())
}

fn create(_root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Error> {
    let root = &sub_args
        .value_of("archive_root")
        .map(PathBuf::from)
        .or_else(|| home_dir().and_then(|hd| Some(hd.join("bufkit"))))
        .expect("Invalid root.");
    // Check if the archive already exists. (try connecting to it)
    let already_exists: bool = Archive::connect(root).is_ok();

    if already_exists && sub_args.is_present("force") {
        ::std::fs::remove_dir_all(root)?;
    } else if already_exists {
        return Err(err_msg(
            "Archive already exists, must use --force to overwrite.",
        ));
    }

    Archive::create_new(root)?;

    Ok(())
}

fn sites(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Error> {
    match sub_args.subcommand() {
        ("list", Some(sub_sub_args)) => sites_list(root, sub_args, &sub_sub_args),
        ("modify", Some(sub_sub_args)) => sites_modify(root, sub_args, &sub_sub_args),
        ("inv", Some(sub_sub_args)) => sites_inventory(root, sub_args, &sub_sub_args),
        _ => unreachable!(),
    }
}

fn sites_list(
    root: &PathBuf,
    _sub_args: &ArgMatches,
    sub_sub_args: &ArgMatches,
) -> Result<(), Error> {
    let arch = Archive::connect(root)?;

    //
    // This filter lets all sites pass
    //
    let pass = &|_site: &Site| -> bool { true };

    //
    // Filter based on state
    //
    let state_value = if let Some(st) = sub_sub_args.value_of("state") {
        StateProv::from_str(&st.to_uppercase()).unwrap_or(StateProv::AL)
    } else {
        StateProv::AL
    };

    let state_filter = &|site: &Site| -> bool {
        match site.state {
            Some(st) => st == state_value,
            None => false,
        }
    };
    let in_state_pred: &Fn(&Site) -> bool = if sub_sub_args.is_present("state") {
        state_filter
    } else {
        pass
    };

    //
    // Filter for missing any data
    //
    let missing_any = &|site: &Site| -> bool { site.name.is_none() || site.state.is_none() };
    let missing_any_pred: &Fn(&Site) -> bool = if sub_sub_args.is_present("missing-data") {
        missing_any
    } else {
        pass
    };

    //
    // Filter for missing state
    //
    let missing_state = &|site: &Site| -> bool { site.state.is_none() };
    let missing_state_pred: &Fn(&Site) -> bool = if sub_sub_args.is_present("missing-state") {
        missing_state
    } else {
        pass
    };

    //
    // Filter based on auto download
    //
    let auto_download = &|site: &Site| -> bool { site.auto_download };
    let no_auto_download = &|site: &Site| -> bool { !site.auto_download };
    let auto_download_pred: &Fn(&Site) -> bool = if sub_sub_args.is_present("auto-download") {
        auto_download
    } else if sub_sub_args.is_present("no-auto-download") {
        no_auto_download
    } else {
        pass
    };

    //
    // Combine filters to make an iterator over the sites.
    //
    let master_list = arch.get_sites()?;
    let sites_iter = || {
        master_list
            .iter()
            .filter(|s| missing_any_pred(s))
            .filter(|s| missing_state_pred(s))
            .filter(|s| in_state_pred(s))
            .filter(|s| auto_download_pred(s))
    };

    let mut table_printer = if sites_iter().count() == 0 {
        println!("No sites matched criteria.");
        return Ok(());
    } else {
        TablePrinter::new()
            .with_title("Sites".to_owned())
            .with_column::<String, String>("ID".to_owned(), &[])
            .with_column::<String, String>("STATE".to_owned(), &[])
            .with_column::<String, String>("NAME".to_owned(), &[])
            .with_column::<String, String>("UTC Offset".to_owned(), &[])
            .with_column::<String, String>("Auto Download".to_owned(), &[])
            .with_column::<String, String>("MODELS".to_owned(), &[])
            .with_column::<String, String>("NOTES".to_owned(), &[])
    };

    let blank = "-".to_owned();

    for site in sites_iter() {
        let id = &site.id;
        let state = site.state.map(|st| st.as_static()).unwrap_or(&"-");
        let name = site.name.as_ref().unwrap_or(&blank);
        let offset = site
            .time_zone
            .map(|val| val.to_string())
            .unwrap_or(blank.clone());
        let notes = site.notes.as_ref().unwrap_or(&blank);
        let auto_dl = if site.auto_download { "Yes" } else { "No" };
        let models = arch
            .models_for_site(id)?
            .into_iter()
            .map(|mdl| mdl.as_static().to_owned())
            .collect::<Vec<String>>()
            .join(",");
        let row = vec![
            id.to_string(),
            state.to_string(),
            name.to_string(),
            offset,
            auto_dl.to_string(),
            models.to_string(),
            notes.to_string(),
        ];
        table_printer.add_row(row);
    }

    table_printer.print()
}

fn sites_modify(
    root: &PathBuf,
    _sub_args: &ArgMatches,
    sub_sub_args: &ArgMatches,
) -> Result<(), Error> {
    let arch = Archive::connect(root)?;

    // Safe to unwrap because the argument is required.
    let site = sub_sub_args.value_of("site").unwrap();
    let mut site = arch.get_site_info(site)?;

    if let Some(new_state) = sub_sub_args.value_of("state") {
        match StateProv::from_str(&new_state.to_uppercase()) {
            Ok(new_state) => site.state = Some(new_state),
            Err(_) => println!("Unable to parse state/providence: {}", new_state),
        }
    }

    if let Some(dl) = sub_sub_args.value_of("auto-download") {
        match dl {
            "Yes" | "yes" => site.auto_download = true,
            "No" | "no" => site.auto_download = false,
            _ => unreachable!(),
        }
    }

    if let Some(new_name) = sub_sub_args.value_of("name") {
        site.name = Some(new_name.to_owned());
    }

    if let Some(new_notes) = sub_sub_args.value_of("notes") {
        site.notes = Some(new_notes.to_owned())
    }

    if let Some(new_offset) = sub_sub_args.value_of("utc-offset") {
        if let Ok(new_offset) = new_offset.parse::<i32>() {
            let seconds = new_offset * 3600;
            if seconds < 0 {
                site.time_zone = Some(FixedOffset::west(seconds.abs()));
            } else {
                site.time_zone = Some(FixedOffset::east(seconds));
            }
        }
    }

    arch.set_site_info(&site)?;
    Ok(())
}

fn sites_inventory(
    root: &PathBuf,
    _sub_args: &ArgMatches,
    sub_sub_args: &ArgMatches,
) -> Result<(), Error> {
    let arch = Archive::connect(root)?;

    // Safe to unwrap because the argument is required.
    let site = sub_sub_args
        .value_of("site")
        .ok_or(BufkitDataErr::GeneralError)?;

    for model in Model::iter() {
        let inv = match arch.get_inventory(site, model) {
            ok @ Ok(_) => ok,
            Err(BufkitDataErr::NotEnoughData) => {
                println!(
                    "No data for model {} and site {}.",
                    model.as_static(),
                    site.to_uppercase()
                );
                continue;
            }
            err @ Err(_) => err,
        }?;

        if inv.missing.is_empty() {
            println!("\nInventory for {} at {}.", model, site.to_uppercase());
            println!("   start: {}", inv.first);
            println!("     end: {}", inv.last);
            println!("          No missing runs!");
        } else {
            let mut tp = TablePrinter::new()
                .with_title(format!(
                    "Inventory for {} at {}.",
                    model,
                    site.to_uppercase()
                )).with_header(format!("{} -> {}", inv.first, inv.last));

            let dl = if inv.auto_download { "" } else { " NOT" };
            tp = tp.with_footer(format!("This site is{} automatically downloaded.", dl));

            let mut cycles = vec![];
            let mut start = vec![];
            let mut end = vec![];
            for missing in &inv.missing {
                let start_run = missing.0;
                let end_run = missing.1;
                let num_cycles = (end_run - start_run).num_hours() / model.hours_between_runs() + 1;
                cycles.push(format!("{}", num_cycles));
                start.push(format!("{}", start_run));
                end.push(format!("{}", end_run));
            }

            tp = tp
                .with_column("Cycles", &cycles)
                .with_column("From", &start)
                .with_column("To", &end);
            tp.print()?;
        }
    }

    Ok(())
}

fn export(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Error> {
    let arch = Archive::connect(root)?;

    // unwrap is ok, these are required.
    let site = sub_args.value_of("site").unwrap();
    let model = sub_args.value_of("model").unwrap();
    let target = sub_args.value_of("target").unwrap();

    //
    // Validate required arguments.
    //
    if !arch.site_exists(site)? {
        bail(&format!("Site {} does not exist in the archive!", site));
    }

    let model = match Model::from_str(model) {
        Ok(model) => model,
        Err(_) => {
            bail(&format!("Model {} does not exist in the archive!", model));
        }
    };

    let target = Path::new(target);
    if !target.is_dir() {
        bail(&format!(
            "Path {} is not a directory that already exists.",
            target.display()
        ));
    }

    //
    //  Set up optional arguments.
    //

    let start_date = if let Some(start_date) = sub_args.value_of("start") {
        parse_date_string(start_date)
    } else {
        match arch.get_most_recent_valid_time(site, model) {
            Ok(vt) => vt,
            Err(_) => bail(&format!("No data for site {} and model {}.", site, model)),
        }
    };

    let end_date = if let Some(end_date) = sub_args.value_of("end") {
        parse_date_string(end_date)
    } else if sub_args.is_present("start") {
        arch.get_most_recent_valid_time(site, model)?
    } else {
        start_date
    };

    for init_time in model.all_runs(&start_date, &end_date) {
        if !arch.exists(site, model, &init_time)? {
            continue;
        }

        let save_path = target.join(arch.file_name(site, model, &init_time));
        let data = arch.get_file(site, model, &init_time)?;
        let mut f = File::create(save_path)?;
        let mut bw = BufWriter::new(f);
        bw.write_all(data.as_bytes())?;
    }

    Ok(())
}

fn import(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Error> {
    let arch = Archive::connect(root)?;

    // unwrap is ok, these are required.
    let site = sub_args.value_of("site").unwrap();
    let model = sub_args.value_of("model").unwrap();

    let files: Vec<PathBuf> = sub_args
        .values_of("file")
        .into_iter()
        .flat_map(|file_iter| file_iter.map(PathBuf::from))
        .collect();

    //
    // Validate required arguments.
    //
    let model = match Model::from_str(model) {
        Ok(model) => model,
        Err(_) => {
            bail(&format!("Model {} does not exist in the archive!", model));
        }
    };

    for file in files {
        let f = BufkitFile::load(&file)?;
        let data = f.data()?;

        let anal = data
            .into_iter()
            .nth(0)
            .ok_or(BufkitDataErr::NotEnoughData)?;
        let init_time = anal
            .sounding()
            .get_valid_time()
            .ok_or(BufkitDataErr::NotEnoughData)?;

        arch.add_file(site, model, &init_time, f.raw_text())?;
    }

    Ok(())
}

fn purge(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Error> {
    let arch = Archive::connect(root)?;

    let mut sites: Vec<String> = sub_args
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(|arg_val| arg_val.to_owned()))
        .collect();

    let mut models: Vec<Model> = sub_args
        .values_of("models")
        .into_iter()
        .flat_map(|model_iter| model_iter.map(Model::from_str))
        .filter_map(|res| res.ok())
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
        sites = arch.get_sites()?.into_iter().map(|site| site.id).collect();
    }

    if models.is_empty() {
        models = Model::iter().collect();
    }

    for site in &sites {
        let available_models = arch.models_for_site(site)?;
        for &model in &models {
            if !available_models.contains(&model) {
                continue;
            }

            let all_runs = model.all_runs(&after, &before);

            for run in all_runs {
                println!("  Removing {} {} {}.", site, model.as_static(), run);
                if let Ok(()) = arch.remove_file(site, model, &run) {}
            }
        }
    }

    Ok(())
}

fn fix(root: &PathBuf, _sub_args: &ArgMatches) -> Result<(), Error> {
    // Check that the root exists.
    println!("Checking if the archive location exists.");
    if !root.as_path().is_dir() {
        println!("Archive root directory not found. Quitting.");
        return Err(err_msg("Invalid root."));
    } else {
        println!("Found, moving on.\n");
    }

    // Check that the data directory exists
    println!("Checking for the data directory within the archive.");
    let data_dir = &root.join("data");
    if !data_dir.as_path().is_dir() {
        println!("Archive data directory not found. Archive is empty. Quitting.");
        return Err(err_msg("Invalid data directory."));
    } else {
        println!("Found, moving on.\n");
    }

    // Check if there is a database, if not, create it!
    println!("Trying to connect to the archive file index (database).");
    let arch = match Archive::connect(root) {
        Ok(arch) => {
            println!("Found the archive file index. Moving on.\n");
            arch
        }
        Err(err) => {
            println!(
                "Error connecting to archive database {}. Trying to create a new database.\n",
                err
            );
            Archive::create_new(root)?
        }
    };

    // Check that all the files listed in the index are also in the data directory
    println!("Checking index for non-existent files.");
    let (total_count, recv) = arch.clean_index()?;
    let mut pb = ProgressBar::new(total_count as u64);
    for (inc, msg_opt) in recv {
        if let Some(msg) = msg_opt {
            pb.message(&msg);
        }

        pb.set(inc as u64);
    }
    pb.finish();
    println!("Done.\n");

    // Check for extra files
    println!("Checking that files in data directory belong and are in the index.");
    let (total_count, recv) = arch.clean_data()?;
    let mut pb = ProgressBar::new(total_count as u64);
    for (inc, msg_opt) in recv {
        if let Some(msg) = msg_opt {
            let msg = format!(" {} ", msg);
            pb.message(&msg);
        }

        pb.set(inc as u64);
    }
    pb.finish();
    println!("Done.\n");

    // Compress the database.
    println!("Optimizing the index.");
    arch.compress_index()?;
    println!("Done.\n");

    Ok(())
}
