use bfkmd::TablePrinter;
use bufkit_data::{Archive, BufkitDataErr, Model, SiteInfo, StateProv, StationNumber};
use chrono::FixedOffset;
use clap::ArgMatches;
use std::{collections::HashSet, error::Error, path::PathBuf, str::FromStr};
use strum::IntoEnumIterator;

pub fn sites(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Box<dyn Error>> {
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
) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(root)?;

    //
    // This filter lets all sites pass
    //
    let pass = &|_site: &SiteInfo| -> bool { true };

    //
    // Filter based on state
    //
    let state_value = if let Some(st) = sub_sub_args.value_of("state") {
        StateProv::from_str(&st.to_uppercase()).unwrap_or(StateProv::AL)
    } else {
        StateProv::AL
    };

    let state_filter = &|site: &SiteInfo| -> bool {
        match site.state {
            Some(st) => st == state_value,
            None => false,
        }
    };
    let in_state_pred: &dyn Fn(&SiteInfo) -> bool = if sub_sub_args.is_present("state") {
        state_filter
    } else {
        pass
    };

    //
    // Filter for missing any data
    //
    let missing_any = &|site: &SiteInfo| -> bool { site.name.is_none() || site.state.is_none() };
    let missing_any_pred: &dyn Fn(&SiteInfo) -> bool = if sub_sub_args.is_present("missing-data") {
        missing_any
    } else {
        pass
    };

    //
    // Filter for missing state
    //
    let missing_state = &|site: &SiteInfo| -> bool { site.state.is_none() };
    let missing_state_pred: &dyn Fn(&SiteInfo) -> bool = if sub_sub_args.is_present("missing-state")
    {
        missing_state
    } else {
        pass
    };

    //
    // Filter based on auto download
    //
    let auto_download = &|site: &SiteInfo| -> bool { site.auto_download };
    let no_auto_download = &|site: &SiteInfo| -> bool { !site.auto_download };
    let auto_download_pred: &dyn Fn(&SiteInfo) -> bool = if sub_sub_args.is_present("auto-download")
    {
        auto_download
    } else if sub_sub_args.is_present("no-auto-download") {
        no_auto_download
    } else {
        pass
    };

    //
    // Combine filters to make an iterator over the sites.
    //
    let mut master_list: Vec<SiteInfo> = arch.sites()?;

    master_list.sort_unstable_by(|left, right| {
        match (
            left.state.map(|l| l.as_static_str()),
            right.state.map(|r| r.as_static_str()),
        ) {
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    });

    let sites_iter = || {
        master_list
            .iter()
            .filter(|s| missing_any_pred(&s))
            .filter(|s| missing_state_pred(&s))
            .filter(|s| in_state_pred(&s))
            .filter(|s| auto_download_pred(&s))
    };

    let mut table_printer = if sites_iter().count() == 0 {
        println!("No sites matched criteria.");
        return Ok(());
    } else {
        TablePrinter::new()
            .with_title("Sites".to_owned())
            .with_column::<String, String>("Stn Num".to_owned(), &[])
            .with_column::<String, String>("IDs".to_owned(), &[])
            .with_column::<String, String>("STATE".to_owned(), &[])
            .with_column::<String, String>("NAME".to_owned(), &[])
            .with_column::<String, String>("UTC Offset".to_owned(), &[])
            .with_column::<String, String>("Auto Download".to_owned(), &[])
            .with_column::<String, String>("MODELS".to_owned(), &[])
            .with_column::<String, String>("NOTES".to_owned(), &[])
            .with_column::<String, String>("Num files".to_owned(), &[])
    };

    let blank = "-".to_owned();

    for site in sites_iter() {
        let station_num = site.station_num;
        let ids = arch
            .models(site.station_num)?
            .into_iter()
            .filter_map(|mdl| arch.ids(site.station_num, mdl).ok())
            .flat_map(|ids| ids.into_iter())
            .collect::<HashSet<String>>()
            .into_iter()
            .collect::<Vec<String>>()
            .join(",");
        let state = site.state.map(|st| st.as_static_str()).unwrap_or(&"-");
        let name = site.name.as_ref().unwrap_or(&blank);
        let offset = site
            .time_zone
            .map(|val| val.to_string())
            .unwrap_or_else(|| blank.clone());
        let notes = site.notes.as_ref().unwrap_or(&blank);
        let auto_dl = if site.auto_download { "Yes" } else { "No" };
        let models = arch
            .models(site.station_num)?
            .into_iter()
            .map(|mdl| mdl.as_static_str().to_owned())
            .collect::<Vec<String>>()
            .join(",");
        let num_files: u32 = Model::iter()
            .filter_map(|m| arch.count(station_num, m).ok())
            .sum();
        let row = vec![
            station_num.to_string(),
            ids.to_string(),
            state.to_string(),
            name.to_string(),
            offset,
            auto_dl.to_string(),
            models.to_string(),
            notes.to_string(),
            num_files.to_string(),
        ];
        table_printer.add_row(row);
    }

    table_printer.print()?;
    Ok(())
}

fn sites_modify(
    root: &PathBuf,
    _sub_args: &ArgMatches,
    sub_sub_args: &ArgMatches,
) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(root)?;

    // Safe to unwrap because the argument is required.
    let site = sub_sub_args
        .value_of("stn")
        .unwrap()
        .parse::<u32>()
        .map(StationNumber::from)?;

    let mut site = arch
        .site(site)
        .ok_or_else(|| BufkitDataErr::InvalidSiteId(site.to_string()))?;

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

    arch.update_site(&site)?;
    Ok(())
}

fn sites_inventory(
    root: &PathBuf,
    _sub_args: &ArgMatches,
    sub_sub_args: &ArgMatches,
) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(root)?;

    // Safe to unwrap because the argument is required.
    let site = sub_sub_args
        .value_of("station_number")
        .unwrap()
        .parse::<u32>()
        .map(StationNumber::from)?;
    let site = arch
        .site(site)
        .ok_or_else(|| BufkitDataErr::InvalidSiteId(site.to_string()))?;

    for model in Model::iter() {
        let inv = match arch.inventory(site.station_num, model) {
            ok @ Ok(_) => ok,
            Err(BufkitDataErr::NotEnoughData) => {
                println!(
                    "No data for model {} and site {}.",
                    model.as_static_str(),
                    site.station_num.to_string()
                );
                continue;
            }
            err @ Err(_) => err,
        }?;

        let first = inv.iter().nth(0).unwrap(); // unwrap OK because we already checked not empty.
        let last = inv.iter().last().unwrap(); // unwrap OK because we already checked not empty.

        let missing = arch.missing_inventory(site.station_num, model, None)?;

        if missing.is_empty() {
            println!(
                "\nInventory for {} at {}.",
                model,
                site.station_num.to_string()
            );
            println!("   start: {}", first);
            println!("     end: {}", last);
            println!("          No missing runs!");
        } else {
            let mut tp = TablePrinter::new()
                .with_title(format!(
                    "Inventory for {} at station {}.",
                    model,
                    site.station_num.to_string()
                ))
                .with_header(format!("{} -> {}", first, last));

            let dl = if site.auto_download { "" } else { " NOT" };
            tp = tp.with_footer(format!("This site is{} automatically downloaded.", dl));

            let mut cycles = vec![];
            let mut start = vec![];
            let mut end = vec![];

            let mut iter = missing.into_iter();
            // Unwrap OK because we already check is_empty()
            let mut start_run = iter.next().unwrap();
            let mut end_run = start_run;
            let mut last_round = start_run;
            for missing in iter {
                if (missing - last_round).num_hours() / model.hours_between_runs() == 1 {
                    end_run = missing;
                    last_round = missing;
                    continue;
                } else {
                    let num_cycles =
                        (end_run - start_run).num_hours() / model.hours_between_runs() + 1;
                    cycles.push(format!("{}", num_cycles));
                    start.push(format!("{}", start_run));
                    end.push(format!("{}", end_run));

                    start_run = missing;
                    end_run = missing;
                    last_round = missing;
                }
            }

            // Don't forget to add the last one!
            let num_cycles = (end_run - start_run).num_hours() / model.hours_between_runs() + 1;
            cycles.push(format!("{}", num_cycles));
            start.push(format!("{}", start_run));
            end.push(format!("{}", end_run));

            tp = tp
                .with_column("Cycles", &cycles)
                .with_column("From", &start)
                .with_column("To", &end);
            tp.print()?;
        }
    }

    Ok(())
}
