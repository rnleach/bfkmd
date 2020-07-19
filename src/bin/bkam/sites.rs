use bfkmd::{bail, TablePrinter};
use bufkit_data::{Archive, BufkitDataErr, Model, StateProv, StationNumber, StationSummary};
use chrono::FixedOffset;
use clap::ArgMatches;
use std::{error::Error, path::PathBuf, str::FromStr};

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
    let pass = &|_site: &StationSummary| -> bool { true };

    //
    // Filter based on state
    //
    let state_value = if let Some(st) = sub_sub_args.value_of("state") {
        StateProv::from_str(&st.to_uppercase()).unwrap_or(StateProv::AL)
    } else {
        StateProv::AL
    };

    let state_filter = &|site: &StationSummary| -> bool {
        match site.state {
            Some(st) => st == state_value,
            None => false,
        }
    };
    let in_state_pred: &dyn Fn(&StationSummary) -> bool = if sub_sub_args.is_present("state") {
        state_filter
    } else {
        pass
    };

    //
    // Filter for missing any data
    //
    let missing_any = &|site: &StationSummary| -> bool {
        site.name.is_none() || site.time_zone.is_none() || site.state.is_none()
    };
    let missing_any_pred: &dyn Fn(&StationSummary) -> bool =
        if sub_sub_args.is_present("missing-data") {
            missing_any
        } else {
            pass
        };

    //
    // Filter for missing state
    //
    let missing_state = &|site: &StationSummary| -> bool { site.state.is_none() };
    let missing_state_pred: &dyn Fn(&StationSummary) -> bool =
        if sub_sub_args.is_present("missing-state") {
            missing_state
        } else {
            pass
        };

    //
    // Filter based on auto download
    //
    let auto_download = &|site: &StationSummary| -> bool { site.auto_download };
    let no_auto_download = &|site: &StationSummary| -> bool { !site.auto_download };
    let auto_download_pred: &dyn Fn(&StationSummary) -> bool =
        if sub_sub_args.is_present("auto-download") {
            auto_download
        } else if sub_sub_args.is_present("no-auto-download") {
            no_auto_download
        } else {
            pass
        };

    //
    // Combine filters to make an iterator over the sites.
    //
    let mut master_list: Vec<StationSummary> = arch.station_summaries()?;

    master_list.sort_unstable_by(|left, right| {
        let lnum: u32 = left.station_num.into();
        let rnum: u32 = right.station_num.into();

        match (
            left.state.map(|l| l.as_static_str()),
            right.state.map(|r| r.as_static_str()),
            lnum,
            rnum,
        ) {
            (Some(_), None, _, _) => std::cmp::Ordering::Less,
            (None, Some(_), _, _) => std::cmp::Ordering::Greater,
            // Within a state, order by station number
            (Some(left), Some(right), lnum, rnum) => match left.cmp(right) {
                std::cmp::Ordering::Equal => lnum.cmp(&rnum),
                x => x,
            },
            // Without a state, order by station number
            (None, None, lnum, rnum) => lnum.cmp(&rnum),
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
        let ids = site.ids_as_string();
        let state = site.state.map(|st| st.as_static_str()).unwrap_or(&"-");
        let name = site.name.as_ref().unwrap_or(&blank);
        let offset = site
            .time_zone
            .map(|val| val.to_string())
            .unwrap_or_else(|| blank.clone());
        let notes = site.notes.as_ref().unwrap_or(&blank);
        let auto_dl = if site.auto_download { "Yes" } else { "No" };
        let models = site.models_as_string();
        let num_files = site.number_of_files;
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
    let arch = &Archive::connect(root)?;

    let site = {
        // Safe to unwrap because the argument is required.
        let str_val = sub_sub_args.value_of("stn").unwrap();

        if let Ok(stn_num) = str_val.parse::<u32>().map(StationNumber::from) {
            stn_num
        } else {
            bfkmd::site_id_to_station_num(arch, str_val)?
        }
    };

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
    let arch = &Archive::connect(root)?;

    let site = {
        // Safe to unwrap because the argument is required.
        let str_val = sub_sub_args.value_of("stn").unwrap();

        if let Ok(stn_num) = str_val.parse::<u32>().map(StationNumber::from) {
            stn_num
        } else {
            bfkmd::site_id_to_station_num(arch, str_val)?
        }
    };

    let site = arch
        .site(site)
        .ok_or_else(|| BufkitDataErr::InvalidSiteId(site.to_string()))?;

    let model = sub_sub_args.value_of("model").unwrap();
    let model = match Model::from_str(model) {
        Ok(model) => model,
        Err(_) => {
            bail(&format!("Model {} does not exist in the archive!", model));
        }
    };

    let inv = match arch.inventory(site.station_num, model) {
        ok @ Ok(_) => ok,
        Err(BufkitDataErr::NotEnoughData) => {
            bail(&format!(
                "No data for model {} and site {}.",
                model.as_static_str(),
                site.station_num.to_string()
            ));
        }
        err @ Err(_) => err,
    }?;

    let (first, last) = match (inv.iter().nth(0), inv.iter().last()) {
        (Some(first), Some(last)) => (first, last),
        _ => unreachable!(),
    };

    let missing = arch.missing_inventory(site.station_num, model, None)?;

    if missing.is_empty() {
        println!(
            "\nInventory for {} at {}({}).",
            model,
            site.name.as_deref().unwrap_or(""),
            site.station_num.to_string()
        );
        println!("   start: {}", first);
        println!("     end: {}", last);
        println!("          No missing runs!");
    } else {
        let mut tp = TablePrinter::new()
            .with_title(format!(
                "Inventory for {} at station {}({}).",
                model,
                site.name.as_deref().unwrap_or(""),
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
        let mut total_missing = 0;
        for missing in iter {
            if (missing - last_round).num_hours() / model.hours_between_runs() == 1 {
                end_run = missing;
                last_round = missing;
                continue;
            } else {
                let num_cycles = (end_run - start_run).num_hours() / model.hours_between_runs() + 1;
                cycles.push(format!("{}", num_cycles));
                start.push(format!("{}", start_run));
                end.push(format!("{}", end_run));
                total_missing += num_cycles;

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
        total_missing += num_cycles;

        cycles.push(format!("-- {} --", total_missing));
        start.push(" -- Total -- ".to_owned());
        end.push("".to_owned());

        tp = tp
            .with_column("Cycles", &cycles)
            .with_column("From", &start)
            .with_column("To", &end);
        tp.print()?;
    }

    Ok(())
}
