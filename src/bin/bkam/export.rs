use bfkmd::{bail, parse_date_string};
use bufkit_data::{Archive, Model};
use clap::ArgMatches;
use std::{
    error::Error,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

pub fn export(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Box<dyn Error>> {
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
        match arch.most_recent_init_time(site, model) {
            Ok(vt) => vt,
            Err(_) => bail(&format!("No data for site {} and model {}.", site, model)),
        }
    };

    let end_date = if let Some(end_date) = sub_args.value_of("end") {
        parse_date_string(end_date)
    } else if sub_args.is_present("start") {
        arch.most_recent_init_time(site, model)?
    } else {
        start_date
    };

    for init_time in model.all_runs(&start_date, &end_date) {
        if !arch.file_exists(site, model, &init_time)? {
            continue;
        }

        let save_path = target.join(arch.file_name(site, model, &init_time));
        let data = arch.retrieve(site, model, init_time)?;
        let f = File::create(save_path)?;
        let mut bw = BufWriter::new(f);
        bw.write_all(data.as_bytes())?;
    }

    Ok(())
}
