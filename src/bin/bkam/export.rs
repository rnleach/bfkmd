use bfkmd::{bail, parse_date_string};
use bufkit_data::{Archive, BufkitDataErr, Model, StationNumber};
use chrono::NaiveDateTime;
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
    let site_id = sub_args.value_of("site").unwrap();
    let model = sub_args.value_of("model").unwrap();
    let target = sub_args.value_of("target").unwrap();

    //
    // Validate required arguments.
    //
    let model = match Model::from_str(model) {
        Ok(model) => model,
        Err(_) => {
            bail(&format!("Model {} does not exist in the archive!", model));
        }
    };

    let site: StationNumber = match arch.station_num_for_id(site_id, model) {
        Ok(station_num) => station_num,
        Err(BufkitDataErr::NotInIndex) => {
            bail(&format!("Site {} does not exist in the archive!", site_id))
        }
        Err(err) => bail(&format!("Database error: {}", err)),
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
    #[derive(Debug, Clone, Copy)]
    enum OptionalDateArg {
        NotSpecified,
        Specified(NaiveDateTime),
    }

    let start_date: OptionalDateArg = match sub_args.value_of("start") {
        Some(start_date) => OptionalDateArg::Specified(parse_date_string(start_date)),
        None => OptionalDateArg::NotSpecified,
    };

    let end_date: OptionalDateArg = match sub_args.value_of("end") {
        Some(end_date) => OptionalDateArg::Specified(parse_date_string(end_date)),
        None => start_date,
    };

    match (start_date, end_date) {
        (OptionalDateArg::NotSpecified, OptionalDateArg::NotSpecified) => {
            let data = arch.retrieve_most_recent(site, model)?;
            save_file(&target, site_id, model, None, &data)?;
        }
        (OptionalDateArg::Specified(start), OptionalDateArg::Specified(end)) => {
            for init_time in model.all_runs(&start, &end) {
                let data = arch.retrieve(site, model, init_time)?;
                save_file(&target, site_id, model, Some(init_time), &data)?;
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn save_file(
    save_dir: &Path,
    site_id: &str,
    model: Model,
    init_time: Option<NaiveDateTime>,
    data: &str,
) -> Result<(), Box<dyn Error>> {
    let fname: String = if let Some(init_time) = init_time {
        let file_string = init_time.format("%Y%m%d%HZ").to_string();

        format!(
            "{}_{}_{}.buf",
            file_string,
            model.as_static_str(),
            site_id.to_uppercase()
        )
    } else {
        format!("{}_{}.buf", site_id.to_uppercase(), model.as_static_str(),)
    };

    let save_path = save_dir.join(fname);
    let f = File::create(save_path)?;
    let mut bw = BufWriter::new(f);
    bw.write_all(data.as_bytes())?;
    Ok(())
}
