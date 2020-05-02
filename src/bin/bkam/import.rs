use bfkmd::bail;
use bufkit_data::{Archive, BufkitDataErr, Model, Site};
use clap::ArgMatches;
use sounding_bufkit::BufkitFile;
use std::{convert::TryFrom, error::Error, path::PathBuf, str::FromStr};

pub fn import(root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let arch = Archive::connect(root)?;

    // unwrap is ok, these are required.
    let site_id = sub_args.value_of("site").unwrap();
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
            .next()
            .ok_or(BufkitDataErr::NotEnoughData)?;
        let init_time = anal.0.valid_time().ok_or(BufkitDataErr::NotEnoughData)?;

        let anal = data
            .into_iter()
            .last()
            .ok_or(BufkitDataErr::NotEnoughData)?;
        let end_time = anal.0.valid_time().ok_or(BufkitDataErr::NotEnoughData)?;

        let station_num: i32 = anal
            .0
            .station_info()
            .station_num()
            .into_option()
            .ok_or(BufkitDataErr::NotEnoughData)?;
        let station_num = u32::try_from(station_num)?;

        let site = Site {
            station_num,
            id: Some(site_id.to_uppercase()),
            ..Site::default()
        };

        arch.add(&site, model, init_time, end_time, f.raw_text())?;
    }

    Ok(())
}
