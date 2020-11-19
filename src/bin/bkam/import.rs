use bfkmd::bail;
use bufkit_data::{Archive, Model};
use clap::ArgMatches;
use sounding_bufkit::BufkitFile;
use std::{error::Error, path::PathBuf, str::FromStr};

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

        arch.add(&site_id.to_uppercase(), model, f.raw_text())?;
    }

    Ok(())
}
