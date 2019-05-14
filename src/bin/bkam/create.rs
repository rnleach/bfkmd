use bufkit_data::Archive;
use clap::ArgMatches;
use dirs::home_dir;
use std::{error::Error, path::PathBuf};

pub fn create(_root: &PathBuf, sub_args: &ArgMatches) -> Result<(), Box<dyn Error>> {
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
        Err("Archive already exists, must use --force to overwrite.")?;
    }

    Archive::create(root)?;

    Ok(())
}
