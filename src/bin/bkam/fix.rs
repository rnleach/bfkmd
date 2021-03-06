use bufkit_data::Archive;
use clap::ArgMatches;
use std::{error::Error, path::PathBuf};

pub fn fix(root: &PathBuf, _sub_args: &ArgMatches) -> Result<(), Box<dyn Error>> {
    // Check that the root exists.
    println!("Checking if the archive location exists.");
    if !root.as_path().is_dir() {
        println!("Archive root directory not found. Quitting.");
        return Err("Invalid root.".into());
    } else {
        println!("Found, moving on.\n");
    }

    // Check that the data directory exists
    println!("Checking for the data directory within the archive.");
    let data_dir = &root.join("data");
    if !data_dir.as_path().is_dir() {
        println!("Archive data directory not found. Archive is empty. Quitting.");
        return Err("Invalid data directory.".into());
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
            Archive::create(root)?
        }
    };

    // Check that all the files listed in the index are also in the data directory
    println!("Cleaning up the index.");
    arch.clean()?;
    println!("Done.\n");

    Ok(())
}
