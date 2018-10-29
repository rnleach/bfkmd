use bufkit_data::Archive;
use clap::ArgMatches;
use failure::{err_msg, Error};
use std::path::PathBuf;

pub fn fix(root: &PathBuf, _sub_args: &ArgMatches) -> Result<(), Error> {
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
    println!("Cleaning up the index.");
    let (jh, recv) = arch.clean_archive()?;
    for message in recv {
        println!("      {}", message);
    }
    println!("Done.\n");

    jh.join().unwrap()?;

    Ok(())
}
