use bufkit_data::{Archive, Model};
use chrono::{NaiveDate, NaiveDateTime};
use climo_db::{start_climo_db_thread, DBRequest, DBResponse};
use crossbeam_channel::{self, Receiver, Sender};
use failure::Error;
use pbr::ProgressBar;
use sounding_analysis::{haines_high, haines_low, haines_mid, hot_dry_windy, Analysis};
use sounding_bufkit::BufkitData;
use std::thread::{self, JoinHandle};

pub fn build_climo(arch: &Archive, site: &str, model: Model) -> Result<(), Error> {
    let root = arch.get_root();

    // Channel to transmit errors back to main (this) thread.
    let (quit_sender, quit_receiver) = crossbeam_channel::bounded::<Error>(1);

    // Start a thread to manage the climo database
    let (climo_handle, to_climo_db) = start_climo_db_thread(root, site, model, quit_sender.clone());

    // Set up a generator that compares the dates in the archive to those in the climo database and
    // filters out dates already in the climate period of record.
    let (date_generator_handle, date_receiver, num_dates, prog_recv) =
        date_generator(arch, site, model, to_climo_db.clone(), quit_sender.clone())?;

    let prog_handle = progress_indicator(prog_recv, num_dates, site, model);

    // Set up a thread to load the files as strings
    let (file_loader_handle, string_receiver) =
        file_loader(arch, site, model, date_receiver, quit_sender.clone());

    // Set up a thread to parse the strings into soundings
    let (parser_handle, anal_receiver) =
        sounding_parser(string_receiver, model, quit_sender.clone());

    let (inv_handle, anal_receiver) =
        inventory_updater(anal_receiver, to_climo_db.clone(), quit_sender.clone());

    let (fire_stats_handle, anal_receiver) =
        fire_stats_calculator(anal_receiver, to_climo_db.clone(), quit_sender.clone());

    let (locations_handle, anal_receiver) =
        location_updater(anal_receiver, to_climo_db.clone(), quit_sender.clone());

    loop {
        select!{
            recv(quit_receiver, msg) => {
                if let Some(msg) = msg {
                    Err(msg)?;
                }
            },
            recv(anal_receiver, msg) => {
                match msg {
                    Some(_) =>{},
                    None => break,
                }
            },
        }
    }

    // Clean up
    drop(to_climo_db);
    drop(quit_sender);
    climo_handle.join().unwrap();
    date_generator_handle.join().unwrap();
    prog_handle.join().unwrap();
    file_loader_handle.join().unwrap();
    parser_handle.join().unwrap();
    inv_handle.join().unwrap();
    fire_stats_handle.join().unwrap();
    locations_handle.join().unwrap();

    println!("Completed {} for {} successfully.", model, site);

    Ok(())
}

fn date_generator(
    arch: &Archive,
    site: &str,
    model: Model,
    to_climo_db: Sender<DBRequest>,
    quitter: Sender<Error>,
) -> Result<
    (
        JoinHandle<()>,
        Receiver<NaiveDateTime>,
        usize,
        Receiver<usize>,
    ),
    Error,
> {
    let (sender, receiver) = crossbeam_channel::bounded::<NaiveDateTime>(256);
    let (prog_sender, prog_receiver) = crossbeam_channel::bounded::<usize>(256);

    let init_times = arch.get_init_times(site, model)?;
    let total = init_times.len();

    let handle = thread::spawn(move || {
        // Get the period of climate data currently in the database.
        let (send_to_me, from_climo_db) = crossbeam_channel::bounded::<DBResponse>(1);
        let req = DBRequest::GetClimoDateRange(send_to_me);
        to_climo_db.send(req);

        let (start, end) =
            if let Some(DBResponse::ClimoDateRange(start, end)) = from_climo_db.recv() {
                (start, end)
            } else {
                quitter.send(format_err!("Invalid response from climo database."));
                return;
            };

        for (i, init_time) in init_times
            .into_iter()
            .enumerate()
            .filter(|&(_, init_time)| init_time < start || init_time > end)
        {
            sender.send(init_time);
            prog_sender.send(i);
        }
    });

    Ok((handle, receiver, total, prog_receiver))
}

fn file_loader(
    arch: &Archive,
    site: &str,
    model: Model,
    init_times: Receiver<NaiveDateTime>,
    quitter: Sender<Error>,
) -> (JoinHandle<()>, Receiver<String>) {
    let root = arch.get_root().to_owned();
    let (sender, receiver) = crossbeam_channel::bounded::<String>(256);
    let site = site.to_owned();

    let handle = thread::spawn(move || {
        let arch = match Archive::connect(&root) {
            Ok(arch) => arch,
            Err(err) => {
                quitter.send(Error::from(err));
                return;
            }
        };

        for init_time in init_times {
            match arch.get_file(&site, model, &init_time) {
                Ok(string_data) => sender.send(string_data),
                Err(err) => {
                    quitter.send(Error::from(err));
                    return;
                }
            }
        }
    });

    (handle, receiver)
}

fn sounding_parser(
    strings: Receiver<String>,
    model: Model,
    quitter: Sender<Error>,
) -> (JoinHandle<()>, Receiver<Analysis>) {
    let (sender, receiver) = crossbeam_channel::bounded::<Analysis>(256);

    let handle = thread::spawn(move || {
        for string in strings {
            let bufkit_data = match BufkitData::new(&string) {
                Ok(bufkit_data) => bufkit_data,
                Err(err) => {
                    quitter.send(err);
                    return;
                }
            };

            for anal in bufkit_data
                .into_iter()
                .take(model.hours_between_runs() as usize)
            {
                // Check to make sure it has a valid time
                if anal.sounding().get_valid_time().is_none() {
                    continue;
                }

                sender.send(anal);
            }
        }
    });

    (handle, receiver)
}

fn inventory_updater(
    anals: Receiver<Analysis>,
    to_climo_db: Sender<DBRequest>,
    quitter: Sender<Error>,
) -> (JoinHandle<()>, Receiver<Analysis>) {
    let (sender, receiver) = crossbeam_channel::bounded::<Analysis>(256);

    let handle = thread::spawn(move || {
        // Get the period of climate data currently in the database.
        let (send_to_me, from_climo_db) = crossbeam_channel::bounded::<DBResponse>(1);
        let req = DBRequest::GetClimoDateRange(send_to_me);
        to_climo_db.send(req);

        let (mut start, mut end) =
            if let Some(DBResponse::ClimoDateRange(start, end)) = from_climo_db.recv() {
                (start, end)
            } else {
                quitter.send(format_err!("Invalid response from climo database."));
                return;
            };

        // If they are equal, there was none in the db, set them to something ridiculous that
        // will be overwritten immediately
        if start == end {
            start = NaiveDate::from_ymd(3000, 12, 31).and_hms(0, 0, 0);
            end = NaiveDate::from_ymd(1900, 1, 1).and_hms(0, 0, 0);
        }

        for anal in anals {
            // unwrap is safe because we checked this in sounding parser
            let valid_time = anal.sounding().get_valid_time().unwrap();

            if valid_time < start {
                start = valid_time;
            }

            if valid_time > end {
                end = valid_time;
            }

            sender.send(anal);
        }

        // update the database
        if start < end {
            let req = DBRequest::UpdateInventory(start, end);
            to_climo_db.send(req);
        }
    });

    (handle, receiver)
}

fn fire_stats_calculator(
    anals: Receiver<Analysis>,
    to_climo_db: Sender<DBRequest>,
    quitter: Sender<Error>,
) -> (JoinHandle<()>, Receiver<Analysis>) {
    let (anal_sender, anal_receiver) = crossbeam_channel::bounded::<Analysis>(256);

    let handle = thread::spawn(move || {
        for anal in anals {
            {
                let snd = anal.sounding();

                // unwrap safe because we checked for it in parser
                let valid_time = snd.get_valid_time().unwrap();

                let hns_low = haines_low(snd).unwrap_or(0.0) as i32;
                let hns_mid = haines_mid(snd).unwrap_or(0.0) as i32;
                let hns_high = haines_high(snd).unwrap_or(0.0) as i32;

                let hdw = match hot_dry_windy(snd) {
                    Ok(hdw) => hdw as i32,
                    Err(err) => {
                        quitter.send(Error::from(err));
                        return;
                    }
                };

                to_climo_db.send(DBRequest::AddFire(
                    valid_time,
                    (hns_high, hns_mid, hns_low),
                    hdw,
                ));
            }
            anal_sender.send(anal);
        }
    });

    (handle, anal_receiver)
}

fn location_updater(
    anals: Receiver<Analysis>,
    to_climo_db: Sender<DBRequest>,
    _quitter: Sender<Error>,
) -> (JoinHandle<()>, Receiver<Analysis>) {
    let (sender, receiver) = crossbeam_channel::bounded::<Analysis>(256);

    let handle = thread::spawn(move || {
        for anal in anals {
            if anal
                .sounding()
                .get_lead_time()
                .into_option()
                .map(|lt| lt == 0)
                .unwrap_or(true)
            {
                let snd = anal.sounding();

                // unwrap safe because we checked for it in parser
                let valid_time = snd.get_valid_time().unwrap();
                let location = snd.get_station_info().location();
                let elevation = snd.get_station_info().elevation().into_option();

                if let (Some(elev_m), Some((lat, lon))) = (elevation, location) {
                    let req = DBRequest::AddLocation(valid_time, lat, lon, elev_m);
                    to_climo_db.send(req);
                }
            }

            sender.send(anal);
        }
    });

    (handle, receiver)
}

fn progress_indicator(
    receiver: Receiver<usize>,
    total: usize,
    site: &str,
    model: Model,
) -> JoinHandle<()> {
    let site = site.to_owned();

    thread::spawn(move || {
        println!("Progress for site {} and model {}.", site, model);

        let mut pb = ProgressBar::new(total as u64);
        for inc in receiver {
            pb.set(inc as u64);
        }
        pb.finish();
    })
}
