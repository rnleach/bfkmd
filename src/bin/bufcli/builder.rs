use bufkit_data::{Archive, Model};
use chrono::NaiveDateTime;
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
    let (error_sender, error_receiver) = crossbeam_channel::bounded::<ErrorMessage>(256);

    // Start a thread to manage the climo database
    let (climo_handle, to_climo_db) =
        start_climo_db_thread(root, site, model, error_sender.clone());

    // Set up a generator that compares the dates in the archive to those in the climo database and
    // filters out dates already in the climate period of record.
    let (date_generator_handle, date_receiver, num_dates) =
        date_generator(arch, site, model, to_climo_db.clone(), error_sender.clone())?;

    // Set up a thread to load the files as strings
    let (file_loader_handle, string_receiver) =
        file_loader(arch, site, model, date_receiver, error_sender.clone());

    // Set up a thread to parse the strings into soundings
    let (parser_handle, anal_receiver) =
        sounding_parser(string_receiver, model, error_sender.clone());

    let (fire_stats_handle, anal_receiver) =
        fire_stats_calculator(anal_receiver, to_climo_db.clone(), error_sender.clone());

    let (locations_handle, anal_receiver) =
        location_updater(anal_receiver, to_climo_db.clone(), error_sender.clone());

    let (progress_handle, progress_sender) = progress_indicator(num_dates, site, model);

    // Clean up
    drop(to_climo_db);
    drop(error_sender);

    let mut data_errors = vec![];
    loop {
        select!{
            recv(error_receiver, msg) => {
                if let Some(msg) = msg {
                    match msg {
                        ErrorMessage::Critical(err) => Err(err)?,
                        ErrorMessage::DataError(init_time, err) => {
                            data_errors.push((init_time, err));
                        },
                    }
                }
            },
            recv(anal_receiver, msg) => {
                match msg {
                    Some((i, _, _)) =>{
                        progress_sender.send(i);
                    },
                    None => break,
                }
            },
        }
    }
    drop(progress_sender);

    // Clean up
    climo_handle.join().unwrap();
    date_generator_handle.join().unwrap();
    file_loader_handle.join().unwrap();
    parser_handle.join().unwrap();
    fire_stats_handle.join().unwrap();
    locations_handle.join().unwrap();
    progress_handle.join().unwrap();

    if !data_errors.is_empty() {
        println!("Errors for {} {}.", site, model);
    }
    for (init_time, err) in data_errors {
        println!("     {} - {}", init_time, err);
    }

    println!("Completed {} for {} successfully.", model, site);

    Ok(())
}

pub enum ErrorMessage {
    Critical(Error),
    DataError(NaiveDateTime, Error),
}

fn date_generator(
    arch: &Archive,
    site: &str,
    model: Model,
    to_climo_db: Sender<DBRequest>,
    error_stream: Sender<ErrorMessage>,
) -> Result<(JoinHandle<()>, Receiver<(usize, NaiveDateTime)>, usize), Error> {
    let (sender, receiver) = crossbeam_channel::bounded::<(usize, NaiveDateTime)>(256);

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
                error_stream.send(ErrorMessage::Critical(format_err!(
                    "Invalid response from climo database."
                )));
                return;
            };

        for (i, init_time) in init_times
            .into_iter()
            .enumerate()
            .filter(|&(_, init_time)| init_time < start || init_time > end)
        {
            sender.send((i, init_time));
        }
    });

    Ok((handle, receiver, total))
}

fn file_loader(
    arch: &Archive,
    site: &str,
    model: Model,
    init_times: Receiver<(usize, NaiveDateTime)>,
    error_stream: Sender<ErrorMessage>,
) -> (JoinHandle<()>, Receiver<(usize, NaiveDateTime, String)>) {
    let root = arch.get_root().to_owned();
    let (sender, receiver) = crossbeam_channel::bounded::<(usize, NaiveDateTime, String)>(256);
    let site = site.to_owned();

    let handle = thread::spawn(move || {
        let arch = match Archive::connect(&root) {
            Ok(arch) => arch,
            Err(err) => {
                error_stream.send(ErrorMessage::Critical(Error::from(err)));
                return;
            }
        };

        for (i, init_time) in init_times {
            match arch.get_file(&site, model, &init_time) {
                Ok(string_data) => sender.send((i, init_time, string_data)),
                Err(err) => {
                    error_stream.send(ErrorMessage::DataError(init_time, Error::from(err)));
                }
            }
        }
    });

    (handle, receiver)
}

fn sounding_parser(
    strings: Receiver<(usize, NaiveDateTime, String)>,
    model: Model,
    error_stream: Sender<ErrorMessage>,
) -> (JoinHandle<()>, Receiver<(usize, NaiveDateTime, Analysis)>) {
    let (sender, receiver) = crossbeam_channel::bounded::<(usize, NaiveDateTime, Analysis)>(256);

    let handle = thread::spawn(move || {
        for (i, init_time, string) in strings {
            let bufkit_data = match BufkitData::new(&string) {
                Ok(bufkit_data) => bufkit_data,
                Err(err) => {
                    error_stream.send(ErrorMessage::DataError(init_time, err));
                    continue;
                }
            };

            for anal in bufkit_data
                .into_iter()
                .take(model.hours_between_runs() as usize)
            {
                if let Some(valid_time) = anal.sounding().get_valid_time() {
                    sender.send((i, valid_time, anal));
                } else {
                    error_stream.send(ErrorMessage::DataError(
                        init_time,
                        format_err!("No valid time."),
                    ));
                    continue;
                }
            }
        }
    });

    (handle, receiver)
}

fn fire_stats_calculator(
    anals: Receiver<(usize, NaiveDateTime, Analysis)>,
    to_climo_db: Sender<DBRequest>,
    error_stream: Sender<ErrorMessage>,
) -> (JoinHandle<()>, Receiver<(usize, NaiveDateTime, Analysis)>) {
    let (anal_sender, anal_receiver) =
        crossbeam_channel::bounded::<(usize, NaiveDateTime, Analysis)>(256);

    let handle = thread::spawn(move || {
        for (i, valid_time, anal) in anals {
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
                        error_stream.send(ErrorMessage::DataError(valid_time, Error::from(err)));
                        continue;
                    }
                };

                to_climo_db.send(DBRequest::AddFire(
                    valid_time,
                    (hns_high, hns_mid, hns_low),
                    hdw,
                ));
            }
            anal_sender.send((i, valid_time, anal));
        }
    });

    (handle, anal_receiver)
}

fn location_updater(
    anals: Receiver<(usize, NaiveDateTime, Analysis)>,
    to_climo_db: Sender<DBRequest>,
    _error_stream: Sender<ErrorMessage>,
) -> (JoinHandle<()>, Receiver<(usize, NaiveDateTime, Analysis)>) {
    let (sender, receiver) = crossbeam_channel::bounded::<(usize, NaiveDateTime, Analysis)>(256);

    let handle = thread::spawn(move || {
        for (i, valid_time, anal) in anals {
            if anal
                .sounding()
                .get_lead_time()
                .into_option()
                .map(|lt| lt == 0)
                .unwrap_or(true)
            {
                let snd = anal.sounding();

                let location = snd.get_station_info().location();
                let elevation = snd.get_station_info().elevation().into_option();

                if let (Some(elev_m), Some((lat, lon))) = (elevation, location) {
                    let req = DBRequest::AddLocation(valid_time, lat, lon, elev_m);
                    to_climo_db.send(req);
                }
            }

            sender.send((i, valid_time, anal));
        }
    });

    (handle, receiver)
}

fn progress_indicator(total: usize, site: &str, model: Model) -> (JoinHandle<()>, Sender<usize>) {
    let site = site.to_owned();
    let (sender, receiver) = crossbeam_channel::bounded::<usize>(256);

    let handle = thread::spawn(move || {
        println!("Progress for site {} and model {}.", site, model);

        let mut pb = ProgressBar::new(total as u64);
        for inc in receiver {
            pb.set(inc as u64);
        }
        pb.finish();
    });

    (handle, sender)
}
