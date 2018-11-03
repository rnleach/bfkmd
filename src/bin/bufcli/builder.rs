use super::CmdLineArgs;
use bufkit_data::{Archive, BufkitDataErr, Model, Site};
use chrono::NaiveDateTime;
use climo_db::{ClimoDB, ClimoDBInterface};
use crossbeam_channel::{self as channel, Receiver, Sender};
use failure::Error;
use pbr::ProgressBar;
use sounding_analysis::{haines_high, haines_low, haines_mid, hot_dry_windy, Analysis};
use sounding_bufkit::BufkitData;
use std::path::Path;
use std::thread::{self, JoinHandle};
use strum::AsStaticRef;

const CAPACITY: usize = 16;

pub(crate) fn build_climo(args: CmdLineArgs) -> Result<(), Error> {
    let root = &args.root.clone();
    let force_rebuild = args.operation == "build";

    // Channels for the main pipeline
    let (entry_point_snd, filter_requests_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);
    let (load_requests_snd, load_requests_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);
    let (parse_requests_snd, parse_requests_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);
    let (fire_requests_snd, fire_requests_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);
    let (loc_requests_snd, loc_requests_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);
    let (comp_notify_snd, comp_notify_rcv) = channel::bounded::<PipelineMessage>(CAPACITY);

    // Channel for adding stats to the climo database
    let (stats_snd, stats_rcv) = channel::bounded::<StatsMessage>(CAPACITY);

    // Channel for sending un-parseable files info to archive so it can remove them
    let (parse_err_snd, parse_err_rcv) = channel::bounded::<ErrorMessage>(CAPACITY);

    // Channel for general error messages
    let (error_snd, error_rcv) = channel::bounded::<ErrorMessage>(CAPACITY);

    // Hook everything together
    let (ep_jh, total_num) = start_entry_point_thread(args, entry_point_snd, error_snd.clone())?;

    let fltr_jh = start_filter_thread(
        root,
        force_rebuild,
        filter_requests_rcv,
        load_requests_snd,
        error_snd.clone(),
    )?;

    let load_jh = start_load_thread(
        root,
        load_requests_rcv,
        parse_requests_snd,
        error_snd.clone(),
    )?;

    let parser_jh = start_parser_thread(
        parse_requests_rcv,
        fire_requests_snd,
        parse_err_snd,
        error_snd.clone(),
    )?;

    let fire_stats_jh = start_fire_stats_thread(
        fire_requests_rcv,
        loc_requests_snd,
        stats_snd.clone(),
        error_snd.clone(),
    )?;

    let loc_stats_jh = start_location_stats_thread(
        loc_requests_rcv,
        comp_notify_snd,
        stats_snd,
        error_snd.clone(),
    )?;

    let parse_err_jh = start_parse_err_handler(root, parse_err_rcv, error_snd.clone())?;
    let stats_jh = start_stats_thread(root, stats_rcv, error_snd)?;

    // Monitor progress and post updates here
    let mut data_errors = vec![];
    let mut pipeline_done = false;
    let mut error_done = false;
    let mut pb = ProgressBar::new(total_num as u64);
    loop {
        select!{
            recv(error_rcv, msg) => {
                if let Some(msg) = msg {
                    match msg {
                        ErrorMessage::Critical(err) => Err(err)?,
                        ErrorMessage::DataError(site, model, valid_time, err) => {
                            data_errors.push((site, model, valid_time, err));
                        },
                    }
                } else {
                    error_done = true;
                }
            },
            recv(comp_notify_rcv, msg) => {
                match msg {
                    Some(PipelineMessage::Completed{num}) =>{
                        pb.set(num as u64);
                    },
                    None => pipeline_done = true,
                    _ => unreachable!(),
                }
            },
        }

        // Everything is in, so send the terminate message to the climo_db. Everything else should
        // shut down on it's own by this point.
        if pipeline_done && error_done {
            pb.finish();
            break;
        }
    }

    // Print any error messages
    if !data_errors.is_empty() {
        println!("Data errors during processing: {}.", data_errors.len());
    }
    for (site, model, valid_time, err) in data_errors {
        println!(
            "     {} - {} - {} - {}",
            site.id,
            model.as_static(),
            valid_time,
            err
        );
    }

    println!("Waiting for service threads to shut down.");
    ep_jh.join().expect("Error joining thread.");
    fltr_jh.join().expect("Error joining thread.");
    load_jh.join().expect("Error joining thread.");
    parser_jh.join().expect("Error joining thread.");
    fire_stats_jh.join().expect("Error joining thread.");
    loc_stats_jh.join().expect("Error joining thread.");
    parse_err_jh.join().expect("Error joining thread.");
    stats_jh.join().expect("Error joining thread.");
    println!("Done.");

    Ok(())
}

macro_rules! check_bail {
    ($res:expr, $err_send:ident) => {
        match $res {
            Ok(val) => val,
            Err(err) => {
                let message = ErrorMessage::Critical(err.into());
                $err_send.send(message);
                return;
            }
        }
    };
}

fn start_entry_point_thread(
    args: CmdLineArgs,
    entry_point_snd: Sender<PipelineMessage>,
    error_snd: Sender<ErrorMessage>,
) -> Result<(JoinHandle<()>, i64), Error> {
    let arch = Archive::connect(&args.root)?;
    let sites = args
        .sites
        .iter()
        .map(|s| arch.site_info(s))
        .collect::<Result<Vec<Site>, BufkitDataErr>>()?;

    let mut total = 0;
    for (site, &model) in iproduct!(&sites, &args.models) {
        total += arch.count_init_times(&site.id, model)?;
    }

    let jh = thread::Builder::new()
        .name("Generator".to_string())
        .spawn(move || {
            let arch = check_bail!(Archive::connect(&args.root), error_snd);

            let mut counter = 0;
            for (site, &model) in iproduct!(&sites, &args.models) {
                let init_times = check_bail!(arch.init_times(&site.id, model), error_snd);

                for init_time in init_times {
                    counter += 1;

                    let message = PipelineMessage::Filter {
                        model,
                        init_time,
                        site: site.clone(),
                        num: counter,
                    };
                    entry_point_snd.send(message);
                }
            }
        })?;

    Ok((jh, total))
}

fn start_filter_thread(
    root: &Path,
    force_rebuild: bool,
    filter_requests_rcv: Receiver<PipelineMessage>,
    load_requests_snd: Sender<PipelineMessage>,
    err_snd: Sender<ErrorMessage>,
) -> Result<JoinHandle<()>, Error> {
    let root = root.to_path_buf();

    let jh = thread::Builder::new()
        .name("FilterDates".to_string())
        .spawn(move || {
            let climo_db = check_bail!(ClimoDB::connect_or_create(&root), err_snd);
            let mut climo_db = check_bail!(ClimoDBInterface::initialize(&climo_db), err_snd);

            for msg in filter_requests_rcv {
                if let PipelineMessage::Filter {
                    num,
                    site,
                    model,
                    init_time,
                } = msg
                {
                    if !check_bail!(climo_db.exists(&site, model, init_time), err_snd)
                        || force_rebuild
                    {
                        let msg = PipelineMessage::Load {
                            num,
                            site,
                            model,
                            init_time,
                        };
                        load_requests_snd.send(msg);
                    }
                } else {
                    unreachable!();
                }
            }
        })?;

    Ok(jh)
}

fn start_load_thread(
    root: &Path,
    load_requests_rcv: Receiver<PipelineMessage>,
    parse_requests_snd: Sender<PipelineMessage>,
    err_snd: Sender<ErrorMessage>,
) -> Result<JoinHandle<()>, Error> {
    let root = root.to_path_buf();

    let jh = thread::Builder::new()
        .name("FileLoader".to_string())
        .spawn(move || {
            let arch = check_bail!(Archive::connect(root), err_snd);

            for load_req in load_requests_rcv {
                if let PipelineMessage::Load {
                    num,
                    site,
                    model,
                    init_time,
                } = load_req
                {
                    match arch.retrieve(&site.id, model, &init_time) {
                        Ok(data) => {
                            let message = PipelineMessage::Parse {
                                num,
                                site,
                                model,
                                init_time,
                                data,
                            };
                            parse_requests_snd.send(message);
                        }
                        Err(err) => {
                            let message =
                                ErrorMessage::DataError(site, model, init_time, Error::from(err));
                            err_snd.send(message);
                        }
                    }
                } else {
                    unreachable!();
                }
            }
        })?;

    Ok(jh)
}

fn start_parser_thread(
    parse_requests: Receiver<PipelineMessage>,
    fire_requests: Sender<PipelineMessage>,
    parse_errors: Sender<ErrorMessage>,
    err_snd: Sender<ErrorMessage>,
) -> Result<JoinHandle<()>, Error> {
    let jh = thread::Builder::new()
        .name("SoundingParser".to_string())
        .spawn(move || {
            for msg in parse_requests {
                if let PipelineMessage::Parse {
                    num,
                    site,
                    model,
                    init_time,
                    data,
                } = msg
                {
                    let bufkit_data = match BufkitData::new(&data) {
                        Ok(bufkit_data) => bufkit_data,
                        Err(err) => {
                            let message =
                                ErrorMessage::DataError(site, model, init_time, Error::from(err));
                            parse_errors.send(message);
                            continue;
                        }
                    };

                    for anal in bufkit_data.into_iter().take_while(|anal| {
                        anal.sounding()
                            .get_lead_time()
                            .into_option()
                            .and_then(|lt| Some((lt as i64) < model.hours_between_runs()))
                            .unwrap_or(false)
                    }) {
                        if let Some(valid_time) = anal.sounding().get_valid_time() {
                            let message = PipelineMessage::Fire {
                                num,
                                site: site.clone(),
                                model,
                                valid_time,
                                anal,
                            };
                            fire_requests.send(message);
                        } else {
                            let message = ErrorMessage::DataError(
                                site.clone(),
                                model,
                                init_time,
                                format_err!("No valid time."),
                            );
                            err_snd.send(message);
                        }
                    }
                } else {
                    unreachable!();
                }
            }
        })?;

    Ok(jh)
}

fn start_fire_stats_thread(
    fire_requests: Receiver<PipelineMessage>,
    location_requests: Sender<PipelineMessage>,
    climo_update_requests: Sender<StatsMessage>,
    err_snd: Sender<ErrorMessage>,
) -> Result<JoinHandle<()>, Error> {
    let jh = thread::Builder::new()
        .name("FireStatsCalc".to_string())
        .spawn(move || {
            for msg in fire_requests {
                if let PipelineMessage::Fire {
                    num,
                    site,
                    model,
                    valid_time,
                    anal,
                } = msg
                {
                    {
                        let snd = anal.sounding();

                        let hns_low = haines_low(snd).unwrap_or(0.0) as i32;
                        let hns_mid = haines_mid(snd).unwrap_or(0.0) as i32;
                        let hns_high = haines_high(snd).unwrap_or(0.0) as i32;

                        let hdw = match hot_dry_windy(snd) {
                            Ok(hdw) => hdw as i32,
                            Err(err) => {
                                let message = ErrorMessage::DataError(
                                    site,
                                    model,
                                    valid_time,
                                    Error::from(err),
                                );
                                err_snd.send(message);
                                continue;
                            }
                        };

                        let message = StatsMessage::Fire {
                            site: site.clone(),
                            model,
                            valid_time,
                            hns_low,
                            hns_mid,
                            hns_high,
                            hdw,
                        };
                        climo_update_requests.send(message);
                    }

                    let message = PipelineMessage::Location {
                        num,
                        site,
                        model,
                        valid_time,
                        anal,
                    };
                    location_requests.send(message);
                } else {
                    unreachable!();
                }
            }
        })?;

    Ok(jh)
}

fn start_location_stats_thread(
    location_requests: Receiver<PipelineMessage>,
    completed_notification: Sender<PipelineMessage>,
    climo_update_requests: Sender<StatsMessage>,
    err_snd: Sender<ErrorMessage>,
) -> Result<JoinHandle<()>, Error> {
    let jh = thread::Builder::new()
        .name("LocationUpdater".to_string())
        .spawn(move || {
            for msg in location_requests {
                if let PipelineMessage::Location {
                    num,
                    site,
                    model,
                    valid_time,
                    anal,
                } = msg
                {
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
                            let message = StatsMessage::Location {
                                site,
                                model,
                                valid_time,
                                lat,
                                lon,
                                elev_m,
                            };
                            climo_update_requests.send(message);
                        } else {
                            let message = ErrorMessage::DataError(
                                site,
                                model,
                                valid_time,
                                format_err!("Missing location information."),
                            );
                            err_snd.send(message);
                        }
                    }

                    let message = PipelineMessage::Completed { num };
                    completed_notification.send(message);
                } else {
                    unreachable!();
                }
            }
        })?;

    Ok(jh)
}

fn start_parse_err_handler(
    root: &Path,
    parse_err_rcv: Receiver<ErrorMessage>,
    err_snd: Sender<ErrorMessage>,
) -> Result<JoinHandle<()>, Error> {
    let root = root.to_path_buf();

    let jh = thread::Builder::new()
        .name("ParseErrorHandler".to_string())
        .spawn(move || {
            let arch = check_bail!(Archive::connect(root), err_snd);

            for msg in parse_err_rcv {
                if let ErrorMessage::DataError(site, model, init_time, _err) = &msg {
                    check_bail!(arch.remove(&site.id, *model, init_time), err_snd);
                }

                // Forward message to the regular error channel.
                err_snd.send(msg);
            }
        })?;

    Ok(jh)
}

fn start_stats_thread(
    root: &Path,
    stats_rcv: Receiver<StatsMessage>,
    err_snd: Sender<ErrorMessage>,
) -> Result<JoinHandle<()>, Error> {
    let root = root.to_path_buf();

    let jh = thread::Builder::new()
        .name("ClimoWriter".to_string())
        .spawn(move || {
            let climo_db = check_bail!(ClimoDB::connect_or_create(&root), err_snd);
            let mut climo_db = check_bail!(ClimoDBInterface::initialize(&climo_db), err_snd);

            for msg in stats_rcv {
                let res = match msg {
                    StatsMessage::Fire {
                        site,
                        model,
                        valid_time,
                        hns_low,
                        hns_mid,
                        hns_high,
                        hdw,
                    } => climo_db.add_fire(
                        &site,
                        model,
                        valid_time,
                        (hns_high, hns_mid, hns_low),
                        hdw,
                    ),
                    StatsMessage::Location {
                        site,
                        model,
                        valid_time,
                        lat,
                        lon,
                        elev_m,
                    } => climo_db.add_location(&site, model, valid_time, lat, lon, elev_m),
                };

                if let Err(err) = res {
                    let message = ErrorMessage::Critical(Error::from(err));
                    err_snd.send(message);
                    return;
                }
            }
        })?;

    Ok(jh)
}

#[derive(Debug)]
enum PipelineMessage {
    Filter {
        num: usize,
        site: Site,
        model: Model,
        init_time: NaiveDateTime,
    },
    Load {
        num: usize,
        site: Site,
        model: Model,
        init_time: NaiveDateTime,
    },
    Parse {
        num: usize,
        site: Site,
        model: Model,
        init_time: NaiveDateTime,
        data: String,
    },
    Fire {
        num: usize,
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        anal: Analysis,
    },
    Location {
        num: usize,
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        anal: Analysis,
    },
    Completed {
        num: usize,
    },
}

#[derive(Clone, Debug)]
enum StatsMessage {
    Fire {
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        hns_low: i32,
        hns_mid: i32,
        hns_high: i32,
        hdw: i32,
    },
    Location {
        site: Site,
        model: Model,
        valid_time: NaiveDateTime,
        lat: f64,
        lon: f64,
        elev_m: f64,
    },
}

#[derive(Debug)]
enum ErrorMessage {
    Critical(Error),
    DataError(Site, Model, NaiveDateTime, Error),
}
