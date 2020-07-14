use super::{ReqInfo, StepResult};
use bufkit_data::{Archive, Model, SiteInfo};
use crossbeam_channel as channel;
use sounding_bufkit::BufkitData;
use std::{collections::HashSet, error::Error, fs::File, io::Write, path::PathBuf, thread::spawn};

pub fn start_writer_thread(
    root: PathBuf,
    curr_dir: Option<PathBuf>,
    save_rx: channel::Receiver<StepResult>,
    save_tx: channel::Sender<StepResult>,
) {
    spawn(move || {
        let arch = match Archive::connect(&root) {
            Ok(arch) => arch,
            Err(err) => {
                save_tx
                    .send(StepResult::IntializationError(err.to_string()))
                    .expect("save_tx error sending.");
                return;
            }
        };

        let mut updated_sites: HashSet<SaveLatestInfo> = HashSet::new();

        for step_result in save_rx {
            let next_step = match step_result {
                StepResult::BufkitFileAsString(req_info, data) => {
                    match BufkitData::init(&data, "") {
                        Ok(bufkit_data) => {
                            updated_sites.insert(SaveLatestInfo::from(&req_info));

                            let init_time_res = bufkit_data
                                .into_iter()
                                .next()
                                .ok_or("No soundings in file")
                                .and_then(|anal| anal.0.valid_time().ok_or("Missing valid time"));
                            let end_time_res = bufkit_data
                                .into_iter()
                                .last()
                                .ok_or("No soundings in file")
                                .and_then(|anal| anal.0.valid_time().ok_or("Missing valid time"));

                            init_time_res
                                .and_then(|init_time| {
                                    end_time_res.map(|end_time| (init_time, end_time))
                                })
                                .map_err(|err| {
                                    StepResult::ParseError(req_info.clone(), err.to_string())
                                })
                                .and_then(|(init_time, end_time)| {
                                    let ReqInfo {
                                        ref site,
                                        ref model,
                                        ..
                                    } = &req_info;

                                    arch.add(&site, *model, init_time, end_time, &data).map_err(
                                        |err| {
                                            StepResult::ArchiveError(
                                                req_info.clone(),
                                                err.to_string(),
                                            )
                                        },
                                    )
                                })
                                .map(|_| StepResult::Success(req_info))
                                .unwrap_or_else(|err| err)
                        }
                        Err(err) => StepResult::ParseError(req_info, err.to_string()),
                    }
                }
                _ => step_result,
            };

            save_tx.send(next_step).expect("save_tx error sending.");
        }

        match save_latest(&arch, curr_dir, updated_sites) {
            Ok(()) => {}
            Err(err) => {
                save_tx
                    .send(StepResult::ErrorSavingCurrent(err.to_string()))
                    .expect("save_tx error sending.");
            }
        }
    });
}

fn save_latest(
    arch: &Archive,
    save_dir: Option<PathBuf>,
    updated_sites: HashSet<SaveLatestInfo>,
) -> Result<(), Box<dyn Error>> {
    let save_dir = if let Some(save_dir) = save_dir {
        save_dir
    } else {
        return Ok(());
    };

    for SaveLatestInfo { site, model } in updated_sites.iter() {
        let buf = match arch.most_recent_file(site, *model) {
            Ok(data_str) => data_str,
            Err(err) => {
                println!(
                    "Unable to save latest data for {} at {} with error: {}.",
                    model.as_static_str(),
                    site.id.as_deref().unwrap_or("None"),
                    err,
                );
                continue;
            }
        };

        let site_id = if let Some(ref site_id) = site.id {
            site_id
        } else {
            continue;
        };

        let fname = format!("{}_{}.buf", site_id, model.as_static_str());
        let mut path = PathBuf::from(&save_dir);
        path.push(fname);

        match File::create(path) {
            Ok(mut f) => {
                let _ = f.write_all(buf.as_bytes());
            }
            Err(err) => println!("Error writing to file {:?}", err.to_string()),
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct SaveLatestInfo {
    site: Site,
    model: Model,
}

impl From<&ReqInfo> for SaveLatestInfo {
    fn from(req: &ReqInfo) -> SaveLatestInfo {
        let ReqInfo {
            ref site,
            ref model,
            ..
        } = req;
        SaveLatestInfo {
            site: site.clone(),
            model: *model,
        }
    }
}
