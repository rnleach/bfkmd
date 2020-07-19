use super::{ReqInfo, StepResult};
use bufkit_data::{AddFileResult, Archive, Model};
use crossbeam_channel as channel;
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
                    .send(StepResult::InitializationError(err.to_string()))
                    .expect("save_tx error sending.");
                return;
            }
        };

        let mut updated_sites: HashSet<SaveLatestInfo> = HashSet::new();

        for step_result in save_rx {
            let next_step = match step_result {
                StepResult::BufkitFileAsString(req_info, data) => {
                    match arch.add(&req_info.site_id, req_info.model, &data) {
                        AddFileResult::Ok(_) | AddFileResult::New(_) => {
                            updated_sites.insert(SaveLatestInfo::from(&req_info));
                            StepResult::Success(req_info)
                        }
                        AddFileResult::Error(err) => {
                            StepResult::ArchiveError(req_info, err.to_string())
                        }
                        AddFileResult::IdMovedStation { old, new } => StepResult::StationIdMoved {
                            info: req_info,
                            old,
                            new,
                        },
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

    for SaveLatestInfo { site_id, model } in updated_sites.into_iter() {
        // HACK FOR KNOWN ISSUES WITH ONLINE ARCHIVE
        let site_id = match (site_id.as_ref(), model) {
            ("KDLN", Model::NAM) | ("KDLN", Model::NAM4KM) => "KLDN",
            (_, _) => site_id.as_ref(),
        };

        let buf = match arch
            .station_num_for_id(site_id, model)
            .and_then(|site| arch.retrieve_most_recent(site, model))
        {
            Ok(data_str) => data_str,
            Err(err) => {
                println!(
                    "Unable to save latest data for {} at {} with error: {}.",
                    model.as_static_str(),
                    site_id,
                    err,
                );
                continue;
            }
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
    site_id: String,
    model: Model,
}

impl From<&ReqInfo> for SaveLatestInfo {
    fn from(req: &ReqInfo) -> SaveLatestInfo {
        let ReqInfo {
            ref model,
            ref site_id,
            ..
        } = req;
        SaveLatestInfo {
            site_id: site_id.clone(),
            model: *model,
        }
    }
}
