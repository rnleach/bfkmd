use super::{ReqInfo, StepResult};
use bufkit_data::{AddFileResult, Archive, Model};
use crossbeam_channel as channel;
use std::{path::PathBuf, thread::spawn};

pub fn start_writer_thread(
    root: PathBuf,
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

        for step_result in save_rx {
            let next_step = match step_result {
                StepResult::BufkitFileAsString(req_info, data) => {
                    match arch.add(&req_info.site_id, req_info.model, &data) {
                        AddFileResult::Ok(_) | AddFileResult::New(_) => {
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
    });
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
