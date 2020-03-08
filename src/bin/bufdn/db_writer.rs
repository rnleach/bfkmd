use super::{ReqInfo, StepResult};
use bufkit_data::Archive;
use crossbeam_channel as channel;
use sounding_bufkit::BufkitData;
use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    thread::spawn,
};

pub fn start_writer_thread(
    root: PathBuf,
    curr_dir: Option<PathBuf>,
    save_rx: channel::Receiver<StepResult>,
    save_tx: channel::Sender<StepResult>,
) -> () {
    spawn(move || -> () {
        let arch = match Archive::connect(&root) {
            Ok(arch) => arch,
            Err(err) => {
                save_tx
                    .send(StepResult::IntializationError(err.to_string()))
                    .expect("save_tx error sending.");
                return;
            }
        };

        for step_result in save_rx {
            let next_step = match step_result {
                StepResult::BufkitFileAsString(req_info, data) => {
                    match BufkitData::init(&data, "") {
                        Ok(bufkit_data) => {
                            // Save to the local file system.
                            if let Some(ref curr_dir) = curr_dir {
                                save_uncompressed(&req_info, curr_dir, &data);
                            }

                            let init_time_res = bufkit_data
                                .into_iter()
                                .nth(0)
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
                                    arch.add(site, *model, init_time, end_time, &data).map_err(
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
    });
}

fn save_uncompressed(req_info: &ReqInfo, curr_dir: &Path, buf: &str) {
    let fname = format!("{}_{}.buf", &req_info.site, req_info.model.as_static_str());
    let mut path = PathBuf::from(curr_dir);
    path.push(fname);

    match File::create(path) {
        Ok(mut f) => {
            let _ = f.write_all(buf.as_bytes());
        }
        Err(err) => println!("Error writing to file {:?}", err.to_string()),
    }
}
