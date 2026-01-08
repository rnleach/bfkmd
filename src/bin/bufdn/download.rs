use super::{ReqInfo, StepResult};
use crossbeam_channel as channel;
use reqwest::{blocking::Client, Url, StatusCode};
use std::{io::Read, thread::spawn, fs};

pub fn start_download_threads(
    dl_rx: channel::Receiver<StepResult>,
    dl_tx: channel::Sender<StepResult>,
) {
    let make_download_thread = || {
        let dl_rx = dl_rx.clone();
        let dl_tx = dl_tx.clone();

        spawn(move || {
            let client = Client::new();

            for step_result in dl_rx {
                let next_step = match step_result {
                    StepResult::Request(req_info) => {
                        let ReqInfo { ref url, .. } = req_info;

                        match client.get(url).send() {
                            Ok(ref mut response) => match response.status() {
                                StatusCode::OK => {
                                    let mut buffer = String::new();
                                    match response.read_to_string(&mut buffer) {
                                        Ok(_) => StepResult::BufkitFileAsString(req_info, buffer),
                                        Err(err) => StepResult::OtherDownloadError(
                                            req_info,
                                            err.to_string(),
                                        ),
                                    }
                                }
                                StatusCode::NOT_FOUND => StepResult::URLNotFound(req_info),
                                code => StepResult::OtherURLStatus(req_info, code),
                            },
                            Err(err) => StepResult::OtherDownloadError(req_info, err.to_string()),
                        }
                    }
                    StepResult::Local(req_info) => {
                        let ReqInfo { ref url, .. } = req_info;
                        match Url::parse(url).map_err(|_| ()).and_then(|u| u.to_file_path()) {
                            Ok(path) => {
                                match fs::read_to_string(&path) {
                                    Ok(buffer) => StepResult::BufkitFileAsString(req_info, buffer),
                                    Err(e) => StepResult::FileNameParseError(format!("Unable to load local file {} : {}",
                                            path.display(), e)),
                                }
                            }
                            Err(_) => StepResult::FileNameParseError(String::from("Unable to decode file url"))
                        }
                    }
                    _ => step_result,
                };

                dl_tx.send(next_step).expect("dl_tx error sending.");
            }
        });
    };

    // The file download threads
    for _ in 0..3 {
        make_download_thread();
    }
}
