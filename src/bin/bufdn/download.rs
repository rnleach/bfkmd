use super::{ReqInfo, StepResult};
use crossbeam_channel as channel;
use reqwest::{blocking::Client, StatusCode};
use std::{io::Read, thread::spawn};

pub fn start_download_threads(
    dl_rx: channel::Receiver<StepResult>,
    dl_tx: channel::Sender<StepResult>,
) -> () {
    let make_download_thread = || -> () {
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
