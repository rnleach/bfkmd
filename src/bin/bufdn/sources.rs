use super::ReqInfo;
use bufkit_data::Model;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use std::ops::Deref;

pub trait Source {
    fn build_req_info(
        &self,
        site_id: String,
        stn_num: Option<bufkit_data::StationNumber>,
        model: bufkit_data::Model,
        init_time: chrono::NaiveDateTime,
    ) -> Option<ReqInfo>;
}

pub struct IowaState {}

impl Source for IowaState {
    fn build_req_info(
        &self,
        site_id: String,
        stn_num: Option<bufkit_data::StationNumber>,
        model: bufkit_data::Model,
        init_time: chrono::NaiveDateTime,
    ) -> Option<super::ReqInfo> {
        if Self::invalid_combination(&site_id, model, init_time) {
            return None;
        }

        let site_id = Self::fix_known_issues_with_site_mismatch_in_url_and_in_the_file(
            site_id, model, init_time,
        );

        let url = Self::build_url(&site_id, model, &init_time);

        Some(ReqInfo {
            site_id,
            site: stn_num,
            model,
            init_time: Some(init_time),
            url,
        })
    }
}

impl IowaState {
    const HOST_URL: &'static str = "http://mtarchive.geol.iastate.edu/";

    fn build_url(site: &str, model: Model, init_time: &NaiveDateTime) -> String {
        let site = site.to_lowercase();

        let year = init_time.year();
        let month = init_time.month();
        let day = init_time.day();
        let hour = init_time.hour();
        let remote_model = match (model, hour) {
            (Model::GFS, _) => "gfs3",
            (Model::NAM, 6) | (Model::NAM, 18) => "namm",
            (Model::NAM, _) => "nam",
            (Model::NAM4KM, _) => "nam4km",
        };

        let remote_file_name = remote_model.to_string() + "_" + &site + ".buf";

        format!(
            "{}{}/{:02}/{:02}/bufkit/{:02}/{}/{}",
            Self::HOST_URL,
            year,
            month,
            day,
            hour,
            model.to_string().to_lowercase(),
            remote_file_name
        )
    }

    fn fix_known_issues_with_site_mismatch_in_url_and_in_the_file(
        site_id: String,
        model: Model,
        init_time: NaiveDateTime,
    ) -> String {
        if site_id == "KLDN"
            && model != Model::GFS
            && init_time >= NaiveDate::from_ymd_opt(2020, 5, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()
        {
            "KDLN".to_owned()
        } else if site_id == "KLDN"
            && model == Model::GFS
            && init_time >= NaiveDate::from_ymd_opt(2021, 3, 22).unwrap().and_hms_opt(18, 0, 0).unwrap()
        {
            "KDLN".to_owned()
        } else {
            site_id
        }
    }

    fn invalid_combination(site: &str, model: Model, init_time: NaiveDateTime) -> bool {
        let site: String = site.to_lowercase();

        let model_site_mismatch = match site.deref() {
            "bam" | "c17" | "lrr" | "s06" | "ssy" | "xkza" | "xxpn" => {
                model == Model::NAM || model == Model::NAM4KM
            }
            "bon" | "hmm" | "mrp" | "smb" | "win" => model == Model::GFS,
            "wntr" => model == Model::GFS || model == Model::NAM4KM,
            "kfca" => model == Model::NAM || model == Model::NAM4KM,
            "paeg" | "pabt" | "pafa" | "pafm" | "pamc" | "pfyu" => model == Model::NAM4KM,
            _ => false, // All other combinations are OK
        };

        let model_init_time_mismatch = match model {
            Model::NAM4KM => init_time < NaiveDate::from_ymd_opt(2013, 3, 25).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            _ => init_time < NaiveDate::from_ymd_opt(2011, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
        };

        let expired_sites = match (site.deref(), model) {
            ("lrr", Model::GFS) => init_time >= NaiveDate::from_ymd_opt(2018, 12, 5).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            ("c17", Model::GFS) => init_time >= NaiveDate::from_ymd_opt(2018, 12, 5).unwrap().and_hms_opt(0, 0, 0).unwrap(),
            ("sta", Model::GFS) => init_time <= NaiveDate::from_ymd_opt(2018, 12, 4).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("xxpn", Model::GFS) => init_time <= NaiveDate::from_ymd_opt(2018, 12, 4).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("wev", Model::GFS) => init_time <= NaiveDate::from_ymd_opt(2018, 12, 4).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("xkza", Model::GFS) => init_time <= NaiveDate::from_ymd_opt(2018, 12, 4).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("mpi", Model::GFS) => init_time <= NaiveDate::from_ymd_opt(2018, 12, 4).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("kmpi", Model::GFS) => {
                init_time <= NaiveDate::from_ymd_opt(2018, 12, 4).unwrap().and_hms_opt(12, 0, 0).unwrap()
                    || init_time >= NaiveDate::from_ymd_opt(2021, 3, 22).unwrap().and_hms_opt(18, 0, 0).unwrap()
            }

            // For the site/model combos below there is sparse data further back, but it's very sparse.
            ("pafm", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("pfyu", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("pabt", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("wev", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("wntr", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("smb", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("hmm", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("sta", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("mpi", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("wja", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("mrp", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("pamc", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("pafa", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("paeg", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("cype", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("cyyc", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("cwlb", Model::NAM) => init_time < NaiveDate::from_ymd_opt(2012, 2, 17).unwrap().and_hms_opt(12, 0, 0).unwrap(),
            ("ssy", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("cwlb", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("bam", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("cyyc", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("paeg", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("cype", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("pfyu", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("pafa", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("pamc", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("pabt", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("wja", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),
            ("pafm", Model::GFS) => init_time < NaiveDate::from_ymd_opt(2012, 2, 16).unwrap().and_hms_opt(18, 0, 0).unwrap(),

            _ => false,
        };

        model_site_mismatch || model_init_time_mismatch || expired_sites
    }
}
