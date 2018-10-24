use bufkit_data::{Archive, Model};
use chrono::NaiveDate;
use climo_db::{ClimoDB, ClimoDBInterface};
use failure::Error;
use sounding_analysis::{haines_high, haines_low, haines_mid, hot_dry_windy};
use sounding_bufkit::BufkitData;
use strum::AsStaticRef;

//
// TODO - split into multiple channels.
//        main channel constructs list of site/model/init times to load
//        Second channel loads files from arch
//        Third channel communicates with climo database.
//        Fourth channel calcs fire stats
//        Fifth channel calcs inventory and count
//
//        Use queries to construct max/min and percentiles
//
//

pub fn build_climo(arch: &Archive, site: &str, model: Model) -> Result<(), Error> {
    let climo_db = ClimoDB::open_or_create(arch.get_root())?;
    let mut climo_db = ClimoDBInterface::initialize(&climo_db)?;

    let site = &site.to_uppercase();
    let model_str = model.as_static();

    let (start_date, end_date) = climo_db.get_current_climo_date_range(site, model_str)?;

    let (mut new_start_date, mut new_end_date) = if start_date == end_date {
        (
            NaiveDate::from_ymd(3000, 12, 31).and_hms(0, 0, 0),
            NaiveDate::from_ymd(1900, 1, 1).and_hms(0, 0, 0),
        )
    } else {
        (start_date, end_date)
    };

    let (mut hdw_max, mut hdw_max_date) = climo_db.get_current_maximums(site, model_str)?;

    for anal in arch
        // Get all the available init times
        .get_init_times(site, model)?
        .into_iter()
        // Filter out dates we already have in the climo database.
        .filter(|&init_time| init_time < start_date || init_time > end_date)
        // Retrieve the sounding file from the archive
        .filter_map(|init_time| arch.get_file(site, model, &init_time).ok())
        // Parse the string data as a sounding analysis series and take enough forecasts to fill in
        // between forecasts.
        .filter_map(|string_data| {
            let analysis: Vec<_> = BufkitData::new(&string_data)
                .ok()?
                .into_iter()
                .take(model.hours_between_runs() as usize)
                .collect();

            Some(analysis)
        }).
        // Flatten them into an hourly time series.
        flat_map(|anal_vec| anal_vec.into_iter())
    {
        let snd = anal.sounding();
        let valid_time = match snd.get_valid_time() {
            Some(valid_time) => valid_time,
            None => continue,
        };

        //
        //  Update inventory end time
        //
        if valid_time > new_end_date {
            new_end_date = valid_time;
        }

        if valid_time < new_start_date {
            new_start_date = valid_time;
        }

        //
        // Get the HDW
        //
        let hdw = hot_dry_windy(snd)? as i32;
        if hdw >= hdw_max {
            hdw_max = hdw;
            hdw_max_date = valid_time;
        }

        //
        // Get the hdw various Haines index values.
        //
        let hns_low = haines_low(snd).unwrap_or(0.0) as i32;
        let hns_mid = haines_mid(snd).unwrap_or(0.0) as i32;
        let hns_high = haines_high(snd).unwrap_or(0.0) as i32;
        climo_db.add_fire(
            site,
            model_str,
            &valid_time,
            (hns_high, hns_mid, hns_low),
            hdw,
        )?;

        //
        // Things only to do for 0 lead time, don' need every sounding
        //
        if anal
            .sounding()
            .get_lead_time()
            .into_option()
            .map(|lt| lt != 0)
            .unwrap_or(false)
        {
            continue;
        }

        //
        // Location climo
        //
        let location = snd.get_station_info().location();
        let elevation = snd.get_station_info().elevation().into_option();

        if let (Some(elev_m), Some((lat, lon))) = (elevation, location) {
            climo_db.add_location(site, model_str, &valid_time, lat, lon, elev_m)?;
        }
    }

    //
    // With new dates for inventory and max value for HDW, update the db
    //
    climo_db.update_inventory(site, model_str, &new_start_date, &new_end_date)?;
    climo_db.update_maximums(site, model_str, hdw_max, &hdw_max_date)?;

    Ok(())
}
