//! firebuf - Calculate fire weather indicies from soundings in your Bufkit Archive.
use bfkmd::{bail, parse_date_string, TablePrinter};
use bufkit_data::{Archive, BufkitDataErr, Model, SiteInfo};
use chrono::{Duration, NaiveDate, NaiveDateTime, Timelike};
use clap::{crate_version, App, Arg};
use dirs::home_dir;
use sounding_analysis::Sounding;
use sounding_bufkit::BufkitData;
use std::{collections::HashMap, error::Error, fs::File, path::PathBuf, str::FromStr};
use strum::{AsStaticRef, IntoEnumIterator};
use strum_macros::{AsStaticStr, EnumIter, EnumString};
use textplots::{Chart, Plot, Shape};

fn main() {
    if let Err(e) = run() {
        println!("error: {}", e);

        let mut err = &*e;

        while let Some(cause) = err.source() {
            println!("caused by: {}", cause);
            err = cause;
        }

        ::std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = &parse_args()?;

    #[cfg(debug_assertions)]
    println!("{:#?}", args);

    let arch = &Archive::connect(&args.root)?;
    let g_stats = &args.graph_stats;
    let t_stats = &args.table_stats;

    for site_id in &args.sites {
        for &model in &args.models {
            let station_num = match arch.station_num_for_id(site_id, model) {
                Ok(station_num) => station_num,
                Err(BufkitDataErr::NotInIndex) => {
                    println!(
                        "No data in archive for {} at {}.",
                        model.as_static_str(),
                        site_id,
                    );
                    continue;
                }
                Err(err) => return Err(err.into()),
            };

            let site = match arch.site(station_num) {
                Some(site) => site,
                None => continue,
            };

            let stats = &match calculate_stats(arch, &site, model, args.init_time, g_stats, t_stats)
            {
                Ok(stats) => stats,
                Err(_) => continue,
            };

            if args.print {
                print_stats(&site, site_id, model, stats, g_stats, t_stats)?;
            }

            if let Some(ref path) = args.save_dir {
                save_stats(site_id, model, stats, g_stats, t_stats, path)?;
            }
        }
    }

    Ok(())
}

fn parse_args() -> Result<CmdLineArgs, Box<dyn Error>> {
    let app = App::new("firebuf")
        .author("Ryan <rnleach@users.noreply.github.com>")
        .version(crate_version!())
        .about("Fire weather analysis & summary.")
        .arg(
            Arg::with_name("sites")
                .multiple(true)
                .short("s")
                .long("sites")
                .takes_value(true)
                .required(true)
                .help("Site identifiers (e.g. kord, katl, smn)."),
        )
        .arg(
            Arg::with_name("models")
                .multiple(true)
                .short("m")
                .long("models")
                .takes_value(true)
                .possible_values(
                    &Model::iter()
                        .map(|val| val.as_static_str())
                        .collect::<Vec<&str>>(),
                )
                .help("Allowable models for this operation/program.")
                .long_help(concat!(
                    "Allowable models for this operation/program.",
                    " Default is to use all possible values."
                )),
        )
        .arg(
            Arg::with_name("table-stats")
                .multiple(true)
                .short("t")
                .long("table-stats")
                .takes_value(true)
                .possible_values(
                    &TableStatArg::iter()
                        .map(|val| val.as_static())
                        .collect::<Vec<&str>>(),
                )
                .help("Which statistics to show in the table.")
                .long_help(concat!(
                    "Which statistics to show in the table.",
                    " Defaults to HDW,  MaxHDW, HainesLow, HainesMid, and HainesHigh"
                )),
        )
        .arg(
            Arg::with_name("graph-stats")
                .multiple(true)
                .short("g")
                .long("graph-stats")
                .takes_value(true)
                .possible_values(
                    &GraphStatArg::iter()
                        .map(|val| val.as_static())
                        .collect::<Vec<&str>>(),
                )
                .help("Which statistics to plot make a graph for.")
                .long_help(concat!(
                    "Which statistics to plot a graph for.",
                    " Defaults to HDW.",
                    " All graphs plot all available data, but each model is on an individual axis."
                )),
        )
        .arg(
            Arg::with_name("init-time")
                .long("init-time")
                .short("i")
                .takes_value(true)
                .help("The model inititialization time. YYYY-MM-DD-HH")
                .long_help(concat!(
                    "The initialization time of the model run to analyze.",
                    " Format is YYYY-MM-DD-HH. If not specified then the model run is assumed to",
                    " be the last available run in the archive."
                ))
                .conflicts_with("start-time")
                .conflicts_with("end_time"),
        )
        .arg(
            Arg::with_name("save-dir")
                .long("save-dir")
                .takes_value(true)
                .help("Directory to save .csv files to.")
                .long_help(concat!(
                    "Directory to save .csv files to. If this is specified then a file",
                    " 'site_model.csv' is created for each site and model in that directory with",
                    " data for each graph statistic specified. The exported data is set by the -g",
                    " option."
                )),
        )
        .arg(
            Arg::with_name("print")
                .long("print")
                .short("p")
                .possible_values(&["Y", "N", "y", "n"])
                .default_value("y")
                .takes_value(true)
                .help("Print the results to the terminal."),
        )
        .arg(
            Arg::with_name("root")
                .short("r")
                .long("root")
                .takes_value(true)
                .help("Set the root of the archive.")
                .long_help(
                    "Set the root directory of the archive you are invoking this command for.",
                )
                .conflicts_with("create")
                .global(true),
        );

    let matches = app.get_matches();

    let root = matches
        .value_of("root")
        .map(PathBuf::from)
        .or_else(|| home_dir().map(|hd| hd.join("bufkit")))
        .expect("Invalid root.");

    let sites: Vec<String> = matches
        .values_of("sites")
        .into_iter()
        .flat_map(|site_iter| site_iter.map(ToOwned::to_owned))
        .collect();

    let mut models: Vec<Model> = matches
        .values_of("models")
        .into_iter()
        .flat_map(|model_iter| model_iter.map(Model::from_str))
        .filter_map(Result::ok)
        .collect();

    if models.is_empty() {
        models = vec![Model::GFS, Model::NAM, Model::NAM4KM];
    }

    let mut table_stats: Vec<TableStatArg> = matches
        .values_of("table-stats")
        .into_iter()
        .flat_map(|stat_iter| stat_iter.map(TableStatArg::from_str))
        .filter_map(Result::ok)
        .collect();

    if table_stats.is_empty() {
        use crate::TableStatArg::{HainesHigh, HainesLow, HainesMid, Hdw, MaxHdw};
        table_stats = vec![Hdw, MaxHdw, HainesLow, HainesMid, HainesHigh];
    }

    let mut graph_stats: Vec<GraphStatArg> = matches
        .values_of("graph-stats")
        .into_iter()
        .flat_map(|stat_iter| stat_iter.map(GraphStatArg::from_str))
        .filter_map(Result::ok)
        .collect();

    if graph_stats.is_empty() {
        use crate::GraphStatArg::Hdw;
        graph_stats = vec![Hdw];
    }

    let init_time = matches.value_of("init-time").map(parse_date_string);

    let print: bool = {
        let arg_val = matches.value_of("print").unwrap(); // Safe, this is a required argument.

        arg_val == "Y" || arg_val == "y"
    };

    let save_dir: Option<PathBuf> = matches
        .value_of("save-dir")
        .map(str::to_owned)
        .map(PathBuf::from);

    if let Some(ref save_dir) = save_dir {
        if !save_dir.is_dir() {
            bail(&format!(
                "save-dir path {} does not exist.",
                save_dir.display()
            ));
        }
    }

    Ok(CmdLineArgs {
        root,
        sites,
        models,
        init_time,
        table_stats,
        graph_stats,
        print,
        save_dir,
    })
}

fn calculate_stats(
    arch: &Archive,
    site: &SiteInfo,
    model: Model,
    init_time: Option<NaiveDateTime>,
    g_stats: &[GraphStatArg],
    t_stats: &[TableStatArg],
) -> Result<ModelStats, Box<dyn Error>> {
    let analysis = match init_time {
        Some(init_time) => arch.retrieve(site.station_num, model, init_time)?,
        None => arch.retrieve_most_recent(site.station_num, model)?,
    };

    let analysis = BufkitData::init(&analysis, "")?;

    let mut model_stats = ModelStats::new();

    let mut curr_time: Option<NaiveDateTime> = None;
    for anal in &analysis {
        let sounding = &anal.0;

        let valid_time = if let Some(valid_time) = sounding.valid_time() {
            valid_time
        } else {
            continue;
        };

        if curr_time.is_none() {
            model_stats.init_time = Some(valid_time);
        }
        curr_time = Some(valid_time);

        let cal_day = (valid_time - Duration::hours(12)).date(); // Daily stats from 12Z to 12Z

        let graph_stats = model_stats
            .graph_stats
            .entry(valid_time)
            .or_insert_with(HashMap::new);

        // Build the graph stats
        for &graph_stat in g_stats {
            use crate::GraphStatArg::*;

            let stat = match graph_stat {
                Hdw => sounding_analysis::hot_dry_windy(sounding),
                HainesLow => sounding_analysis::haines_low(sounding).map(f64::from),
                HainesMid => sounding_analysis::haines_mid(sounding).map(f64::from),
                HainesHigh => sounding_analysis::haines_high(sounding).map(f64::from),
                AutoHaines => sounding_analysis::haines(sounding).map(f64::from),
                None => continue,
            };
            let stat = match stat {
                Ok(stat) => stat,
                Err(_) => ::std::f64::NAN,
            };

            graph_stats.insert(graph_stat, stat);
        }

        // Build the daily stats
        for &table_stat in t_stats {
            use crate::TableStatArg::*;

            let zero_z = |old_val: (f64, u32), new_val: (f64, u32)| -> (f64, u32) {
                if valid_time.hour() == 0 {
                    new_val
                } else {
                    old_val
                }
            };

            let max = |old_val: (f64, u32), new_val: (f64, u32)| -> (f64, u32) {
                if (old_val.0.is_nan() && !new_val.0.is_nan()) || (new_val.0 > old_val.0) {
                    new_val
                } else {
                    old_val
                }
            };

            let stat_func: &dyn Fn(&Sounding) -> Result<f64, _> = match table_stat {
                Hdw => &sounding_analysis::hot_dry_windy,
                MaxHdw => &sounding_analysis::hot_dry_windy,
                HainesLow => &|snd| sounding_analysis::haines_low(snd).map(f64::from),
                MaxHainesLow => &|snd| sounding_analysis::haines_low(snd).map(f64::from),
                HainesMid => &|snd| sounding_analysis::haines_mid(snd).map(f64::from),
                MaxHainesMid => &|snd| sounding_analysis::haines_mid(snd).map(f64::from),
                HainesHigh => &|snd| sounding_analysis::haines_high(snd).map(f64::from),
                MaxHainesHigh => &|snd| sounding_analysis::haines_high(snd).map(f64::from),
                AutoHaines => &|snd| sounding_analysis::haines(snd).map(f64::from),
                MaxAutoHaines => &|snd| sounding_analysis::haines(snd).map(f64::from),
                TableStatArg::None => continue,
            };
            let stat = match stat_func(sounding) {
                Ok(stat) => stat,
                Err(_) => ::std::f64::NAN,
            };

            let selector: &dyn Fn((f64, u32), (f64, u32)) -> (f64, u32) = match table_stat {
                Hdw => &zero_z,
                MaxHdw => &max,
                HainesLow => &zero_z,
                MaxHainesLow => &max,
                HainesMid => &zero_z,
                MaxHainesMid => &max,
                HainesHigh => &zero_z,
                MaxHainesHigh => &max,
                AutoHaines => &zero_z,
                MaxAutoHaines => &max,
                TableStatArg::None => unreachable!(),
            };

            let table_stats = model_stats
                .table_stats
                .entry(table_stat)
                .or_insert_with(HashMap::new);

            let day_entry = table_stats.entry(cal_day).or_insert((::std::f64::NAN, 12));
            let hour = valid_time.hour();

            *day_entry = selector(*day_entry, (stat, hour));
        }
    }

    model_stats.end_time = curr_time;

    Ok(model_stats)
}

fn print_stats(
    site: &SiteInfo,
    site_id: &str,
    model: Model,
    stats: &ModelStats,
    g_stats: &[GraphStatArg],
    t_stats: &[TableStatArg],
) -> Result<(), Box<dyn Error>> {
    //
    // Table
    //
    if !stats.table_stats.is_empty() {
        let table_stats = &stats.table_stats;
        let vals = &table_stats[&t_stats[0]];

        let mut days: Vec<NaiveDate> = vals.keys().cloned().collect();
        days.sort();

        let title = format!(
            "Fire Indexes for {}{} ({}).",
            site.name.as_deref().unwrap_or(""),
            match site.state {
                Some(st) => format!(", {}", st.as_static_str()),
                None => "".to_owned(),
            },
            site_id
        );

        let header = format!(
            "{} data from {} to {}.",
            model,
            stats
                .init_time
                .map(|dt| dt.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
            stats
                .end_time
                .map(|dt| dt.to_string())
                .unwrap_or_else(|| "unknown".to_owned())
        );
        let footer = concat!(
            "For daily maximum values, first and last days may be partial. ",
            "Days run from 12Z on the date listed until 12Z the next day."
        )
        .to_owned();

        let mut tp = TablePrinter::new()
            .with_title(title)
            .with_header(header)
            .with_footer(footer)
            .with_column("Date", &days);

        for &table_stat in t_stats {
            use crate::TableStatArg::*;

            let vals = match table_stats.get(&table_stat) {
                Some(vals) => vals,
                Option::None => continue,
            };

            let mut days: Vec<NaiveDate> = vals.keys().cloned().collect();
            days.sort();

            let daily_stat_values = days.iter().map(|d| vals[d]);
            let daily_stat_values: Vec<String> = match table_stat {
                Hdw | HainesLow | HainesMid | HainesHigh | AutoHaines => daily_stat_values
                    .map(|(val, _)| format!("{:.0}", val))
                    .map(|val| {
                        if val.contains("NaN") {
                            "".to_owned()
                        } else {
                            val
                        }
                    })
                    .collect(),
                _ => daily_stat_values
                    .map(|(val, hour)| format!("{:.0} ({:02}Z)", val, hour))
                    .map(|val| {
                        if val.contains("NaN") {
                            "".to_owned()
                        } else {
                            val
                        }
                    })
                    .collect(),
            };

            tp = tp.with_column(table_stat.as_static(), &daily_stat_values);
        }
        tp.print_with_min_width(78)?;
    }

    //
    // END TABLE
    //

    //
    // GRAPHS
    //
    let graph_stats = &stats.graph_stats;
    let mut valid_times: Vec<NaiveDateTime> = graph_stats.keys().cloned().collect();
    valid_times.sort();
    for &g_stat in g_stats {
        let mut vals: Vec<(NaiveDateTime, f32)> = vec![];
        for vt in &valid_times {
            graph_stats.get(vt).and_then(|hm| {
                hm.get(&g_stat).map(|stat_val| {
                    vals.push((*vt, *stat_val as f32));
                })
            });
        }

        let base_time = if let Some(first) = vals.get(0) {
            first.0
        } else {
            continue;
        };

        let base_hour = if base_time.hour() == 0 {
            24f32
        } else {
            base_time.hour() as f32
        };

        let values_start = [
            (0.0, g_stat.default_max_y()),
            (1.0 / 24.0, g_stat.default_min_y()),
            ((base_hour - 1.0) / 24.0, g_stat.default_min_y()),
        ];
        let values_iter = vals.iter().map(|&(v_time, val)| {
            (
                ((v_time - base_time).num_hours() as f32 + base_hour) / 24.0,
                val as f32,
            )
        });

        let values_plot: Vec<(f32, f32)> =
            values_start.iter().cloned().chain(values_iter).collect();

        println!(
            "{:^78}",
            format!(
                "{} {} for {}{} ({})",
                model,
                g_stat.as_static(),
                site.name.as_deref().unwrap_or(""),
                match site.state {
                    Some(st) => format!(", {}", st.as_static_str()),
                    None => "".to_owned(),
                },
                site_id
            )
        );

        Chart::new(160, 45, 0.0, 9.0)
            .lineplot(Shape::Steps(values_plot.as_slice()))
            .nice();
    }
    //
    // END GRAPHS
    //

    Ok(())
}

fn save_stats(
    site_id: &str,
    model: Model,
    stats: &ModelStats,
    g_stats: &[GraphStatArg],
    _t_stats: &[TableStatArg],
    save_dir: &PathBuf,
) -> Result<(), Box<dyn Error>> {
    let graph_stats = &stats.graph_stats;
    let mut vts: Vec<NaiveDateTime> = graph_stats.keys().cloned().collect();
    vts.sort();

    let init_time_str = &vts[0].format("_%Y%m%d%H").to_string();

    let file_name = String::new() + &site_id + "_" + model.as_static_str() + init_time_str + ".csv";
    let path = save_dir.join(&file_name);
    let file = File::create(path)?;
    let mut wtr = csv::Writer::from_writer(file);

    let mut stat_key_strings: Vec<&str> = g_stats.iter().map(AsStaticRef::as_static).collect();
    stat_key_strings.insert(0, "Time");

    wtr.write_record(&stat_key_strings)?;

    let mut string_vec: Vec<String> = vec![];
    for vt in vts {
        string_vec.push(format!("{}", vt));

        let vals = match graph_stats.get(&vt) {
            Some(vals) => vals,
            None => continue,
        };

        for stat_key in g_stats {
            let val = match vals.get(stat_key) {
                Some(val) => val,
                None => continue,
            };

            string_vec.push(format!("{}", val));
        }

        wtr.write_record(&string_vec)?;
        string_vec.clear();
    }

    Ok(())
}

#[derive(Debug)]
struct CmdLineArgs {
    root: PathBuf,
    sites: Vec<String>,
    models: Vec<Model>,
    init_time: Option<NaiveDateTime>,
    table_stats: Vec<TableStatArg>,
    graph_stats: Vec<GraphStatArg>,
    print: bool,
    save_dir: Option<PathBuf>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumString, AsStaticStr, EnumIter, Hash)]
enum GraphStatArg {
    #[strum(serialize = "HDW")]
    Hdw,
    HainesLow,
    HainesMid,
    HainesHigh,
    AutoHaines,
    None,
}

impl GraphStatArg {
    fn default_min_y(self) -> f32 {
        0.0
    }

    fn default_max_y(self) -> f32 {
        use crate::GraphStatArg::*;

        match self {
            Hdw => 700.0,
            _ => 6.0,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumString, AsStaticStr, EnumIter, Hash)]
enum TableStatArg {
    #[strum(serialize = "HDW")]
    Hdw,
    #[strum(serialize = "MaxHDW")]
    MaxHdw,
    HainesLow,
    MaxHainesLow,
    HainesMid,
    MaxHainesMid,
    HainesHigh,
    MaxHainesHigh,
    AutoHaines,
    MaxAutoHaines,
    None,
}

#[derive(Debug)]
struct ModelStats {
    graph_stats: HashMap<NaiveDateTime, HashMap<GraphStatArg, f64>>,
    table_stats: HashMap<TableStatArg, HashMap<NaiveDate, (f64, u32)>>,
    init_time: Option<NaiveDateTime>,
    end_time: Option<NaiveDateTime>,
}

impl ModelStats {
    fn new() -> Self {
        ModelStats {
            graph_stats: HashMap::new(),
            table_stats: HashMap::new(),
            init_time: None,
            end_time: None,
        }
    }
}
