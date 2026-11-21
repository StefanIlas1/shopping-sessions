mod input;
mod output;
mod pipeline;

use input::{Files, Window, scan, unique_rows};
use pipeline::{Dimensions, aggregate, load_clicks, load_events, sort_inputs};
use std::env;
use std::path::PathBuf;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    let started = Instant::now();
    let mut args = env::args_os().skip(1);
    let data = args
        .next()
        .or_else(|| env::var_os("DATA_DIR"))
        .unwrap_or_else(|| "/data".into());
    let out = args
        .next()
        .or_else(|| env::var_os("OUTPUT_DIR"))
        .unwrap_or_else(|| "/output".into());
    let mode = env::var("SHOPPING_MODE").unwrap_or_else(|_| "full".into());
    if args.next().is_some() || !matches!(mode.as_str(), "scan" | "dedup" | "full") {
        return Err(
            "usage: SHOPPING_MODE=scan|dedup|full shopping-sessions [DATA_DIR OUTPUT_DIR]".into(),
        );
    }
    let threads = env::var("NCPU")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(8);
    if threads == 0 {
        return Err("NCPU must be positive".into());
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()?;
    let data = PathBuf::from(data);
    let out = PathBuf::from(out);
    let profiled = env::var_os("SHOPPING_PROFILE").is_some();
    let mut last = started;
    let mut report = |name: &str| {
        let now = Instant::now();
        if profiled {
            eprintln!("{name} {:.3} ms", (now - last).as_secs_f64() * 1_000.0);
        }
        last = now;
    };

    let events = Files::open_parts(&data, "events")?;
    let clicks = Files::open_parts(&data, "clicks")?;
    let links = Files::open_parts(&data, "links")?;
    let products = Files::open_file(&data.join("products.csv"))?;
    let campaigns = Files::open_file(&data.join("campaigns.csv"))?;
    let window = Window::read(&data)?;
    report("open");
    match mode.as_str() {
        "scan" => {
            let inputs = [&events, &clicks, &links, &products, &campaigns];
            let bytes: usize = inputs.iter().map(|files| files.bytes()).sum();
            let rows: usize = inputs.iter().map(|files| scan(files)).sum();
            eprintln!("scan bytes={bytes} rows={rows}");
            report("scan");
        }
        "dedup" => {
            let (events, clicks) = rayon::join(
                || unique_rows::<_, _, 7>(&events, Some(3), |_, _| Some(())).len(),
                || unique_rows::<_, _, 4>(&clicks, None, |_, _| Some(())).len(),
            );
            eprintln!("dedup events={events} clicks={clicks}");
            report("scan_and_dedup");
        }
        _ => {
            let dimensions = Dimensions::load(&links, &products, &campaigns);
            report("dimensions");
            let (mut events, mut clicks) = rayon::join(
                || load_events(&events, &dimensions, &window),
                || load_clicks(&clicks, &dimensions, &window),
            );
            report("parse_dedup_validate");
            sort_inputs(&mut events, &mut clicks);
            report("sort");
            let (users, campaigns) = aggregate(&events, &clicks);
            report("aggregate");
            output::write_tables(&out, &users, campaigns, &dimensions)?;
            report("parquet");
        }
    }
    if profiled {
        eprintln!("total {:.3} ms", started.elapsed().as_secs_f64() * 1_000.0);
    }
    Ok(())
}
