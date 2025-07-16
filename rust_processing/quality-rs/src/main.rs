use clap::Parser;
use color_eyre::eyre::Result;
use std::path::PathBuf;
use std::sync::Arc;

/*======================================================
=                              ARGS                    =
======================================================*/

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(required = false, long)]
    input: Vec<PathBuf>,

    #[arg(required = true, long)]
    output: PathBuf,

    #[arg(long, default_value_t = 0)]
    threads: usize,

    #[arg(long)]
    queue_id: Option<String>,

    #[arg(long, default_value_t = false)]
    use_redis_task: bool,

    #[arg(long, default_value_t = false)]
    no_progress_bar: bool,
}

/*==============================================================
=                         MAIN BLOCK                           =
==============================================================*/

fn main() -> Result<()> {
    let args = Args::parse();

    let filters: Arc<Vec<Box<dyn quality_rs::filter::Filter>>> = Arc::new(vec![
        Box::new(quality_rs::filter::LineRemovalModifier {
            max_removed_ratio: -1.0,
            max_uppercase_ratio: 0.99,
            min_word_cnt_per_line: 3,
            lang: "en".to_string(),
        }),
        Box::new(quality_rs::filter::CacheTokenFilter::default()),
        Box::new(quality_rs::filter::GopherRepetitionFilter::default()),
        Box::new(quality_rs::filter::GopherQualityFilter::default()),
        Box::new(quality_rs::filter::FinewebQualityFilter::default()),
        Box::new(quality_rs::filter::UncacheTokenFilter::default()),
    ]);

    let queue_id: &str = args.queue_id.as_deref().unwrap_or("default");
    quality_rs::run(
        args.input,
        &args.output,
        &filters,
        &args.threads,
        queue_id,
        args.use_redis_task,
        args.no_progress_bar,
    )
}
