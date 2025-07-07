use clap::Parser;
use std::io::Result;
use std::path::Path;
use std::path::PathBuf;
use std::thread::available_parallelism;

/*======================================================
=                              ARGS                    =
======================================================*/

#[derive(Parser, Debug)]
struct Args {
    #[arg(required = true, long)]
    input: Vec<PathBuf>,

    #[arg(required = true, long)]
    output: PathBuf,

    #[arg(long, default_value_t = 0)]
    threads: usize,
}

/*==============================================================
=                         MAIN BLOCK                           =
==============================================================*/

fn main() -> Result<()> {
    env_logger::init();
    println!("Hello");

    let _args = Args::parse();

    let threads = if args.threads == 0 {
        available_parallelism().unwrap().get()
    } else {
        args.threads
    };

    println!("------- {:?}", threads);
    Ok(())
}
