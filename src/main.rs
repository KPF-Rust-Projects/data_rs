use std::ffi::OsStr;
use std::path::PathBuf;

use anyhow::bail;
use clap::{Parser, Subcommand};

mod convert;

#[derive(Parser, Debug)]
#[clap(author, version, about="Data conversion tool", long_about = None)]
struct Args {
    /// Turn debugging information on
    #[clap(short, long, value_parser)]
    debug: bool,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Convert a CSV data file to a Parquet file
    Convert {
        /// Path to CSV file
        #[clap(value_parser, value_name = "CSV_FILE")]
        csv: Option<PathBuf>,

        /// Force conversion for files without .csv extension [false]
        #[clap(short, long, value_parser, default_value_t = false)]
        force: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.debug {
        println!("DEBUG Arguments: {:#?}", args);
    }

    match &args.command {
        Commands::Convert { csv, force } => {
            if let Some(csv_path) = csv {
                if csv_path.extension().unwrap_or_else(|| OsStr::new("")) != "csv" && !(*force) {
                    bail!("File extension is not '.csv'! Use --force parameter to convert anyway.")
                }
                convert::convert_csv_file(csv_path, args.debug)?;
            } else {
                bail!("'convert' subcommand needs a CSV file path")
            }
        }
    }

    Ok(())
}
