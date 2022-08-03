use std::ffi::OsStr;
use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};

mod commands;
mod data_file;

#[derive(Parser, Debug)]
#[clap(author, version, about="Data file tool", long_about = None)]
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
    ToParquet {
        /// Path to CSV file
        #[clap(value_parser, value_name = "CSV_FILE")]
        csv_path: PathBuf,

        /// Force conversion for files without .csv extension [false]
        #[clap(short, long, value_parser, default_value_t = false)]
        force: bool,
    },
    /// Convert a Parquet data file to a CSV file
    ToCSV {
        /// Path to Parquet file
        #[clap(value_parser, value_name = "PARQUET_FILE")]
        parquet_path: PathBuf,
    },
    /// Scan a CSV data file and show its structure
    CsvSchema {
        /// Path to CSV file
        #[clap(value_parser, value_name = "CSV_FILE")]
        csv_path: PathBuf,

        /// Force scan for files without .csv extension [false]
        #[clap(short, long, value_parser, default_value_t = false)]
        force: bool,
    },
    /// Scan a Parquet data file and show its structure
    ParquetSchema {
        /// Path to Parquet file
        #[clap(value_parser, value_name = "PARQUET_FILE")]
        parquet_path: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.debug {
        println!("DEBUG Arguments: {:#?}", args);
    }

    match &args.command {
        Commands::ToParquet { csv_path, force } => {
            if csv_path.extension().unwrap_or_else(|| OsStr::new("")) != "csv" && !(*force) {
                bail!("File extension is not '.csv'! Use --force parameter to convert anyway.")
            }
            commands::csv_to_parquet(csv_path, args.debug)?;
        }
        Commands::ToCSV { parquet_path } => {
            commands::parquet_to_csv(parquet_path, args.debug)?;
        }
        Commands::CsvSchema { csv_path, force } => {
            if csv_path.extension().unwrap_or_else(|| OsStr::new("")) != "csv" && !(*force) {
                bail!("File extension is not '.csv'! Use --force parameter to scan anyway.")
            }
            let output = commands::csv_schema(csv_path)?;
            println!("Scanned file {:?}, {}", csv_path, output);
        }
        Commands::ParquetSchema { parquet_path } => {
            let output = commands::parquet_schema(parquet_path)?;
            println!("Scanned file {:?}, {}", parquet_path, output);
        }
    }

    Ok(())
}
