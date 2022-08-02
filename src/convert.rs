use std::fs::File;
use std::path::PathBuf;

use anyhow::Context;
use polars::io::csv;
use polars::prelude::*;

/// Parse a CSV file and write its contents to a Parquet file,
/// replacing .csv extension by .parquet.
pub fn convert_csv_file(csv_path: &PathBuf, debug: bool) -> anyhow::Result<()> {
    // read from CSV
    if debug {
        println!("DEBUG: convert CSV file {:?}", csv_path)
    }
    let mut dataframe = csv::CsvReader::from_path(csv_path)
        .with_context(|| format!("could not open CSV file {:?}", &csv_path))?
        .has_header(true)
        .with_parse_dates(true)
        .finish()
        .with_context(|| format!("could not parse CSV file {:?}", &csv_path))?;
    if debug {
        println!("DEBUG: Parsed dataframe: {:#?}", dataframe)
    }

    // write to Parquet
    let parquet_file_path = csv_path.with_extension("parquet");
    if debug {
        println!("DEBUG: Writing dataframe to file {:?}", &parquet_file_path)
    }
    let parquet_file = File::create(&parquet_file_path)
        .with_context(|| format!("Could not create parquet file {:?}", &parquet_file_path))?;
    ParquetWriter::new(parquet_file)
        .finish(&mut dataframe)
        .expect(format!("Failed writing dataframe to file {:?}", parquet_file_path).as_str());

    Ok(())
}
