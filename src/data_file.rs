use std::fs::File;
use std::path::PathBuf;

use anyhow::{Context, Result};
use polars::prelude::{
    CsvReader, CsvWriter, DataFrame, LazyCsvReader, LazyFrame, ParquetCompression, ParquetReader,
    ParquetWriter, ScanArgsParquet, SerReader, SerWriter,
};

pub fn load_csv_file(file_path: &PathBuf) -> Result<DataFrame> {
    CsvReader::from_path(file_path)
        .with_context(|| format!("could not open CSV file {:?}", &file_path))?
        .has_header(true)
        .with_delimiter(b',')
        .with_quote_char(Some(b'"'))
        .with_parse_dates(true)
        .finish()
        .with_context(|| format!("could not load CSV file {:?}", &file_path))
}

pub fn scan_csv_file(file_path: &PathBuf) -> Result<LazyFrame> {
    LazyCsvReader::new(file_path.to_string_lossy().parse()?)
        .has_header(true)
        .with_delimiter(b',')
        .with_quote_char(Some(b'"'))
        .with_parse_dates(true)
        .finish()
        .with_context(|| format!("could not scan CSV file {:?}", &file_path))
}

pub fn save_csv_file(file_path: &PathBuf, data: &mut DataFrame) -> Result<()> {
    let csv_file = File::create(&file_path)
        .with_context(|| format!("Could not create CSV file {:?}", &file_path))?;
    CsvWriter::new(csv_file)
        .has_header(true)
        .with_delimiter(b',')
        .with_quoting_char(b'"')
        .finish(data)
        .unwrap_or_else(|_| panic!("Failed writing dataframe to file {:?}", file_path));
    Ok(())
}

pub fn load_parquet_file(file_path: &PathBuf) -> Result<DataFrame> {
    let parquet_file = File::open(file_path)
        .with_context(|| format!("could not open Parquet file {:?}", file_path))?;
    ParquetReader::new(parquet_file)
        .finish()
        .with_context(|| format!("could not load Parquet file {:?}", &file_path))
}

pub fn scan_parquet_file(file_path: &PathBuf) -> Result<LazyFrame> {
    LazyFrame::scan_parquet(
        file_path.to_string_lossy().parse()?,
        ScanArgsParquet::default(),
    )
    .with_context(|| format!("could not scan Parquet file {:?}", &file_path))
}

pub fn save_parquet_file(file_path: &PathBuf, data: &mut DataFrame) -> Result<()> {
    let parquet_file = File::create(&file_path)
        .with_context(|| format!("Could not create parquet file {:?}", &file_path))?;
    ParquetWriter::new(parquet_file)
        .with_compression(ParquetCompression::Snappy)
        .finish(data)
        .unwrap_or_else(|_| panic!("Failed writing dataframe to file {:?}", file_path));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs;

    use super::*;

    #[test]
    fn round_trip() {
        let csv_path = PathBuf::from(OsStr::new("round_trip_test_data.csv"));
        let parquet_path = csv_path.with_extension("parquet");
        assert!(csv_path.is_file());
        fs::remove_file(&parquet_path).unwrap_or_default();

        let result = load_csv_file(&csv_path);
        assert!(result.is_ok());
        let mut dataframe = result.unwrap();

        let result = save_parquet_file(&parquet_path, &mut dataframe);
        assert!(result.is_ok());
        assert!(parquet_path.is_file());

        let result = load_parquet_file(&parquet_path);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), dataframe);

        assert!(fs::remove_file(&parquet_path).is_ok());
    }
}
