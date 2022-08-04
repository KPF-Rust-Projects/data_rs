use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use encoding::all::WINDOWS_1252;
use encoding::{DecoderTrap, Encoding};
use polars::prelude::{
    CsvReader, CsvWriter, DataFrame, LazyCsvReader, LazyFrame, ParquetCompression, ParquetReader,
    ParquetWriter, ScanArgsParquet, SerReader, SerWriter,
};

pub fn win_to_utf8(file_path: &PathBuf, new_file_path: &PathBuf) -> Result<()> {
    let file =
        File::open(file_path).with_context(|| format!("could not open file {:?}", file_path))?;
    let new_file = File::create(new_file_path)
        .with_context(|| format!("could not create file {:?}", new_file_path))?;
    let reader = BufReader::new(&file);
    let mut writer = BufWriter::new(&new_file);

    for line in reader.split(b'\n').map(|line| line.unwrap()) {
        let mut decoded = WINDOWS_1252.decode(&line, DecoderTrap::Strict).unwrap();
        decoded += "\n";
        writer.write(decoded.as_ref()).with_context(|| {
            format!(
                "error writing string {:?} to target file {:?}",
                &decoded, &new_file_path
            )
        })?;
    }
    Ok(())
}

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
mod data_file_tests {
    use std::fs;
    use std::io::Read;

    use super::*;

    #[test]
    fn fail_on_non_utf8_text() {
        let csv_path = PathBuf::from("test_data/test_data_windows-1252.csv");
        let result = load_csv_file(&csv_path);
        assert!(result.is_err());

        let error = result.err().unwrap();
        assert_eq!(
            error.to_string(),
            "could not load CSV file \"test_data/test_data_windows-1252.csv\""
        );
        assert_eq!(
            error.source().unwrap().to_string(),
            "invalid utf8 data in csv"
        )
    }

    #[test]
    fn full_round_trip() {
        // prepare file paths
        let csv_path = PathBuf::from("test_data/round_trip_test_data.csv");
        let parquet_path = csv_path.with_extension("parquet");
        assert!(csv_path.is_file());
        fs::remove_file(&parquet_path).unwrap_or_default();

        // load CSV file
        let result = load_csv_file(&csv_path);
        assert!(result.is_ok());
        let mut dataframe = result.unwrap();

        // write to Parquet file
        let result = save_parquet_file(&parquet_path, &mut dataframe);
        assert!(result.is_ok());
        assert!(parquet_path.is_file());

        // reload Parquet file, compare new data to initial data
        let result = load_parquet_file(&parquet_path);
        assert!(result.is_ok());
        let mut new_dataframe = result.unwrap();
        assert_eq!(new_dataframe, dataframe);

        // save new data to new CSV file
        let csv_path_new = csv_path.with_extension("csv_new");
        let result = save_csv_file(&csv_path_new, &mut new_dataframe);
        assert!(result.is_ok());
        assert!(csv_path_new.is_file());

        // load new CSV file, compare data again
        let result = load_csv_file(&csv_path_new);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), dataframe);

        // remove result files
        assert!(fs::remove_file(&parquet_path).is_ok());
        assert!(fs::remove_file(&csv_path_new).is_ok());
    }

    #[test]
    fn convert_win_1252() {
        // convert file to new file, ensure result
        let csv_path = PathBuf::from("test_data/test_data_windows-1252.csv");
        let new_csv_path = csv_path.with_extension("csv_utf8");
        let result = win_to_utf8(&csv_path, &new_csv_path);
        assert!(result.is_ok());
        assert!(new_csv_path.is_file());

        // compare contents of new and old file
        let mut old_file = File::open(csv_path).unwrap();
        let mut old_content = Vec::new();
        old_file.read_to_end(&mut old_content).unwrap();
        let mut new_file = File::open(&new_csv_path).unwrap();
        let mut new_content = Vec::new();
        new_file.read_to_end(&mut new_content).unwrap();
        assert_eq!(
            String::from_utf8(new_content).unwrap(),
            WINDOWS_1252
                .decode(old_content.as_slice(), DecoderTrap::Strict)
                .unwrap()
        );

        // remove new file
        assert!(fs::remove_file(&new_csv_path).is_ok());
    }
}
