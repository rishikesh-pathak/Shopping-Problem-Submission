use crate::{CampaignDay, Result, UserDay};
use parquet::basic::{Compression, Encoding};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::{properties::WriterProperties, writer::SerializedFileWriter};
use parquet::schema::parser::parse_message_type;
use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};

const USERS: &str = "
message user_daily {
    REQUIRED BINARY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BINARY top_category (UTF8);
}";

const CAMPAIGNS: &str = "
message campaign_daily {
    REQUIRED BINARY campaign_id (UTF8);
    REQUIRED BINARY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

const ROW_GROUP_SIZE: usize = 64 * 1024;

// A successful run can replace one that wrote more user partitions.
pub fn remove_unused_user_parts(dir: &Path, files_written: usize) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let index = name
            .to_str()
            .and_then(|name| name.strip_prefix("part-"))
            .and_then(|name| name.strip_suffix(".parquet"))
            .and_then(|index| index.parse::<usize>().ok());
        if let Some(index) = index.filter(|&index| index >= files_written) {
            if name == format!("part-{index}.parquet").as_str() && entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }
    }
    Ok(())
}

fn writer(path: &Path, schema: &str) -> Result<SerializedFileWriter<File>> {
    let schema = Arc::new(parse_message_type(schema)?);
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_encoding(Encoding::PLAIN)
        .build();
    Ok(SerializedFileWriter::new(
        File::create(path)?,
        schema,
        Arc::new(properties),
    )?)
}

pub fn users(path: &Path, categories: &[&str], days: &[UserDay<'_>]) -> Result<()> {
    let mut writer = writer(path, USERS)?;
    for batch in days.chunks(ROW_GROUP_SIZE) {
        let mut group = writer.next_row_group()?;
        let mut index = 0;
        while let Some(mut column) = group.next_column()? {
            match column.untyped() {
                ColumnWriter::ByteArrayColumnWriter(w) if index == 0 => {
                    let values: Vec<_> =
                        batch.iter().map(|row| ByteArray::from(row.user)).collect();
                    w.write_batch(&values, None, None)?;
                }
                ColumnWriter::ByteArrayColumnWriter(w) => {
                    let values: Vec<_> = batch
                        .iter()
                        .filter_map(|row| row.category)
                        .map(|id| ByteArray::from(categories[id as usize]))
                        .collect();
                    let levels: Vec<_> = batch
                        .iter()
                        .map(|row| i16::from(row.category.is_some()))
                        .collect();
                    w.write_batch(&values, Some(&levels), None)?;
                }
                ColumnWriter::Int32ColumnWriter(w) => {
                    let values: Vec<_> = batch.iter().map(|row| row.day).collect();
                    w.write_batch(&values, None, None)?;
                }
                ColumnWriter::Int64ColumnWriter(w) => {
                    let values: Vec<_> = batch
                        .iter()
                        .map(|row| match index {
                            2 => row.sessions,
                            3 => row.events,
                            _ => row.revenue,
                        })
                        .collect();
                    w.write_batch(&values, None, None)?;
                }
                _ => unreachable!("user output schema"),
            }
            column.close()?;
            index += 1;
        }
        group.close()?;
    }
    writer.close()?;
    Ok(())
}

pub fn campaigns(path: &Path, names: &[(&str, &str)], days: &[CampaignDay]) -> Result<()> {
    let mut writer = writer(path, CAMPAIGNS)?;
    for batch in days.chunks(ROW_GROUP_SIZE) {
        let mut group = writer.next_row_group()?;
        let mut index = 0;
        while let Some(mut column) = group.next_column()? {
            match column.untyped() {
                ColumnWriter::ByteArrayColumnWriter(w) => {
                    let values: Vec<_> = batch
                        .iter()
                        .map(|row| {
                            let (id, channel) = names[row.campaign as usize];
                            ByteArray::from(if index == 0 { id } else { channel })
                        })
                        .collect();
                    w.write_batch(&values, None, None)?;
                }
                ColumnWriter::Int32ColumnWriter(w) => {
                    let values: Vec<_> = batch.iter().map(|row| row.day).collect();
                    w.write_batch(&values, None, None)?;
                }
                ColumnWriter::Int64ColumnWriter(w) => {
                    let values: Vec<_> = batch
                        .iter()
                        .map(|row| match index {
                            3 => row.totals.sessions,
                            4 => row.totals.purchases,
                            5 => row.totals.revenue,
                            _ => row.totals.assists,
                        })
                        .collect();
                    w.write_batch(&values, None, None)?;
                }
                _ => unreachable!("campaign output schema"),
            }
            column.close()?;
            index += 1;
        }
        group.close()?;
    }
    writer.close()?;
    Ok(())
}
