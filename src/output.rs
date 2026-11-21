use crate::pipeline::{CampaignTotals, Dimensions, UserDay};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::errors::Result as ParquetResult;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::parser::parse_message_type;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const ROW_GROUP: usize = 128 * 1024;
const USER_SCHEMA: &str = "message user_daily {
    REQUIRED BINARY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BINARY top_category (UTF8);
}";
const CAMPAIGN_SCHEMA: &str = "message campaign_daily {
    REQUIRED BINARY campaign_id (UTF8);
    REQUIRED BINARY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

pub fn write_tables(
    root: &Path,
    users: &[UserDay<'_>],
    campaigns: CampaignTotals,
    dimensions: &Dimensions<'_>,
) -> Result<()> {
    let rows: Vec<_> = campaigns.into_iter().collect();
    let (users, campaigns) = rayon::join(
        || {
            write_table(root, "user_daily", USER_SCHEMA, users, |rows, group| {
                bytes(group, rows.iter().map(|row| row.person))?;
                dates(group, rows.iter().map(|row| row.day))?;
                integers(group, rows.iter().map(|row| row.sessions))?;
                integers(group, rows.iter().map(|row| row.events))?;
                integers(group, rows.iter().map(|row| row.revenue))?;
                let levels: Vec<_> = rows
                    .iter()
                    .map(|row| i16::from(row.top_category.is_some()))
                    .collect();
                let values: Vec<_> = rows
                    .iter()
                    .filter_map(|row| row.top_category)
                    .map(|id| ByteArray::from(dimensions.categories[id as usize].to_vec()))
                    .collect();
                let mut column = group.next_column()?.unwrap();
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&values, Some(&levels), None)?;
                column.close()
            })
        },
        || {
            write_table(
                root,
                "campaign_daily",
                CAMPAIGN_SCHEMA,
                &rows,
                |rows, group| {
                    bytes(
                        group,
                        rows.iter()
                            .map(|(key, _)| dimensions.campaign_labels[key.0 as usize].0),
                    )?;
                    bytes(
                        group,
                        rows.iter()
                            .map(|(key, _)| dimensions.campaign_labels[key.0 as usize].1),
                    )?;
                    dates(group, rows.iter().map(|(key, _)| key.1))?;
                    integers(group, rows.iter().map(|(_, row)| row.sessions))?;
                    integers(group, rows.iter().map(|(_, row)| row.purchasing))?;
                    integers(group, rows.iter().map(|(_, row)| row.revenue))?;
                    integers(group, rows.iter().map(|(_, row)| row.assists))
                },
            )
        },
    );
    users?;
    campaigns?;
    Ok(())
}

type RowGroup<'a> = SerializedRowGroupWriter<'a, BufWriter<File>>;

fn write_table<T, F>(root: &Path, name: &str, schema: &str, rows: &[T], write: F) -> Result<()>
where
    F: Fn(&[T], &mut RowGroup<'_>) -> ParquetResult<()>,
{
    let directory = root.join(name);
    fs::create_dir_all(&directory)?;
    let pending = directory.join(".part-00000.parquet");
    let file = BufWriter::with_capacity(1024 * 1024, File::create(&pending)?);
    let options = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let mut writer = SerializedFileWriter::new(
        file,
        Arc::new(parse_message_type(schema)?),
        Arc::new(options),
    )?;
    for part in rows
        .chunks(ROW_GROUP)
        .chain(rows.is_empty().then_some(rows))
    {
        let mut group = writer.next_row_group()?;
        write(part, &mut group)?;
        group.close()?;
    }
    writer.finish()?;
    writer.inner_mut().flush()?;
    fs::rename(pending, directory.join("part-00000.parquet"))?;
    Ok(())
}

fn bytes<'a>(group: &mut RowGroup<'_>, data: impl Iterator<Item = &'a [u8]>) -> ParquetResult<()> {
    let values: Vec<_> = data.map(|value| ByteArray::from(value.to_vec())).collect();
    let mut column = group.next_column()?.unwrap();
    column
        .typed::<ByteArrayType>()
        .write_batch(&values, None, None)?;
    column.close()
}

fn dates(group: &mut RowGroup<'_>, data: impl Iterator<Item = i32>) -> ParquetResult<()> {
    let values: Vec<_> = data.collect();
    let mut column = group.next_column()?.unwrap();
    column
        .typed::<Int32Type>()
        .write_batch(&values, None, None)?;
    column.close()
}

fn integers(group: &mut RowGroup<'_>, data: impl Iterator<Item = i64>) -> ParquetResult<()> {
    let values: Vec<_> = data.collect();
    let mut column = group.next_column()?.unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&values, None, None)?;
    column.close()
}
