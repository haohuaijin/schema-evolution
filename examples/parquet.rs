use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
use datafusion::{
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    prelude::{SessionConfig, SessionContext},
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path();

    // Create first file: code field as UTF8
    let schema1 = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("code", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]);

    let id1 = Int64Array::from(vec![1, 2, 3]);
    let code1 = StringArray::from(vec!["A100", "B200", "C300"]);
    let value1 = Int64Array::from(vec![100, 200, 300]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema1.clone()),
        vec![Arc::new(id1), Arc::new(code1), Arc::new(value1)],
    )?;

    let file1_path = temp_path.join("data_utf8.parquet");
    let file1 = std::fs::File::create(&file1_path)?;
    let props = WriterProperties::builder().build();
    let mut writer1 = ArrowWriter::try_new(file1, batch1.schema(), Some(props))?;
    writer1.write(&batch1)?;
    writer1.close()?;

    // Create second file: code field as Int64
    let schema2 = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("code", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]);

    let id2 = Int64Array::from(vec![4, 5, 6]);
    let code2 = Int64Array::from(vec![400, 500, 600]);
    let value2 = Int64Array::from(vec![400, 500, 600]);

    let batch2 = RecordBatch::try_new(
        Arc::new(schema2),
        vec![Arc::new(id2), Arc::new(code2), Arc::new(value2)],
    )?;

    let file2_path = temp_path.join("data_int64.parquet");
    let file2 = std::fs::File::create(&file2_path)?;
    let props = WriterProperties::builder().build();
    let mut writer2 = ArrowWriter::try_new(file2, batch2.schema(), Some(props))?;
    writer2.write(&batch2)?;
    writer2.close()?;

    let config = SessionConfig::from_env()?;
    let ctx = SessionContext::new_with_config(config);

    let file_options = ListingOptions::new(Arc::new(
        datafusion::datasource::file_format::parquet::ParquetFormat::default(),
    ))
    .with_file_extension(".parquet");

    let prefix = ListingTableUrl::parse(temp_path.to_str().unwrap())?;
    let listing_config = ListingTableConfig::new(prefix.clone())
        .with_listing_options(file_options)
        .with_schema(Arc::new(schema1.clone()))
        .with_expr_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory {}));

    let table = ListingTable::try_new(listing_config)?;
    ctx.register_table("test_data", Arc::new(table))?;

    let sql = "SELECT * FROM test_data ORDER BY id";
    let _ = ctx.sql(sql).await?.show().await?;

    // Keep temp dir for inspection
    let temp_path_str = temp_path.to_string_lossy().to_string();
    let _ = temp_dir.keep();
    println!("Test completed. Files preserved in: {}", temp_path_str);

    Ok(())
}
