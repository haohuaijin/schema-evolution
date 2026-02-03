use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
use datafusion::{
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    prelude::{SessionConfig, SessionContext},
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

/// This example demonstrates a schema evolution error in DataFusion/Vortex.
///
/// Two Parquet files with incompatible schemas for the 'code' field:
/// - File 1: code is UTF8 (string)
/// - File 2: code is Int64 (integer)
///
/// DataFusion will fail when attempting to query both files with a unified schema.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path();

    // ============================================================================
    // Step 1: Create first Parquet file with 'code' as UTF8
    // ============================================================================
    let schema_with_string_code = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("code", DataType::Utf8, false), // UTF8 type
        Field::new("value", DataType::Int64, false),
    ]);

    let batch_with_string_code = RecordBatch::try_new(
        Arc::new(schema_with_string_code.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A100", "B200", "C300"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;

    write_parquet_file(
        &temp_path.join("data_utf8.parquet"),
        &batch_with_string_code,
    )?;

    // ============================================================================
    // Step 2: Create second Parquet file with 'code' as Int64 (SCHEMA CONFLICT!)
    // ============================================================================
    let schema_with_int_code = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("code", DataType::Int64, false), // Int64 type - DIFFERENT!
        Field::new("value", DataType::Int64, false),
    ]);

    let batch_with_int_code = RecordBatch::try_new(
        Arc::new(schema_with_int_code),
        vec![
            Arc::new(Int64Array::from(vec![4, 5, 6])),
            Arc::new(Int64Array::from(vec![400, 500, 600])),
            Arc::new(Int64Array::from(vec![400, 500, 600])),
        ],
    )?;

    write_parquet_file(&temp_path.join("data_int64.parquet"), &batch_with_int_code)?;

    // ============================================================================
    // Step 3: Attempt to query both files with DataFusion
    // ============================================================================
    let ctx = SessionContext::new_with_config(SessionConfig::from_env()?);
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let table_url = ListingTableUrl::parse(temp_path.to_str().unwrap())?;
    let table_config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(Arc::new(schema_with_string_code))
        .with_expr_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory {}));

    let listing_table = ListingTable::try_new(table_config)?;
    ctx.register_table("test_data", Arc::new(listing_table))?;

    let result = ctx
        .sql("SELECT * FROM test_data ORDER BY id")
        .await?
        .show()
        .await;

    match result {
        Ok(_) => println!("Query succeeded unexpectedly"),
        Err(e) => println!("Schema evolution error occurred:\n{}", e),
    }

    println!("\nFiles preserved in: {}", temp_path.display());
    let _ = temp_dir.keep();

    Ok(())
}

/// Helper function to write a RecordBatch to a Parquet file
fn write_parquet_file(path: &Path, batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}
