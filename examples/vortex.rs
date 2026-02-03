use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
use datafusion::{
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    prelude::{SessionConfig, SessionContext},
};
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::file::WriteOptionsSessionExt;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormat;

/// This example demonstrates a schema evolution error in DataFusion with Vortex format.
///
/// Two Vortex files with incompatible schemas for the 'code' field:
/// - File 1: code is UTF8 (string)
/// - File 2: code is Int64 (integer)
///
/// DataFusion will fail when attempting to query both files with a unified schema.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path();

    let vortex_session = VortexSession::default();

    // ============================================================================
    // Step 1: Create first Vortex file with 'code' as UTF8
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

    write_vortex_file(
        &temp_path.join("data_utf8.vortex"),
        &batch_with_string_code,
        &vortex_session,
    )
    .await?;

    // ============================================================================
    // Step 2: Create second Vortex file with 'code' as Int64 (SCHEMA CONFLICT!)
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

    write_vortex_file(
        &temp_path.join("data_int64.vortex"),
        &batch_with_int_code,
        &vortex_session,
    )
    .await?;

    // ============================================================================
    // Step 3: Attempt to query both files with DataFusion
    // ============================================================================
    let ctx = SessionContext::new_with_config(SessionConfig::from_env()?);
    let listing_options = ListingOptions::new(Arc::new(VortexFormat::new(vortex_session)));
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

/// Helper function to write a RecordBatch to a Vortex file
async fn write_vortex_file(
    path: &Path,
    batch: &RecordBatch,
    session: &VortexSession,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = tokio::fs::File::create(path).await?;
    let vortex_array = ArrayRef::from_arrow(batch.clone(), false)?;
    session
        .write_options()
        .write(&mut file, vortex_array.to_array_stream())
        .await?;
    Ok(())
}
