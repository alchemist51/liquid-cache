use clap::{Parser, command};
use datafusion::{error::Result, execution::object_store::ObjectStoreUrl, prelude::*};
use liquid_cache_client::LiquidCacheBuilder;
use liquid_cache_common::CacheMode;
use std::path::Path;
use std::sync::Arc;
use std::thread::sleep;
use std::time;
use std::time::Instant;
use datafusion::config::ConfigOptions;

#[derive(Parser, Clone)]
#[command(name = "Example Client")]
struct CliArgs {
    #[arg(
        long,
        default_value = "SELECT SUM(backend_status_code) FROM parquet_table where backend_status_code == 200"
    )]
    query: String,

    #[arg(
        long,
        default_value = "/Users/abandeji/Public/workplace/rush_exploration/output.parquet"
    )]
    file_path: String,

    #[arg(long, default_value = "http://localhost:15214")]
    cache_server: String,
}


#[tokio::main]
pub async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let path = Path::new(&args.file_path);

    let ctx = LiquidCacheBuilder::new(args.cache_server.clone())
        .with_object_store(ObjectStoreUrl::parse("file:///")?, None)
        .with_cache_mode(CacheMode::Arrow)
        .build(SessionConfig::from_env()?)?;

    let mut options_mut = ConfigOptions::new();
    // options_mut.execution.parquet.pushdown_filters = true;
    // options_mut.execution.parquet.binary_as_string = true;
    // options_mut.execution.batch_size = 8192 * 2;
    //config.execution.parquet.reorder_filters = true;
    // let ctx = SessionContext::new_with_config(SessionConfig::from(options_mut));
    //
    // let ctx = Arc::new(ctx);

    let file_path = if path.is_dir() {
        let entries = std::fs::read_dir(path)?;
        let first_parquet = entries
            .filter_map(Result::ok)
            .find(|entry| {
                entry.path()
                    .extension()
                    .map_or(false, |ext| ext == "parquet")
            })
            .ok_or_else(|| datafusion::error::DataFusionError::Execution("No parquet files found in directory".to_string()))?;
        first_parquet.path()
    } else {
        path.to_path_buf()
    };

    println!("Reading from file: {:?}", file_path);

    ctx.register_parquet(
        "parquet_table",
        file_path.to_str().unwrap(),
        Default::default(),
    )
        .await?;

    loop {
        println!("\nEnter query (or 'exit' to quit):");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        let input = input.trim();
        if input.eq_ignore_ascii_case("exit") {
            break;
        }

        let query = if input.is_empty() { &args.query } else { input };

        println!("Executing query: {}", query);
        let start = Instant::now();

        match ctx.sql(query).await {
            Ok(df) => {
                match df.show().await {
                    Ok(_) => {
                        let duration = start.elapsed();
                        println!("Query completed in: {:?}", duration);
                    }
                    Err(e) => println!("Error showing results: {}", e),
                }
            }
            Err(e) => println!("Error executing query: {}", e),
        }

    }

    Ok(())
}