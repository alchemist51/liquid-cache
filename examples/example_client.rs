use clap::{Parser, command};
use datafusion::{error::Result, execution::object_store::ObjectStoreUrl, prelude::*};
use liquid_cache_client::LiquidCacheBuilder;
use liquid_cache_common::CacheMode;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use reqwest;

#[derive(Parser, Clone)]
#[command(name = "Example Client")]
struct CliArgs {
    #[arg(
        long,
        default_value = "SELECT sum(target_status_code) FROM parquet_table where target_status_code=200"
    )]
    query: String,

    #[arg(
        long,
        default_value = "/Users/gbh/Downloads/generation-1-optimized.parquet"
    )]
    file_path: String,

    #[arg(long, default_value = "http://localhost:15214")]
    cache_server: String,
}

// select sum(target_status_code) from parquet_table where timestamp>1747469204820 and timestamp<1747469653634 and target_status_code=500
async fn get_cache_info(admin_port: u16) -> Result<String> {
    let client = reqwest::Client::new();
    let url = format!("http://localhost:{}/cache_info", admin_port);
    let response = client.get(&url).send().await.unwrap();
    Ok(response.text().await.unwrap())
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let path = Path::new(&args.file_path);

    let ctx = LiquidCacheBuilder::new(args.cache_server.clone())
        .with_object_store(ObjectStoreUrl::parse("file:///")?, None)
        .with_cache_mode(CacheMode::Liquid)
        .build(SessionConfig::from_env()?)?;

    //let ctx  = SessionContext::new();
    let ctx = Arc::new(ctx);

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

    println!("\nInitial cache state:");
    println!("{}", get_cache_info(8080).await?);

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

                        // Get cache statistics after query
                        println!("\nCache state after query:");
                        println!("{}", get_cache_info(8080).await?);
                    }
                    Err(e) => println!("Error showing results: {}", e),
                }
            }
            Err(e) => println!("Error executing query: {}", e),
        }
    }

    Ok(())
}