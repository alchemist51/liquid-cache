use std::net::SocketAddr;
use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::prelude::SessionContext;
use liquid_cache_common::{CacheEvictionStrategy, CacheMode};
use liquid_cache_server::{run_admin_server, LiquidCacheService};
use tonic::transport::Server;
use std::sync::Arc;
use fastrace_tonic::FastraceServerLayer;
use mimalloc::MiMalloc;

//  select sum(target_status_code) from parquet_table where target_status_code=400
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let liquid_cache = LiquidCacheService::new(
        SessionContext::new(),
        Some(1024 * 1024 * 1024 * 4),          // max memory cache size 1GB
        Some(tempfile::tempdir()?.into_path()), // disk cache dir
        CacheMode::Parquet,
        CacheEvictionStrategy::Discard,
    )?;

    let liquid_cache = Arc::new(liquid_cache);
    let flight = FlightServiceServer::from_arc(liquid_cache.clone());

    let admin_addr: SocketAddr = "127.0.0.1:\
    ".parse()?;
    let server_addr: SocketAddr = "0.0.0.0:15214".parse()?;

    println!("Starting admin server on {}", admin_addr);
    println!("Starting main server on {}", server_addr);

    // Run both servers concurrently
    tokio::select! {
        result = Server::builder()
            .layer(FastraceServerLayer)
            .add_service(flight)
            .serve(server_addr) => {
            result?;
        },
        result = run_admin_server(admin_addr, liquid_cache) => {
            result?;
        },
    }

    Ok(())
}