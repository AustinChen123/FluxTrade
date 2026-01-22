mod connector;
mod model;

use tracing::info;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting FluxTrade Data Service...");

    // Application logic will go here
}
