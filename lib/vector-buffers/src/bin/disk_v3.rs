use std::io::Write;

#[tokio::main]
async fn main() {
    if let Err(error) = vector_buffers::disk_v3_cli::run().await {
        let mut stderr = std::io::stderr().lock();
        let _result = writeln!(stderr, "disk-v3: {error}");
        std::process::exit(1);
    }
}
