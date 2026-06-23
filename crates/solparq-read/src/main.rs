use solparq_read::config::Cli;

#[tokio::main]
async fn main() {
    if let Err(err) = solparq_read::run(Cli::parse_args()).await {
        eprintln!("{err:?}");
        std::process::exit(1);
    }
}
