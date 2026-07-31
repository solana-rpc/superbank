use superbank_solparq::read::config::Cli;

#[tokio::main]
async fn main() {
    if let Err(err) = superbank_solparq::read::run(Cli::parse_args()).await {
        eprintln!("{err:?}");
        std::process::exit(1);
    }
}
