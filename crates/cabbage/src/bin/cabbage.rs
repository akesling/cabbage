use std::collections::BTreeMap;

use anyhow::{Context as _, Ok, Result, bail};
use cabbage::proxy::handle_connection;
use clap::Parser;
use tokio::net::TcpListener;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
struct Args {
    #[command(subcommand)]
    command: Command,

    /// Log level pairs of the form <MODULE>:<LEVEL>.
    #[arg(long)]
    log_levels: Option<Vec<String>>,
}

struct GlobalOptions {}

#[derive(clap::Parser, Debug)]
struct HaikuOptions {
    /// Print all haikus
    #[arg(long)]
    all: bool,
}

async fn haiku(_context: &GlobalOptions, options: &HaikuOptions) -> anyhow::Result<()> {
    cabbage::print_haiku(options.all)
}

/// Convert a series of <MODULE>:<LEVEL> pairs into actionable `(module, LevelFilter)` pairs
fn as_level_pairs(config: &[String]) -> Result<Vec<(&str, simplelog::LevelFilter)>> {
    let mut pairs = Vec::with_capacity(config.len());
    for c in config {
        let tokens: Vec<&str> = c.split(":").collect();
        if tokens.len() != 2 {
            bail!("Flag config pair was not of the form <MODULE>:<LEVEL>: '{c}'");
        }
        pairs.push((
            tokens[0],
            match tokens[1].to_lowercase().as_str() {
                "trace" => simplelog::LevelFilter::Trace,
                "debug" => simplelog::LevelFilter::Debug,
                "info" => simplelog::LevelFilter::Info,
                "warn" => simplelog::LevelFilter::Warn,
                "error" => simplelog::LevelFilter::Error,
                _ => bail!("Unrecognized level name in '{c}'"),
            },
        ))
    }

    Ok(pairs)
}

fn initialize_logging(
    module_path_filters: &[(&str, simplelog::LevelFilter)],
) -> anyhow::Result<()> {
    simplelog::CombinedLogger::init(
        module_path_filters
            .iter()
            .map(|(module_path_filter, level)| {
                simplelog::TermLogger::new(
                    *level,
                    simplelog::ConfigBuilder::new()
                        .add_filter_allow(module_path_filter.to_string())
                        .build(),
                    simplelog::TerminalMode::Mixed,
                    simplelog::ColorChoice::Auto,
                ) as Box<dyn simplelog::SharedLogger>
            })
            .collect(),
    )
    .map_err(|e| e.into())
}

#[derive(clap::Parser, Debug)]
struct ProxyOptions {
    #[arg(long, default_value = "127.0.0.1:5000")]
    client: String,

    #[arg(long, default_value = "127.0.0.1:6379")]
    target: String,
}

async fn proxy(_context: &GlobalOptions, options: &ProxyOptions) -> anyhow::Result<()> {
    let client_listener = TcpListener::bind(options.client.clone()).await?;

    log::info!(
        "Proxy listening on {} -> {}",
        options.client,
        options.target
    );

    loop {
        let (client_socket, client_addr) = client_listener.accept().await?;
        let target_addr = options.target.clone();

        log::info!("New connection from {}", client_addr);

        if let Err(e) = handle_connection(client_socket, target_addr).await {
            log::error!("Connection error: {}", e);
        }
    }
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Print a random haiku
    Haiku(HaikuOptions),
    Proxy(ProxyOptions),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let levels_arg: Option<Vec<String>> = args.log_levels;

    let log_levels: Vec<(&str, simplelog::LevelFilter)> = {
        let mut log_levels = BTreeMap::from([
            ("cabbage", simplelog::LevelFilter::Debug),
            // If compiled via Bazel, "cabbage" binary crate module will be named "bin"
            //("bin", simplelog::LevelFilter::Debug),
        ]);

        for (module, level) in levels_arg
            .as_deref()
            .map(as_level_pairs)
            .unwrap_or(Ok(vec![]))
            .context("Log level override parsing failed")?
        {
            log_levels.insert(module, level);
        }
        log_levels.into_iter().collect()
    };
    let _ = initialize_logging(&log_levels[..]);
    log::trace!("Logging initialized, commands parsed...");

    let context = GlobalOptions {};
    match args.command {
        Command::Haiku(options) => haiku(&context, &options).await?,
        Command::Proxy(options) => proxy(&context, &options).await?,
    }
    Ok(())
}
