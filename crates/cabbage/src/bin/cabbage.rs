use std::{collections::BTreeMap, net::SocketAddr};

use anyhow::{Context as _, Ok, Result, bail};
use cabbage::{
    middleware::{ProxyLogger, ProxyLoggerLayer},
    proxy::serve,
    service::Resp2Backend,
};
use clap::Parser;
use tokio::net::TcpListener;
use tower::{Layer, service_fn};
use uuid::Uuid;

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

// TODO(akesling): Add connection timeout, etc.
async fn create_proxy_service(
    client_addr: SocketAddr,
    target_addr: String,
) -> anyhow::Result<ProxyLogger<Resp2Backend>> {
    let connection_id = Uuid::new_v4();
    log::info!("New connection from {} (ID#{})", client_addr, connection_id);

    let target_socket = tokio::net::TcpStream::connect(&target_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to target {}: {}", target_addr, e))?;

    log::info!(
        "connection {}: connected with target at: {}",
        connection_id,
        target_addr
    );

    let target_framed =
        tokio_util::codec::Framed::new(target_socket, redis_protocol::codec::Resp2::default());
    let connection_id_string = connection_id.to_string();

    let logger = ProxyLoggerLayer::new(connection_id_string);
    let service = logger.layer(Resp2Backend::new(target_framed));

    Ok(service)
}

async fn proxy(_context: &GlobalOptions, options: &ProxyOptions) -> anyhow::Result<()> {
    let client_listener = TcpListener::bind(options.client.clone()).await?;

    log::info!(
        "Proxy listening on {} -> {}",
        options.client,
        options.target
    );

    let target_addr = options.target.clone();
    let make_service = service_fn(move |client_addr: SocketAddr| {
        let target_addr = target_addr.clone();
        async move { create_proxy_service(client_addr, target_addr).await }
    });

    serve(client_listener, make_service).await
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
