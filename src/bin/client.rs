use cli::{format::Format, Repl};
use structopt::StructOpt;

mod cli;

/// A database engine as poor as a house elf
#[derive(Debug, StructOpt)]
#[structopt(name = "dobby")]
struct Options {
    /// URL of the dobby server
    #[structopt(short, long, env = "DOBBY_URL")]
    url: String,

    /// The output format
    #[structopt(
        short,
        long,
        case_insensitive = true,
        default_value = "ascii",
        possible_values = &["ascii", "json", "csv", "html"]
    )]
    format: Format,
}

#[tokio::main]
async fn main() {
    let opt = Options::from_args();
    let mut repl = Repl::init(opt.url, opt.format).await;
    repl.run().await;
}
