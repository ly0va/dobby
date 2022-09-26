use dobby::core::types::{DataType, FieldSet, TypedValue};
use dobby::grpc::proto::{self, database_client::DatabaseClient};
use prettytable::{csv, format, Cell, Row, Table};
use std::error::Error;
use structopt::StructOpt;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Format {
    Json,
    Ascii,
    Csv,
}

impl std::str::FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "json" => Ok(Format::Json),
            "ascii" => Ok(Format::Ascii),
            "csv" => Ok(Format::Csv),
            _ => Err(format!("Unknown format: {}", s)),
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "dobby", about = "A database engine as poor as a house elf.")]
struct Options {
    #[structopt(short, long, env = "DOBBY_URL")]
    url: String,

    #[structopt(
        short,
        long,
        case_insensitive = true,
        default_value = "ascii",
        possible_values = &["ascii", "json", "csv"]
    )]
    format: Format,

    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    Select {
        #[structopt(short, long)]
        table: String,
        #[structopt(short, long)]
        columns: Vec<String>,
        #[structopt(short = "w", long = "where", parse(try_from_str = parse_key_val))]
        conditions: Vec<(String, TypedValue)>,
    },
    Insert {
        #[structopt(short, long)]
        table: String,
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        values: Vec<(String, TypedValue)>,
    },
    Update {
        #[structopt(short, long)]
        table: String,
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        values: Vec<(String, TypedValue)>,
        #[structopt(short = "w", long = "where", parse(try_from_str = parse_key_val))]
        conditions: Vec<(String, TypedValue)>,
    },
    Delete {
        #[structopt(short, long)]
        table: String,
        #[structopt(short = "w", long = "where", parse(try_from_str = parse_key_val))]
        conditions: Vec<(String, TypedValue)>,
    },
    Drop {
        #[structopt(short, long)]
        table: String,
    },
    Create {
        #[structopt(short, long)]
        table: String,
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        columns: Vec<(String, DataType)>,
    },
    Rename {
        #[structopt(short, long)]
        table: String,
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        columns: Vec<(String, String)>,
    },
}

/// Parse a single key-value pair
fn parse_key_val<'a, T>(s: &'a str) -> Result<(String, T), Box<dyn Error>>
where
    T: TryFrom<&'a str>,
    <T as TryFrom<&'a str>>::Error: Error + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid key=value: no `=` found in `{}`", s))?;
    Ok((s[..pos].to_string(), s[pos + 1..].try_into()?))
}

// TODO: try to use a macro to generate this / reuse existing Query struct
impl From<Command> for proto::Query {
    fn from(command: Command) -> Self {
        let convert = |values: Vec<(String, TypedValue)>| {
            values.into_iter().map(|(k, v)| (k, v.into())).collect()
        };

        match command {
            Command::Select { table, columns, conditions } => proto::Query {
                query: Some(proto::query::Query::Select(proto::Select {
                    from: table,
                    columns,
                    conditions: convert(conditions),
                })),
            },
            Command::Insert { table, values } => proto::Query {
                query: Some(proto::query::Query::Insert(proto::Insert {
                    into: table,
                    values: convert(values),
                })),
            },
            Command::Update { table, values, conditions } => proto::Query {
                query: Some(proto::query::Query::Update(proto::Update {
                    table,
                    set: convert(values),
                    conditions: convert(conditions),
                })),
            },
            Command::Delete { table, conditions } => proto::Query {
                query: Some(proto::query::Query::Delete(proto::Delete {
                    from: table,
                    conditions: convert(conditions),
                })),
            },
            Command::Drop { table } => proto::Query {
                query: Some(proto::query::Query::Drop(proto::Drop { table })),
            },
            Command::Create { table, columns } => proto::Query {
                query: Some(proto::query::Query::Create(proto::Create {
                    table,
                    columns: columns.into_iter().map(|(k, v)| (k, v as i32)).collect(),
                })),
            },
            Command::Rename { table, columns } => proto::Query {
                query: Some(proto::query::Query::Alter(proto::Alter {
                    table,
                    rename: columns.into_iter().collect(),
                })),
            },
        }
    }
}

fn pretty_table(rows: &[FieldSet]) -> Table {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    let columns: Vec<_> = rows.first().unwrap().keys().cloned().collect();
    table.set_titles(Row::new(columns.iter().map(|s| Cell::new(s)).collect()));

    for row in rows {
        table.add_row(Row::new(
            columns
                .iter()
                .map(|c| Cell::new(&serde_json::to_string_pretty(&row[c]).unwrap()))
                .collect(),
        ));
    }

    table
}

#[tokio::main]
async fn main() {
    let opt = Options::from_args();

    let mut client = DatabaseClient::connect(opt.url).await.unwrap();
    let response = client
        .execute(tonic::Request::new(opt.command.into()))
        .await;

    match response {
        Ok(response) => {
            let response: Vec<FieldSet> = response.into_inner().into();
            if response.is_empty() {
                return;
            }

            let table = pretty_table(&response);

            match opt.format {
                Format::Json => {
                    println!("{}", serde_json::to_string_pretty(&response).unwrap());
                }
                Format::Ascii => {
                    table.printstd();
                }
                Format::Csv => {
                    let writer = csv::Writer::from_writer(std::io::stdout());
                    table.to_csv_writer(writer).unwrap();
                }
            }
        }
        Err(err) => {
            eprintln!("Error: {}", err.message());
            std::process::exit(1);
        }
    }
}
