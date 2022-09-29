use dobby::core::types::{DataType, TypedValue};
use dobby::grpc::proto;
use std::error::Error;
use structopt::{clap::AppSettings, StructOpt};

#[derive(Debug, StructOpt)]
#[structopt(no_version, setting = AppSettings::NoBinaryName)]
pub enum Command {
    /// Read rows from the table
    #[structopt(setting = AppSettings::DisableVersion)]
    Select {
        #[structopt(short, long)]
        table: String,
        #[structopt(short, long)]
        columns: Vec<String>,
        #[structopt(short = "w", long = "where", parse(try_from_str = parse_key_val))]
        conditions: Vec<(String, TypedValue)>,
    },

    /// Insert a row into the table
    #[structopt(setting = AppSettings::DisableVersion)]
    Insert {
        #[structopt(short, long)]
        table: String,
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        values: Vec<(String, TypedValue)>,
    },

    /// Update rows in the table
    #[structopt(setting = AppSettings::DisableVersion)]
    Update {
        #[structopt(short, long)]
        table: String,
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        values: Vec<(String, TypedValue)>,
        #[structopt(short = "w", long = "where", parse(try_from_str = parse_key_val))]
        conditions: Vec<(String, TypedValue)>,
    },

    /// Delete rows from the table
    #[structopt(setting = AppSettings::DisableVersion)]
    Delete {
        #[structopt(short, long)]
        table: String,
        #[structopt(short = "w", long = "where", parse(try_from_str = parse_key_val))]
        conditions: Vec<(String, TypedValue)>,
    },

    /// Drop the whole table
    #[structopt(setting = AppSettings::DisableVersion)]
    Drop {
        #[structopt(short, long)]
        table: String,
    },

    /// Create a new table
    #[structopt(setting = AppSettings::DisableVersion)]
    Create {
        #[structopt(short, long)]
        table: String,
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        columns: Vec<(String, DataType)>,
    },

    /// Rename columns in the table
    #[structopt(setting = AppSettings::DisableVersion)]
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
