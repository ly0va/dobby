use crate::core::types::{DataType, TypedValue};
use crate::grpc::proto;
use std::error::Error;
use structopt::{clap::AppSettings, StructOpt};

#[derive(Debug, StructOpt)]
#[structopt(no_version, setting = AppSettings::NoBinaryName)]
pub enum Command {
    /// Read rows from the table
    #[structopt(setting = AppSettings::DisableVersion)]
    Select {
        /// The table to read from
        #[structopt(short, long)]
        table: String,
        /// The columns to read (table projection)
        #[structopt(short, long)]
        columns: Vec<String>,
        /// The filter to apply to the rows, in the form of column=value
        #[structopt(short = "w", long = "where", parse(try_from_str = parse_key_val))]
        conditions: Vec<(String, TypedValue)>,
    },

    /// Insert a row into the table
    #[structopt(setting = AppSettings::DisableVersion)]
    Insert {
        /// The table to insert into
        #[structopt(short, long)]
        table: String,
        /// Space-separated values to insert, specified as column=value
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        values: Vec<(String, TypedValue)>,
    },

    /// Update rows in the table
    #[structopt(setting = AppSettings::DisableVersion)]
    Update {
        /// The table to update
        #[structopt(short, long)]
        table: String,
        /// The columns to update, specified as column=value
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        values: Vec<(String, TypedValue)>,
        /// The filter to apply to the rows, in the form of column=value
        #[structopt(short = "w", long = "where", parse(try_from_str = parse_key_val))]
        conditions: Vec<(String, TypedValue)>,
    },

    /// Delete rows from the table
    #[structopt(setting = AppSettings::DisableVersion)]
    Delete {
        /// The table to delete from
        #[structopt(short, long)]
        table: String,
        /// The filter to apply to the rows, in the form of column=value
        #[structopt(short = "w", long = "where", parse(try_from_str = parse_key_val))]
        conditions: Vec<(String, TypedValue)>,
    },

    /// Drop the whole table
    #[structopt(setting = AppSettings::DisableVersion)]
    Drop {
        /// The table to drop
        #[structopt(short, long)]
        table: String,
    },

    /// Create a new table
    #[structopt(setting = AppSettings::DisableVersion)]
    Create {
        /// The name of the new table
        #[structopt(short, long)]
        table: String,
        /// The columns to create, specified as column=type
        /// where type is one of: int, float, char, string, char_invl, string_invl
        #[structopt(short, long, parse(try_from_str = parse_key_val))]
        columns: Vec<(String, DataType)>,
    },

    /// Rename columns in the table
    #[structopt(setting = AppSettings::DisableVersion)]
    Rename {
        /// The table to rename columns in
        #[structopt(short, long)]
        table: String,
        /// The columns to rename, specified as old=new
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
