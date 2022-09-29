use super::{command::Command, format::Format, helpers::DobbyHelper};

use dobby::core::types::ColumnSet;
use dobby::grpc::proto::database_client::DatabaseClient;

use colored::Colorize;
use prettytable::{csv, format::consts, Row, Table as PrettyTable};
use rustyline::Editor;
use structopt::StructOpt;
use tonic::{transport::Channel, Request};

#[derive(Debug)]
pub struct Repl {
    client: DatabaseClient<Channel>,
    editor: Editor<DobbyHelper>,
    format: Format,
}

impl Repl {
    pub async fn init(address: String, format: Format) -> Self {
        let mut editor = Editor::<DobbyHelper>::new().expect("Failed to init readline");
        editor.set_helper(Some(DobbyHelper::default()));
        Self {
            client: DatabaseClient::connect(address)
                .await
                .expect("Failed to connect to server"),
            editor,
            format,
        }
    }

    fn get_table(rows: &[ColumnSet]) -> PrettyTable {
        let columns: Vec<String> = if let Some(first) = rows.first() {
            first.keys().cloned().collect()
        } else {
            return PrettyTable::new();
        };

        let mut table: PrettyTable = rows
            .iter()
            .map(|row| columns.iter().map(|c| row[c].clone()).collect::<Row>())
            .collect();

        table.set_titles(columns.iter().collect::<Row>());
        table.set_format(*consts::FORMAT_NO_LINESEP_WITH_TITLE);
        table
    }

    pub fn print_rows(&self, rows: Vec<ColumnSet>) {
        if rows.is_empty() {
            return;
        }

        match self.format {
            Format::Json => {
                println!("{}", serde_json::to_string_pretty(&rows).unwrap());
            }
            Format::Ascii => {
                Self::get_table(&rows).printstd();
            }
            Format::Csv => {
                let writer = csv::Writer::from_writer(std::io::stdout());
                Self::get_table(&rows).to_csv_writer(writer).unwrap();
            }
            Format::Html => {
                let mut out = std::io::stdout();
                Self::get_table(&rows).print_html(&mut out).unwrap();
            }
        }
    }

    pub async fn execute(&mut self, command: String) -> Result<Vec<ColumnSet>, String> {
        // parse the command
        // NOTE: this makes it impossible for strings to have whitespace inside -
        // consider using `shlex` parser.
        let command =
            Command::from_iter_safe(command.split_whitespace()).map_err(|e| e.to_string())?;

        // execute the command
        let response = self
            .client
            .execute(Request::new(command.into()))
            .await
            .map_err(|e| format!("{} {}\n", "error:".red().bold(), e.message()))?;

        Ok(response.into_inner().into())
    }

    pub async fn run(&mut self) {
        loop {
            // read the command
            let readline = self.editor.readline("db> ");
            match readline {
                Ok(line) => {
                    self.editor.add_history_entry(line.as_str());

                    // print the response
                    match self.execute(line).await {
                        Ok(response) => {
                            self.print_rows(response);
                            println!();
                        }
                        Err(e) => {
                            println!("{}", e);
                        }
                    }
                }
                Err(_) => break,
            }
        }
    }
}
