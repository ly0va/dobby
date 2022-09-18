use std::collections::{hash_map::Entry, HashMap};
use std::fmt;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

type Bytes = Vec<u8>;

#[derive(Copy, Clone)]
pub enum DataType {
    Int,
    Float,
    Char,
    Str,
}

impl fmt::Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            DataType::Int => write!(f, "int"),
            DataType::Float => write!(f, "float"),
            DataType::Char => write!(f, "char"),
            DataType::Str => write!(f, "str"),
        }
    }
}

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        match s {
            "int" => DataType::Int,
            "float" => DataType::Float,
            "char" => DataType::Char,
            "str" => DataType::Str,
            _ => panic!("Unknown data type: {}", s),
        }
    }
}

// TODO: add thiserror
pub enum DbError {
    TableAlreadyExists(String),
    TableNotFound(String),
    ColumnNotFound(String),
    ColumnAlreadyExists(String),
    InvalidDataType(String),
    InvalidValue(String),
    InvalidQuery(String),
    IoError(io::Error),
}

pub enum Query {
    Select {
        from: String,
        columns: Vec<String>,
        conditions: HashMap<String, Bytes>,
    },
    Insert {
        into: String,
        values: HashMap<String, Bytes>,
    },
    Update {
        table: String,
        set: HashMap<String, Bytes>,
        conditions: HashMap<String, Bytes>,
    },
    Delete {
        from: String,
        conditions: HashMap<String, Bytes>,
    },
    Create {
        table: String,
        columns: Vec<(String, DataType)>,
    },
    Drop {
        table: String,
    },
    Alter {
        table: String,
        add: HashMap<String, DataType>,
        drop: Vec<String>,
        rename: HashMap<String, String>,
    },
}

pub struct Database {
    pub connections: Vec<Connection>,
    // pub tables: Vec<std::fs::File>,
    pub path: std::path::PathBuf,
    pub schema: Schema,
}

impl Database {
    pub fn open(path: PathBuf) -> Self {
        let schema = Schema::load(&path).expect("Failed to load schema");
        Database {
            connections: Vec::new(),
            path,
            schema,
        }
    }

    pub fn create(path: PathBuf, name: String) -> Self {
        let schema = Schema::new(name);
        Database {
            connections: Vec::new(),
            path,
            schema,
        }
    }
}

impl Database {
    pub fn process_query(&mut self, query: Query) -> Result<(), DbError> {
        match query {
            Query::Select { .. } => {}
            Query::Insert { .. } => {}
            Query::Update { .. } => {}
            Query::Delete { .. } => {}
            Query::Create { table, columns } => {
                if let Entry::Vacant(entry) = self.schema.tables.entry(table.clone()) {
                    entry.insert(columns);
                } else {
                    return Err(DbError::TableAlreadyExists(table));
                }
            }
            Query::Drop { table } => {
                if let Entry::Occupied(entry) = self.schema.tables.entry(table.clone()) {
                    entry.remove();
                } else {
                    return Err(DbError::TableNotFound(table));
                }
            }
            Query::Alter {
                table,
                add,
                drop,
                rename,
            } => {
                if let Entry::Occupied(mut entry) = self.schema.tables.entry(table.clone()) {
                    for name in drop {
                        if let Some(index) = entry.get().iter().position(|(n, _)| n == &name) {
                            entry.get_mut().remove(index);
                        } else {
                            return Err(DbError::ColumnNotFound(name));
                        }
                    }

                    for (old_name, new_name) in rename {
                        if let Some(column) =
                            entry.get_mut().iter_mut().find(|(n, _)| n == &old_name)
                        {
                            column.0 = new_name;
                        } else {
                            return Err(DbError::ColumnNotFound(old_name));
                        }
                    }

                    for (name, _) in entry.get() {
                        if add.contains_key(name) {
                            return Err(DbError::ColumnAlreadyExists(name.clone()));
                        }
                    }
                    for (name, data_type) in add {
                        entry.get_mut().push((name, data_type));
                    }
                } else {
                    return Err(DbError::TableNotFound(table));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub tables: HashMap<String, Vec<(String, DataType)>>,
    pub name: String,
}

impl Schema {
    pub fn new(name: String) -> Self {
        Schema {
            tables: HashMap::new(),
            name,
        }
    }

    pub fn load(path: &Path) -> Result<Schema, io::Error> {
        let file = std::fs::File::open(path.join("schema"))?;
        let mut reader = io::BufReader::new(file).lines();
        let mut tables = HashMap::new();
        let name = reader.next().unwrap()?;
        for line in reader {
            let line = line?;
            let (table, columns) = line.split_once("::").unwrap();
            for column in columns.split(',') {
                let (column, data_type) = column.split_once(':').unwrap();
                tables
                    .entry(table.to_string())
                    .or_insert_with(Vec::new)
                    .push((column.to_string(), data_type.into()));
            }
        }
        Ok(Schema { tables, name })
    }

    pub fn dump(&self, path: &Path) -> Result<(), io::Error> {
        let mut file = std::fs::File::create(path.join(".schema"))?;
        file.write_all(self.name.as_bytes())?;
        file.write_all(b"\n")?;
        for (table, columns) in &self.tables {
            let mut table_schema: String = columns
                .iter()
                .map(|(column, data_type)| format!("{}:{:?},", column, data_type))
                .collect();
            table_schema.pop();
            file.write_all(format!("{}::{}\n", table, table_schema).as_bytes())?;
        }
        Ok(())
    }
}

pub struct Connection;
