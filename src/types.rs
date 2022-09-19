use std::collections::HashMap;
use std::fmt;
use thiserror::Error;

pub type Bytes = Vec<u8>;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Table {0} already exists")]
    TableAlreadyExists(String),

    #[error("Table {0} not found")]
    TableNotFound(String),

    #[error("Column {0} already exists in table {1}")]
    ColumnAlreadyExists(String, String),

    #[error("Column {0} not found in table {1}")]
    ColumnNotFound(String, String),

    #[error("Invalid datatype {0} in found in schema")]
    InvalidDataType(String),

    #[error("Name {0} cannot be used for a table or a column")]
    InvalidName(String),

    #[error("Invalid value {0} for datatype {1:?}")]
    InvalidValue(String, DataType),

    #[error("Invalid query {0}")]
    InvalidQuery(String),

    #[error("IO Error")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
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
        add: Option<(String, DataType)>,
        drop: Option<String>,
        rename: Option<(String, String)>,
    },
}

#[derive(Copy, Clone, PartialEq, Eq)]
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
            DataType::Str => write!(f, "string"),
        }
    }
}

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        match s {
            "int" => DataType::Int,
            "float" => DataType::Float,
            "char" => DataType::Char,
            "string" => DataType::Str,
            _ => panic!("Unknown data type: {}", s),
        }
    }
}

impl DataType {
    pub fn valid(&self, bytes: &[u8]) -> bool {
        match self {
            DataType::Int => bytes.len() == 8,
            DataType::Float => bytes.len() == 8,
            DataType::Char => bytes.len() == 1,
            DataType::Str => String::from_utf8(bytes.to_vec()).is_ok(),
        }
    }
}
