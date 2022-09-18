use std::collections::HashMap;
use std::fmt;

pub type Bytes = Vec<u8>;

// TODO: add thiserror
pub enum DbError {
    TableAlreadyExists(String),
    TableNotFound(String),
    ColumnNotFound(String),
    ColumnAlreadyExists(String),
    InvalidDataType(String),
    InvalidValue(String),
    InvalidQuery(String),
    IoError(std::io::Error),
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
        add: Option<(String, DataType)>,
        drop: Option<String>,
        rename: Option<(String, String)>,
    },
}

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

impl DataType {
    pub fn valid(&self, bytes: &[u8]) -> bool {
        match self {
            DataType::Int => bytes.len() == 8,
            DataType::Float => bytes.len() == 8,
            DataType::Char => bytes.len() == 1,
            DataType::Str => {
                if bytes.len() < 8 {
                    return false;
                }
                let len = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                bytes.len() == 8 + len as usize
            }
        }
    }
}
