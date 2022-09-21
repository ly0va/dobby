use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io;
use thiserror::Error;
use warp::http::StatusCode;

pub type FieldSet = HashMap<String, TypedValue>;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Table {0} already exists")]
    TableAlreadyExists(String),

    #[error("Table {0} not found")]
    TableNotFound(String),

    #[error("Column {0} not found in table {1}")]
    ColumnNotFound(String, String),

    #[error("Name {0} cannot be used for a table or a column")]
    InvalidName(String),

    #[error("Invalid value {0:?} for datatype {1:?}")]
    InvalidValue(TypedValue, DataType),

    #[error("Incomplete data - missing {0} for table {1}")]
    IncompleteData(String, String),

    #[error("IO Error")]
    IoError(#[from] std::io::Error),
}

impl warp::reject::Reject for DbError {}

impl Serialize for DbError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl DbError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            DbError::TableAlreadyExists(_) => StatusCode::CONFLICT,
            DbError::TableNotFound(_) => StatusCode::NOT_FOUND,
            DbError::ColumnNotFound(_, _) => StatusCode::NOT_FOUND,
            DbError::InvalidName(_) => StatusCode::BAD_REQUEST,
            DbError::InvalidValue(_, _) => StatusCode::BAD_REQUEST,
            DbError::IncompleteData(_, _) => StatusCode::BAD_REQUEST,
            DbError::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Query {
    Select {
        from: String,
        columns: Vec<String>,
        conditions: FieldSet,
    },
    Insert {
        into: String,
        values: FieldSet,
    },
    Update {
        table: String,
        set: FieldSet,
        conditions: FieldSet,
    },
    Delete {
        from: String,
        conditions: FieldSet,
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
        rename: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TypedValue {
    Int(i64),
    Float(f64),
    Char(char),
    Str(String),
}

#[derive(Copy, Clone, PartialEq, Eq, Deserialize)]
pub enum DataType {
    Int,
    Float,
    Char,
    Str,
}

impl TypedValue {
    pub fn data_type(&self) -> DataType {
        match self {
            TypedValue::Int(_) => DataType::Int,
            TypedValue::Float(_) => DataType::Float,
            TypedValue::Char(_) => DataType::Char,
            TypedValue::Str(_) => DataType::Str,
        }
    }

    pub fn read<R: io::Read>(data_type: DataType, reader: &mut R) -> Result<Self, io::Error> {
        match data_type {
            DataType::Int => {
                let mut buf = [0; 8];
                reader.read_exact(&mut buf)?;
                Ok(i64::from_le_bytes(buf).into())
            }
            DataType::Float => {
                let mut buf = [0; 8];
                reader.read_exact(&mut buf)?;
                Ok(f64::from_le_bytes(buf).into())
            }
            DataType::Char => {
                let mut buf = [0; 1];
                reader.read_exact(&mut buf)?;
                Ok(char::from(buf[0]).into())
            }
            DataType::Str => {
                let mut length = [0; 8];
                reader.read_exact(&mut length)?;
                let length = u64::from_le_bytes(length);
                let mut buf = vec![0; length as usize];
                reader.read_exact(&mut buf)?;
                Ok(TypedValue::Str(String::from_utf8(buf).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 string")
                })?))
            }
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            TypedValue::Int(i) => i.to_le_bytes().to_vec(),
            TypedValue::Float(f) => f.to_le_bytes().to_vec(),
            TypedValue::Char(c) => vec![c as u8],
            TypedValue::Str(s) => {
                let bytes = s.into_bytes();
                let length = (bytes.len() as u64).to_le_bytes().to_vec();
                [length, bytes].concat()
            }
        }
    }

    pub fn coerce(self, to: DataType) -> Result<Self, DbError> {
        if self.data_type() == to {
            return Ok(self);
        }

        match (&self, to) {
            (TypedValue::Str(s), DataType::Char) => {
                if s.len() == 1 {
                    Ok(TypedValue::Char(s.chars().next().unwrap()))
                } else {
                    Err(DbError::InvalidValue(self, to))
                }
            }
            (TypedValue::Str(s), DataType::Int) => s
                .parse::<i64>()
                .map(TypedValue::Int)
                .map_err(|_| DbError::InvalidValue(self, to)),
            (TypedValue::Str(s), DataType::Float) => s
                .parse::<f64>()
                .map(TypedValue::Float)
                .map_err(|_| DbError::InvalidValue(self, to)),

            (TypedValue::Char(c), DataType::Str) => Ok(TypedValue::Str(c.to_string())),
            (TypedValue::Char(c), DataType::Int) => c
                .to_string()
                .parse::<i64>()
                .map(TypedValue::Int)
                .map_err(|_| DbError::InvalidValue(self, to)),
            (TypedValue::Char(c), DataType::Float) => c
                .to_string()
                .parse::<f64>()
                .map(TypedValue::Float)
                .map_err(|_| DbError::InvalidValue(self, to)),

            (TypedValue::Int(i), DataType::Float) => Ok(TypedValue::Float(*i as f64)),
            (v, _) => Err(DbError::InvalidValue(v.clone(), to)),
        }
    }
}

impl From<i64> for TypedValue {
    fn from(value: i64) -> Self {
        TypedValue::Int(value)
    }
}

impl From<f64> for TypedValue {
    fn from(value: f64) -> Self {
        TypedValue::Float(value)
    }
}

impl From<char> for TypedValue {
    fn from(value: char) -> Self {
        TypedValue::Char(value)
    }
}

impl From<String> for TypedValue {
    fn from(value: String) -> Self {
        TypedValue::Str(value)
    }
}

impl From<&str> for TypedValue {
    fn from(value: &str) -> Self {
        TypedValue::Str(value.to_string())
    }
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
