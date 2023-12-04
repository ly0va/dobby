use std::collections::HashMap;
use std::fmt;
use std::io;

use num::complex::Complex;
use rusqlite::types::ToSqlOutput;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type ColumnSet = HashMap<String, TypedValue>;

#[derive(Debug, Error)]
pub enum DobbyError {
    #[error("Table {0} already exists")]
    TableAlreadyExists(String),

    #[error("Table {0} not found")]
    TableNotFound(String),

    #[error("Column {0} already exists in table {1}")]
    ColumnAlreadyExists(String, String),

    #[error("Can't create a table without columns")]
    NoColumns,

    #[error("Column {0} not found in table {1}")]
    ColumnNotFound(String, String),

    #[error("Name {0} cannot be used for a table or a column")]
    InvalidName(String),

    #[error("Invalid value {0:?} for datatype {1:?}")]
    InvalidValue(TypedValue, DataType),

    #[error("Incomplete data - missing {0} for table {1}")]
    IncompleteData(String, String),

    #[error("Invalid datatype: {0}")]
    InvalidDataType(String),

    #[error("Invalid range: {0} > {1}")]
    InvalidRange(String, String),

    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("SQL Error: {0}")]
    SqlError(#[from] rusqlite::Error),
}

impl Serialize for DobbyError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Debug, Clone)]
pub enum Query {
    Select {
        from: String,
        columns: Vec<String>,
        conditions: ColumnSet,
    },
    Insert {
        into: String,
        values: ColumnSet,
    },
    Update {
        table: String,
        set: ColumnSet,
        conditions: ColumnSet,
    },
    Delete {
        from: String,
        conditions: ColumnSet,
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
    String(String),
    ComplexInt(i64, i64),
    ComplexFloat(f64, f64),
}

#[derive(Copy, Clone, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Int = 0,
    Float = 1,
    Char = 2,
    String = 3,
    ComplexInt = 4,
    ComplexFloat = 5,
}

impl rusqlite::ToSql for TypedValue {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        match self {
            TypedValue::Int(i) => i.to_sql(),
            TypedValue::Float(f) => f.to_sql(),
            TypedValue::String(s) => s.to_sql(),
            TypedValue::Char(c) => Ok(ToSqlOutput::from(c.to_string())),
            TypedValue::ComplexInt(r, i) => Ok(ToSqlOutput::from(Complex::new(*r, *i).to_string())),
            TypedValue::ComplexFloat(r, i) => {
                Ok(ToSqlOutput::from(Complex::new(*r, *i).to_string()))
            }
        }
    }
}

impl TypedValue {
    pub fn validate(&self) -> Result<(), DobbyError> {
        Ok(())
    }

    pub fn data_type(&self) -> DataType {
        match self {
            TypedValue::Int(_) => DataType::Int,
            TypedValue::Float(_) => DataType::Float,
            TypedValue::Char(_) => DataType::Char,
            TypedValue::String(_) => DataType::String,
            TypedValue::ComplexInt(_, _) => DataType::ComplexInt,
            TypedValue::ComplexFloat(_, _) => DataType::ComplexFloat,
        }
    }

    pub fn read<R: io::Read>(data_type: DataType, reader: &mut R) -> Result<Self, io::Error> {
        let mut read_string = || {
            let mut length = [0; 8];
            reader.read_exact(&mut length)?;
            let length = u64::from_le_bytes(length);
            let mut buf = vec![0; length as usize];
            reader.read_exact(&mut buf)?;
            String::from_utf8(buf)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 string"))
        };

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
            DataType::String => Ok(TypedValue::String(read_string()?)),
            DataType::ComplexInt => {
                let mut buf = [0; 8];
                reader.read_exact(&mut buf)?;
                let r = i64::from_le_bytes(buf);
                reader.read_exact(&mut buf)?;
                let i = i64::from_le_bytes(buf);
                Ok(TypedValue::ComplexInt(r, i))
            }
            DataType::ComplexFloat => {
                let mut buf = [0; 8];
                reader.read_exact(&mut buf)?;
                let r = f64::from_le_bytes(buf);
                reader.read_exact(&mut buf)?;
                let i = f64::from_le_bytes(buf);
                Ok(TypedValue::ComplexFloat(r, i))
            }
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        let convert_string = |s: String| {
            let bytes = s.into_bytes();
            let length = (bytes.len() as u64).to_le_bytes().to_vec();
            [length, bytes].concat()
        };

        match self {
            TypedValue::Int(i) => i.to_le_bytes().to_vec(),
            TypedValue::Float(f) => f.to_le_bytes().to_vec(),
            TypedValue::Char(c) => vec![c as u8],
            TypedValue::String(s) => convert_string(s),
            TypedValue::ComplexInt(r, i) => {
                [r.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()].concat()
            }
            TypedValue::ComplexFloat(r, i) => {
                [r.to_le_bytes().to_vec(), i.to_le_bytes().to_vec()].concat()
            }
        }
    }

    pub fn coerce(self, to: DataType) -> Result<Self, DobbyError> {
        let string_to_char = |s: &str| {
            if s.len() == 1 {
                Ok(s.chars().next().unwrap())
            } else {
                Err(DobbyError::InvalidValue(self.clone(), to))
            }
        };

        if self.data_type() == to {
            return Ok(self);
        }

        match (&self, to) {
            (TypedValue::String(s), DataType::Char) => string_to_char(s).map(TypedValue::Char),
            (TypedValue::String(s), DataType::Int) => s
                .parse::<i64>()
                .map(TypedValue::Int)
                .map_err(|_| DobbyError::InvalidValue(self, to)),
            (TypedValue::String(s), DataType::Float) => s
                .parse::<f64>()
                .map(TypedValue::Float)
                .map_err(|_| DobbyError::InvalidValue(self, to)),
            (TypedValue::String(s), DataType::ComplexInt) => s
                .parse::<Complex<i64>>()
                .map(|c| TypedValue::ComplexInt(c.re, c.im))
                .map_err(|_| DobbyError::InvalidValue(self, to)),
            (TypedValue::String(s), DataType::ComplexFloat) => s
                .parse::<Complex<f64>>()
                .map(|c| TypedValue::ComplexFloat(c.re, c.im))
                .map_err(|_| DobbyError::InvalidValue(self, to)),

            (TypedValue::Char(c), DataType::String) => Ok(TypedValue::String(c.to_string())),
            (TypedValue::Char(c), DataType::Int) => c
                .to_string()
                .parse::<i64>()
                .map(TypedValue::Int)
                .map_err(|_| DobbyError::InvalidValue(self, to)),
            (TypedValue::Char(c), DataType::Float) => c
                .to_string()
                .parse::<f64>()
                .map(TypedValue::Float)
                .map_err(|_| DobbyError::InvalidValue(self, to)),
            (TypedValue::Char(c), DataType::ComplexInt) => c
                .to_string()
                .parse::<Complex<i64>>()
                .map(|c| TypedValue::ComplexInt(c.re, c.im))
                .map_err(|_| DobbyError::InvalidValue(self, to)),
            (TypedValue::Char(c), DataType::ComplexFloat) => c
                .to_string()
                .parse::<Complex<f64>>()
                .map(|c| TypedValue::ComplexFloat(c.re, c.im))
                .map_err(|_| DobbyError::InvalidValue(self, to)),

            (TypedValue::Int(i), DataType::Float) => Ok(TypedValue::Float(*i as f64)),
            (TypedValue::ComplexInt(r, i), DataType::ComplexFloat) => {
                Ok(TypedValue::ComplexFloat(*r as f64, *i as f64))
            }
            (v, _) => Err(DobbyError::InvalidValue(v.clone(), to)),
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
        TypedValue::String(value)
    }
}

impl From<&str> for TypedValue {
    fn from(value: &str) -> Self {
        TypedValue::String(value.to_string())
    }
}

impl ToString for TypedValue {
    fn to_string(&self) -> String {
        match self {
            TypedValue::Int(i) => i.to_string(),
            TypedValue::Float(f) => f.to_string(),
            TypedValue::Char(c) => c.to_string(),
            TypedValue::String(s) => s.to_string(),
            TypedValue::ComplexInt(r, i) => Complex::new(*r, *i).to_string(),
            TypedValue::ComplexFloat(r, i) => Complex::new(*r, *i).to_string(),
        }
    }
}

impl fmt::Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            DataType::Int => write!(f, "int"),
            DataType::Float => write!(f, "float"),
            DataType::Char => write!(f, "char"),
            DataType::String => write!(f, "string"),
            DataType::ComplexInt => write!(f, "complex_int"),
            DataType::ComplexFloat => write!(f, "complex_float"),
        }
    }
}

impl TryFrom<&str> for DataType {
    type Error = DobbyError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "int" => Ok(DataType::Int),
            "float" => Ok(DataType::Float),
            "char" => Ok(DataType::Char),
            "string" => Ok(DataType::String),
            "complex_int" => Ok(DataType::ComplexInt),
            "complex_float" => Ok(DataType::ComplexFloat),
            _ => Err(DobbyError::InvalidDataType(s.to_string())),
        }
    }
}

impl From<i32> for DataType {
    fn from(i: i32) -> Self {
        match i {
            0 => DataType::Int,
            1 => DataType::Float,
            2 => DataType::Char,
            3 => DataType::String,
            4 => DataType::ComplexInt,
            5 => DataType::ComplexFloat,
            _ => unreachable!("Invalid data type"),
        }
    }
}

impl DataType {
    pub fn to_sql(&self) -> String {
        match self {
            DataType::Int => "INTEGER".to_string(),
            DataType::Float => "REAL".to_string(),
            _ => "TEXT".to_string(),
        }
    }
}
