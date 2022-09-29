use std::collections::HashMap;
use std::fmt;
use std::io;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type FieldSet = HashMap<String, TypedValue>;

#[derive(Debug, Error)]
pub enum DbError {
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

    #[error("IO Error")]
    IoError(#[from] std::io::Error),
}

impl Serialize for DbError {
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
    String(String),
    CharInvl(char, char),
    StringInvl(String, String),
}

#[derive(Copy, Clone, PartialEq, Eq, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
pub enum DataType {
    Int,
    Float,
    Char,
    String,
    CharInvl,
    StringInvl,
}

impl TypedValue {
    pub fn data_type(&self) -> DataType {
        match self {
            TypedValue::Int(_) => DataType::Int,
            TypedValue::Float(_) => DataType::Float,
            TypedValue::Char(_) => DataType::Char,
            TypedValue::String(_) => DataType::String,
            TypedValue::CharInvl(_, _) => DataType::CharInvl,
            TypedValue::StringInvl(_, _) => DataType::StringInvl,
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
            DataType::StringInvl => Ok(TypedValue::StringInvl(read_string()?, read_string()?)),
            DataType::CharInvl => {
                let mut buf = [0; 2];
                reader.read_exact(&mut buf)?;
                Ok(TypedValue::CharInvl(char::from(buf[0]), char::from(buf[1])))
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
            TypedValue::CharInvl(c1, c2) => vec![c1 as u8, c2 as u8],
            TypedValue::StringInvl(s1, s2) => [convert_string(s1), convert_string(s2)].concat(),
        }
    }

    pub fn coerce(self, to: DataType) -> Result<Self, DbError> {
        let string_to_char = |s: &str| {
            if s.len() == 1 {
                Ok(s.chars().next().unwrap())
            } else {
                Err(DbError::InvalidValue(self.clone(), to))
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
                .map_err(|_| DbError::InvalidValue(self, to)),
            (TypedValue::String(s), DataType::Float) => s
                .parse::<f64>()
                .map(TypedValue::Float)
                .map_err(|_| DbError::InvalidValue(self, to)),
            (TypedValue::String(s), DataType::StringInvl) => {
                if let Some((s1, s2)) = s.split_once("..") {
                    Ok(TypedValue::StringInvl(s1.to_string(), s2.to_string()))
                } else {
                    Err(DbError::InvalidValue(self, to))
                }
            }
            (TypedValue::String(s), DataType::CharInvl) => {
                if let Some((s1, s2)) = s.split_once("..") {
                    Ok(TypedValue::CharInvl(
                        string_to_char(s1)?,
                        string_to_char(s2)?,
                    ))
                } else {
                    Err(DbError::InvalidValue(self, to))
                }
            }

            (TypedValue::Char(c), DataType::String) => Ok(TypedValue::String(c.to_string())),
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
            (TypedValue::StringInvl(s1, s2), DataType::CharInvl) => Ok(TypedValue::CharInvl(
                string_to_char(s1)?,
                string_to_char(s2)?,
            )),
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
            TypedValue::CharInvl(c1, c2) => format!("{}..{}", c1, c2),
            TypedValue::StringInvl(s1, s2) => format!("{}..{}", s1, s2),
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
            DataType::CharInvl => write!(f, "char_invl"),
            DataType::StringInvl => write!(f, "string_invl"),
        }
    }
}

impl TryFrom<&str> for DataType {
    type Error = DbError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "int" => Ok(DataType::Int),
            "float" => Ok(DataType::Float),
            "char" => Ok(DataType::Char),
            "string" => Ok(DataType::String),
            "char_invl" => Ok(DataType::CharInvl),
            "string_invl" => Ok(DataType::StringInvl),
            _ => Err(DbError::InvalidDataType(s.to_string())),
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
            4 => DataType::CharInvl,
            5 => DataType::StringInvl,
            _ => unreachable!("Invalid data type"),
        }
    }
}
