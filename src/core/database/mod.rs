use super::schema::Schema;
use super::types::{ColumnSet, DobbyError, Query};
use std::sync::Mutex;

pub mod dobby;
pub mod sqlite;

pub trait Database: Send + Sync {
    fn execute(&self, query: Query) -> Result<Vec<ColumnSet>, DobbyError>;
}

impl Database for Mutex<dobby::Dobby> {
    fn execute(&self, query: Query) -> Result<Vec<ColumnSet>, DobbyError> {
        self.lock().unwrap().execute(query)
    }
}

impl Database for Mutex<sqlite::Sqlite> {
    fn execute(&self, query: Query) -> Result<Vec<ColumnSet>, DobbyError> {
        self.lock().unwrap().execute(query)
    }
}
