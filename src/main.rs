mod core;

use crate::core::{types::Query, Database};
use std::collections::HashMap;

fn main() {
    let mut db = Database::open("test".into());
    db.execute(Query::Insert {
        into: "test".into(),
        values: HashMap::from([
            ("id".into(), 2i64.into()),
            ("name".into(), "test-name".into()),
        ]),
    })
    .unwrap();
    // db.execute(Query::Update {
    //     table: "test".into(),
    //     set: HashMap::from([("name".into(), "updated-name".into())]),
    //     conditions: HashMap::from([("id".into(), 1i64.into())]),
    // })
    // .unwrap();

    let result = db
        .execute(Query::Select {
            from: "test".into(),
            columns: vec!["id".into(), "name".into()],
            conditions: HashMap::new(),
        })
        .unwrap()
        .unwrap();
    dbg!(result);
}
