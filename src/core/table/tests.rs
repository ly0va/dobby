use super::*;

fn table() -> Table {
    Table {
        name: "test".into(),
        columns: vec![
            ("id".into(), DataType::Int),
            ("price".into(), DataType::Float),
        ],
        file: tempfile::tempfile().unwrap(),
    }
}

#[test]
fn select() -> Result<(), DbError> {
    let mut table = table();
    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row.clone())?;

    let rows = table.select(vec![], [].into())?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], row);

    Ok(())
}

#[test]
fn project() -> Result<(), DbError> {
    let mut table = table();
    let mut row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row.clone())?;

    let rows = table.select(vec!["price".into()], [].into())?;
    assert_eq!(rows.len(), 1);

    row.remove("id");
    assert_eq!(rows[0], row);

    Ok(())
}

#[test]
fn filter() -> Result<(), DbError> {
    let mut table = table();
    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row)?;

    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(2)),
        ("price".into(), TypedValue::Float(18.18)),
    ]
    .into();

    table.insert(row.clone())?;

    let rows = table.select(vec![], [("id".into(), TypedValue::Int(2))].into())?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], row);

    Ok(())
}

#[test]
fn update() -> Result<(), DbError> {
    let mut table = table();
    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row)?;
    table.update(
        [("price".into(), TypedValue::Float(123.45))].into(),
        [].into(),
    )?;

    let rows = table.select(vec![], [].into())?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["price"], TypedValue::Float(123.45));

    Ok(())
}

#[test]
fn delete() -> Result<(), DbError> {
    let mut table = table();
    let row: HashMap<_, _> = [
        ("id".into(), TypedValue::Int(1)),
        ("price".into(), TypedValue::Float(1.23)),
    ]
    .into();

    table.insert(row)?;
    table.delete([].into())?;

    let rows = table.select(vec![], [].into())?;
    assert!(rows.is_empty());

    Ok(())
}
