use super::*;

#[test]
fn create() -> Result<(), DbError> {
    let mut schema = Schema::default();
    let table_schema = vec![("column".into(), DataType::Str)];

    schema.create_table("test_table".to_string(), table_schema.clone())?;

    assert_eq!(schema.tables.len(), 1);
    assert_eq!(schema.tables["test_table"], table_schema);
    Ok(())
}

#[test]
fn drop() -> Result<(), DbError> {
    let mut schema = Schema::default();
    let table_schema = vec![("column".into(), DataType::Str)];

    schema.create_table("test_table".to_string(), table_schema)?;
    schema.drop_table("test_table".to_string())?;

    assert_eq!(schema.tables.len(), 0);
    Ok(())
}

#[test]
fn alter() -> Result<(), DbError> {
    let mut schema = Schema::default();
    let table_schema = vec![("column".into(), DataType::Str)];

    schema.create_table("test_table".to_string(), table_schema)?;
    schema.alter_table(
        "test_table".to_string(),
        [("column".into(), "renamed".into())].into(),
    )?;

    assert_eq!(schema.tables.len(), 1);
    assert_eq!(
        schema.tables["test_table"],
        vec![("renamed".into(), DataType::Str)]
    );
    Ok(())
}