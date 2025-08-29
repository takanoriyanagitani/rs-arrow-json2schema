use std::io;

use serde_json::Value;

use arrow_schema::Schema;

use arrow::json;

pub fn infer_schema_from_value(v: &Value) -> Result<Schema, io::Error> {
    let vals: Vec<_> = vec![v];
    let rs: Result<Schema, _> =
        json::reader::infer_json_schema_from_iterator(vals.into_iter().map(Ok));
    rs.map_err(io::Error::other)
}

pub fn infer_schema_from_json_obj_bytes(json: &[u8]) -> Result<Schema, io::Error> {
    let v: Value = serde_json::from_slice(json)?;
    infer_schema_from_value(&v)
}

pub fn parse_json_schema(json: &[u8]) -> Result<Schema, io::Error> {
    serde_json::from_slice(json).map_err(io::Error::other)
}

pub fn merge_schema_unordered(defined: Schema, inf: Schema) -> Schema {
    rs_arrow_merge_schema::merge_schema_unordered(defined, inf)
}

pub fn merge_unordered(
    defined_schema_json: &[u8],
    sample_json: &[u8],
) -> Result<Schema, io::Error> {
    let df: Schema = parse_json_schema(defined_schema_json)?;
    let ss: Schema = infer_schema_from_json_obj_bytes(sample_json)?;
    Ok(merge_schema_unordered(df, ss))
}

pub fn schema2json2writer<W>(s: &Schema, wtr: &mut W) -> Result<(), io::Error>
where
    W: io::Write,
{
    serde_json::to_writer(wtr, s).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use arrow_schema::{Field, Schema};

    #[test]
    fn test_infer_schema_from_value() {
        let v = serde_json::json!({"name": "Alice", "age": 30});
        let schema = infer_schema_from_value(&v).expect("schema inference");
        assert_eq!(schema.fields().len(), 2);

        let name_field = schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::Utf8);

        let age_field = schema.field_with_name("age").unwrap();
        assert_eq!(age_field.data_type(), &DataType::Int64);
    }

    #[test]
    fn test_infer_schema_from_json_obj_bytes() {
        let json_bytes = br#"{"foo":"bar","count":42}"#;
        let schema = infer_schema_from_json_obj_bytes(json_bytes).expect("infer");
        assert_eq!(schema.fields().len(), 2);

        let foo_field = schema.field_with_name("foo").unwrap();
        assert_eq!(foo_field.data_type(), &DataType::Utf8);

        let count_field = schema.field_with_name("count").unwrap();
        assert_eq!(count_field.data_type(), &DataType::Int64);
    }

    #[test]
    fn test_merge_schema_unordered() {
        let schema1 = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema2 = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let merged = merge_schema_unordered(schema1, schema2);
        assert_eq!(merged.fields().len(), 2);
        assert!(merged.field_with_name("id").is_ok());
        assert!(merged.field_with_name("name").is_ok());
    }

    #[test]
    fn test_invalid_json() {
        let bad_json = br#"{"name":"Alice","age":30"#;
        let err = infer_schema_from_json_obj_bytes(bad_json).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
