use std::collections::HashMap;

use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::Schema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BasicField {
    pub name: String,
    pub dtyp: DataType,
    pub null: bool,
}

impl From<BasicField> for Field {
    fn from(b: BasicField) -> Self {
        Field::new(b.name, b.dtyp, b.null)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BasicFields {
    pub fields: Vec<BasicField>,
}

pub fn vec2fields(v: Vec<Field>) -> Fields {
    Fields::from(v)
}

impl From<BasicFields> for Fields {
    fn from(b: BasicFields) -> Self {
        let arrow_fields: Vec<Field> = b.fields.into_iter().map(Field::from).collect();
        vec2fields(arrow_fields)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BasicSchema {
    pub fields: BasicFields,
    pub metadata: HashMap<String, String>,
}

pub fn fields2schema(fields: Fields, meta: HashMap<String, String>) -> Schema {
    Schema::new_with_metadata(fields, meta)
}

impl From<BasicSchema> for Schema {
    fn from(b: BasicSchema) -> Self {
        let fields: Fields = b.fields.into();
        Schema::new_with_metadata(fields, b.metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    use crate::asch::{infer_schema_from_json_obj_bytes, merge_schema_unordered};

    fn basic_schema_from_json(json: &str) -> Result<BasicSchema, serde_json::Error> {
        serde_json::from_str::<BasicSchema>(json)
    }

    #[test]
    fn test_basic_schema_parsing() {
        let json = r#"
        {
            "fields": {
                "fields": [
                    {"name":"id","dtyp":"Int64","null":false},
                    {"name":"name","dtyp":"Utf8","null":true}
                ]
            },
            "metadata": {
                "owner":"test"
            }
        }
        "#;

        let parsed = basic_schema_from_json(json).expect("parse BasicSchema");
        assert_eq!(parsed.fields.fields.len(), 2);
        assert_eq!(parsed.metadata.len(), 1);
        assert_eq!(parsed.metadata["owner"], "test");

        let id_field = &parsed.fields.fields[0];
        assert_eq!(id_field.name, "id");
        assert_eq!(id_field.dtyp, DataType::Int64);
        assert!(!id_field.null);

        let name_field = &parsed.fields.fields[1];
        assert_eq!(name_field.name, "name");
        assert_eq!(name_field.dtyp, DataType::Utf8);
        assert!(name_field.null);
    }

    #[test]
    fn test_basic_schema_to_schema() {
        let json = r#"
        {
            "fields": {
                "fields": [
                    {"name":"id","dtyp":"Int64","null":false},
                    {"name":"name","dtyp":"Utf8","null":true}
                ]
            },
            "metadata": {
                "owner":"test",
                "team":"dev"
            }
        }
        "#;

        let basic: BasicSchema = basic_schema_from_json(json).expect("parse BasicSchema");
        let schema: Schema = basic.into();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.metadata().len(), 2);
        assert_eq!(schema.metadata().get("owner").unwrap(), "test");
        assert_eq!(schema.metadata().get("team").unwrap(), "dev");

        let id = schema.field_with_name("id").unwrap();
        assert_eq!(id.data_type(), &DataType::Int64);
        assert!(!id.is_nullable());

        let name = schema.field_with_name("name").unwrap();
        assert_eq!(name.data_type(), &DataType::Utf8);
        assert!(name.is_nullable());
    }

    #[test]
    fn test_merge_basic_and_guessed_schema() {
        let defined_json = r#"
        {
            "fields": {
                "fields": [
                    {"name":"id","dtyp":"Int64","null":false},
                    {"name":"name","dtyp":"Utf8","null":true}
                ]
            },
            "metadata": {
                "owner":"test"
            }
        }
        "#;
        let defined_basic: BasicSchema = basic_schema_from_json(defined_json).unwrap();
        let defined_schema: Schema = defined_basic.into();

        let sample_json = br#"{"id":42,"name":"Alice","age":30}"#;
        let guessed_schema = infer_schema_from_json_obj_bytes(sample_json).unwrap();

        let merged = merge_schema_unordered(defined_schema, guessed_schema);

        assert_eq!(merged.fields().len(), 3);
        assert!(merged.field_with_name("id").is_ok());
        assert!(merged.field_with_name("name").is_ok());
        assert!(merged.field_with_name("age").is_ok());

        assert_eq!(merged.metadata().get("owner").unwrap(), "test");
    }

    #[test]
    fn test_basic_schema_missing_fields() {
        let bad_json = r#"
        {
            "fields": {
                "name":"id","dtyp":"Int64","null":false
            },
            "metadata": {}
        }
        "#;

        let result: Result<BasicSchema, _> = basic_schema_from_json(bad_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_basic_schema_invalid_dtyp() {
        let bad_json = r#"
        {
            "fields": {
                "fields": [
                    {"name":"id","dtyp":"NotADatatype","null":false}
                ]
            },
            "metadata": {}
        }
        "#;

        let result: Result<BasicSchema, _> = basic_schema_from_json(bad_json);
        assert!(result.is_err());
    }
}
