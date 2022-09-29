#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Format {
    Json,
    Ascii,
    Csv,
    Html,
}

impl std::str::FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "json" => Ok(Format::Json),
            "ascii" => Ok(Format::Ascii),
            "csv" => Ok(Format::Csv),
            "html" => Ok(Format::Html),
            _ => Err(format!("Unknown format: {}", s)),
        }
    }
}
