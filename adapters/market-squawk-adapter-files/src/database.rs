//! Closed, read-only extraction from one allowlisted SQLite base table.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;

use rusqlite::config::DbConfig;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::limits::Limit;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, MAIN_DB};

use crate::{CellValue, FileAdapterError, ParseBudget, ParsedRow, ParserLimit};

const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";
const MINIMUM_SQLITE_HEADER_BYTES: usize = 100;
const SQLITE_CACHE_KIBIBYTES: usize = 512;
const SQLITE_CONNECTION_ALLOWANCE_BYTES: usize = 256 * 1024;
const SCHEMA_QUERY: &str =
    "SELECT type, sql FROM main.sqlite_schema WHERE name = ?1 COLLATE BINARY";

pub(crate) fn parse(
    bytes: &[u8],
    table: &str,
    columns: &[String],
    order_by: &[String],
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    let page_size = validate_header(bytes)?;
    budget.columns(columns.len())?;
    // rusqlite 0.40.1 allocates exactly `bytes.len()` with sqlite3_malloc64 before reading the
    // database image. Admit that owned deserialize buffer exactly before opening the connection.
    budget.allocation_bytes(bytes.len())?;
    let cache_bytes = SQLITE_CACHE_KIBIBYTES
        .checked_mul(1024)
        .and_then(|bytes| bytes.checked_add(page_size))
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    let runtime_bound = cache_bytes
        .checked_add(SQLITE_CONNECTION_ALLOWANCE_BYTES)
        .and_then(|bytes| bytes.checked_add(budget.limits.input.max_text_bytes))
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes))?;
    // The bundled SQLite page cache honors a negative KiB cache-size request. One source page is
    // added for page-count rounding; the fixed allowance covers connection/query structures and
    // max_text_bytes covers one admitted schema/SQL text allocation. Doubling covers allocator
    // growth without mutating SQLite's process-global heap limit.
    budget.pre_admit_dynamic_bytes(runtime_bound)?;
    let query = select_query(table, columns, budget.row_limit(), budget)?;
    let mut connection = hardened_connection(bytes, query.len(), budget)?;
    require_base_table(&connection, table, budget)?;
    install_authorizer(&connection, table, columns, budget)?;
    let mut rows = read_rows(&mut connection, &query, columns, budget)?;
    rows.sort_by(|left, right| compare_rows(left, right, order_by));
    Ok(rows)
}

fn validate_header(bytes: &[u8]) -> Result<usize, FileAdapterError> {
    if bytes.len() < MINIMUM_SQLITE_HEADER_BYTES
        || bytes.get(..SQLITE_HEADER.len()) != Some(SQLITE_HEADER.as_slice())
        || bytes.get(18) != Some(&1)
        || bytes.get(19) != Some(&1)
    {
        return Err(FileAdapterError::UnsafeDatabase);
    }
    let encoded_page_size = u16::from_be_bytes(
        bytes
            .get(16..18)
            .ok_or(FileAdapterError::UnsafeDatabase)?
            .try_into()
            .map_err(|_| FileAdapterError::UnsafeDatabase)?,
    );
    let page_size = if encoded_page_size == 1 {
        65_536
    } else {
        usize::from(encoded_page_size)
    };
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        return Err(FileAdapterError::UnsafeDatabase);
    }
    Ok(page_size)
}

fn hardened_connection(
    bytes: &[u8],
    generated_query_bytes: usize,
    budget: &ParseBudget<'_>,
) -> Result<Connection, FileAdapterError> {
    let mut connection = Connection::open_in_memory().map_err(database_error)?;
    set_config(&connection, DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    set_config(
        &connection,
        DbConfig::SQLITE_DBCONFIG_WRITABLE_SCHEMA,
        false,
    )?;
    set_config(&connection, DbConfig::SQLITE_DBCONFIG_DQS_DML, false)?;
    set_config(&connection, DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)?;
    set_config(&connection, DbConfig::SQLITE_DBCONFIG_ENABLE_VIEW, false)?;
    set_config(&connection, DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false)?;
    set_config(
        &connection,
        DbConfig::SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER,
        false,
    )?;
    set_config(&connection, DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA, false)?;
    set_limits(&connection, generated_query_bytes, budget)?;
    connection
        .deserialize_read_exact(MAIN_DB, Cursor::new(bytes), bytes.len(), true)
        .map_err(database_error)?;
    configure_page_cache(&connection)?;
    connection
        .pragma_update(None, "query_only", true)
        .map_err(database_error)?;
    Ok(connection)
}

fn configure_page_cache(connection: &Connection) -> Result<(), FileAdapterError> {
    let cache_kibibytes =
        i64::try_from(SQLITE_CACHE_KIBIBYTES).map_err(|_| FileAdapterError::UnsafeDatabase)?;
    let configured = cache_kibibytes
        .checked_neg()
        .ok_or(FileAdapterError::UnsafeDatabase)?;
    connection
        .pragma_update(Some("main"), "cache_size", configured)
        .map_err(database_error)?;
    let actual: i64 = connection
        .pragma_query_value(Some("main"), "cache_size", |row| row.get(0))
        .map_err(database_error)?;
    if actual != configured {
        return Err(FileAdapterError::UnsafeDatabase);
    }
    Ok(())
}

fn set_config(
    connection: &Connection,
    config: DbConfig,
    enabled: bool,
) -> Result<(), FileAdapterError> {
    let actual = connection
        .set_db_config(config, enabled)
        .map_err(database_error)?;
    if actual != enabled {
        return Err(FileAdapterError::UnsafeDatabase);
    }
    Ok(())
}

fn set_limits(
    connection: &Connection,
    generated_query_bytes: usize,
    budget: &ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let sql_bytes = generated_query_bytes.max(SCHEMA_QUERY.len());
    let sql_bytes = i32::try_from(sql_bytes).map_err(|_| FileAdapterError::UnsafeDatabase)?;
    let text_bytes = i32::try_from(budget.limits.input.max_text_bytes)
        .map_err(|_| FileAdapterError::UnsafeDatabase)?;
    let columns = i32::try_from(budget.limits.input.max_columns.max(2))
        .map_err(|_| FileAdapterError::UnsafeDatabase)?;
    let expression_depth = i32::try_from(budget.limits.input.max_nesting_depth)
        .map_err(|_| FileAdapterError::UnsafeDatabase)?;
    let limits = [
        (Limit::SQLITE_LIMIT_LENGTH, text_bytes),
        (Limit::SQLITE_LIMIT_SQL_LENGTH, sql_bytes),
        (Limit::SQLITE_LIMIT_COLUMN, columns),
        (Limit::SQLITE_LIMIT_EXPR_DEPTH, expression_depth),
        (Limit::SQLITE_LIMIT_COMPOUND_SELECT, 1),
        (Limit::SQLITE_LIMIT_VDBE_OP, 100_000),
        (Limit::SQLITE_LIMIT_FUNCTION_ARG, 0),
        (Limit::SQLITE_LIMIT_ATTACHED, 0),
        (Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 0),
        (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 1),
        (Limit::SQLITE_LIMIT_TRIGGER_DEPTH, 0),
        (Limit::SQLITE_LIMIT_WORKER_THREADS, 0),
    ];
    for (limit, value) in limits {
        connection.set_limit(limit, value).map_err(database_error)?;
    }
    Ok(())
}

fn require_base_table(
    connection: &Connection,
    table: &str,
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let mut statement = connection.prepare(SCHEMA_QUERY).map_err(database_error)?;
    let mut rows = statement.query([table]).map_err(database_error)?;
    let row = rows
        .next()
        .map_err(database_error)?
        .ok_or(FileAdapterError::UnsafeDatabase)?;
    let object_type = sqlite_text(row.get_ref(0).map_err(database_error)?, budget)?;
    let schema_sql = sqlite_text(row.get_ref(1).map_err(database_error)?, budget)?;
    let trimmed_schema = schema_sql.trim_start();
    let virtual_table = trimmed_schema
        .get(.."CREATE VIRTUAL TABLE".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CREATE VIRTUAL TABLE"));
    if object_type != "table" || virtual_table || rows.next().map_err(database_error)?.is_some() {
        return Err(FileAdapterError::UnsafeDatabase);
    }
    Ok(())
}

fn install_authorizer(
    connection: &Connection,
    table: &str,
    columns: &[String],
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let table = budget.owned_text(table)?;
    let mut selected_columns = BTreeSet::new();
    for column in columns {
        let column = budget.owned_text(column)?;
        budget.set_entry::<String>()?;
        let _ = selected_columns.insert(column);
    }
    connection
        .authorizer(Some(move |context: AuthContext<'_>| {
            let allowed = match context.action {
                AuthAction::Select => context.accessor.is_none(),
                AuthAction::Read {
                    table_name,
                    column_name,
                } => {
                    context.database_name == Some("main")
                        && context.accessor.is_none()
                        && table_name == table
                        && selected_columns.contains(column_name)
                }
                _ => false,
            };
            if allowed {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }))
        .map_err(database_error)
}

fn read_rows(
    connection: &mut Connection,
    query: &str,
    columns: &[String],
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    let mut statement = connection.prepare(query).map_err(database_error)?;
    if statement.column_names().as_slice() != columns {
        return Err(FileAdapterError::UnsafeDatabase);
    }
    let mut query_rows = statement.query([]).map_err(database_error)?;
    let mut parsed = Vec::new();
    while let Some(row) = query_rows.next().map_err(database_error)? {
        budget.record()?;
        let mut fields = BTreeMap::new();
        for (index, column) in columns.iter().enumerate() {
            budget.cell()?;
            let value = match row.get_ref(index).map_err(database_error)? {
                ValueRef::Null => CellValue::Null,
                ValueRef::Integer(value) => {
                    let value = budget.formatted_text(20, format_args!("{value}"))?;
                    CellValue::Text(value)
                }
                ValueRef::Text(value) => CellValue::Text(budget.owned_text(
                    std::str::from_utf8(value).map_err(|_| FileAdapterError::UnsafeDatabase)?,
                )?),
                ValueRef::Real(_) | ValueRef::Blob(_) => {
                    return Err(FileAdapterError::UnsafeDatabase);
                }
            };
            let column = budget.owned_text(column)?;
            budget.map_entry::<String, CellValue>()?;
            if fields.insert(column, value).is_some() {
                return Err(FileAdapterError::DuplicateField);
            }
        }
        budget.fields(fields.len())?;
        let row = ParsedRow::try_new(fields, budget)?;
        budget.reserve_vec_slot(&mut parsed)?;
        parsed.push(row);
    }
    Ok(parsed)
}

fn select_query(
    table: &str,
    columns: &[String],
    max_records: usize,
    budget: &mut ParseBudget<'_>,
) -> Result<String, FileAdapterError> {
    let limit = max_records
        .checked_add(1)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::Records))?;
    let limit = budget.formatted_text(10, format_args!("{limit}"))?;
    let columns_bytes = columns.iter().try_fold(0_usize, |total, column| {
        total
            .checked_add(column.len())
            .and_then(|bytes| bytes.checked_add(2))
            .ok_or(FileAdapterError::LimitExceeded(ParserLimit::TextBytes))
    })?;
    let separators = columns
        .len()
        .saturating_sub(1)
        .checked_mul(2)
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::TextBytes))?;
    let query_bytes = "SELECT "
        .len()
        .checked_add(columns_bytes)
        .and_then(|bytes| bytes.checked_add(separators))
        .and_then(|bytes| bytes.checked_add(" FROM \"".len()))
        .and_then(|bytes| bytes.checked_add(table.len()))
        .and_then(|bytes| bytes.checked_add("\" LIMIT ".len()))
        .and_then(|bytes| bytes.checked_add(limit.len()))
        .ok_or(FileAdapterError::LimitExceeded(ParserLimit::TextBytes))?;
    budget.text(query_bytes)?;
    let mut query = budget.string_with_capacity(query_bytes)?;
    query.push_str("SELECT ");
    for (index, column) in columns.iter().enumerate() {
        if index != 0 {
            query.push_str(", ");
        }
        query.push('"');
        query.push_str(column);
        query.push('"');
    }
    query.push_str(" FROM \"");
    query.push_str(table);
    query.push_str("\" LIMIT ");
    query.push_str(&limit);
    Ok(query)
}

fn sqlite_text(
    value: ValueRef<'_>,
    budget: &mut ParseBudget<'_>,
) -> Result<String, FileAdapterError> {
    let ValueRef::Text(value) = value else {
        return Err(FileAdapterError::UnsafeDatabase);
    };
    budget.owned_text(std::str::from_utf8(value).map_err(|_| FileAdapterError::UnsafeDatabase)?)
}

fn compare_rows(left: &ParsedRow, right: &ParsedRow, order_by: &[String]) -> Ordering {
    for column in order_by {
        let ordering = left.fields.get(column).cmp(&right.fields.get(column));
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.fields.cmp(&right.fields)
}

fn database_error(_: rusqlite::Error) -> FileAdapterError {
    FileAdapterError::UnsafeDatabase
}
